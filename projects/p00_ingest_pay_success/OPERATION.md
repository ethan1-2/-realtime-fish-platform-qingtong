# P00 操作记录：pay_success 明细入库

## 概述

最小闭环：Kafka `tp_pay_success` → Flink JSON解析+校验 → Doris `p00_payment_detail` + `p00_dirty_event`

---

## Step 1: 基础设施准备

### 1.1 Doris 建表

#### 第一次尝试（失败）

尝试直接执行 `doris-ddl/scene1_settlement.sql`（全局 DDL）：
```
ERROR 1105 (HY000): Syntax error... Encountered: INTEGER LITERAL Expected
```
**原因**：Doris 2.1.7 的 Unique Key + merge-on-write 模式对 `DEFAULT` 值语法有限制。

#### 调整方案

为 P00 创建专用 DDL（`projects/p00_ingest_pay_success/ddl.sql`），去掉所有 DEFAULT 值。

#### 第二次尝试（又失败）

```
ERROR 1105 (HY000): Create unique keys table should not contain random distribution desc
```
**原因**：Doris Unique Key 表必须显式指定 `DISTRIBUTED BY HASH(列) BUCKETS N`，不能使用动态分区默认的随机分布。

#### 最终修复

在 DDL 中添加 `DISTRIBUTED BY HASH(idempotency_key) BUCKETS 16`，执行成功。

**教训**：Doris Unique Key 表两个必须：
1. 不能有复杂的 DEFAULT 值
2. 必须显式指定 `DISTRIBUTED BY HASH`

#### 执行结果

```bash
mysql -h 11.99.173.11 -P 9030 -uroot -p12345678 < projects/p00_ingest_pay_success/ddl.sql
```

验证：
```
mysql> USE saas_payment; SHOW TABLES;
+-------------------------+
| Tables_in_saas_payment  |
+-------------------------+
| p00_dirty_event         |
| p00_payment_detail      |
+-------------------------+
```

### 1.2 Kafka 创建 Topic

```bash
ssh root@11.26.164.164 'bash -s' < scripts/create-kafka-topics.sh
```

结果：5 个 topic 全部创建成功
```
tp_pay_success          (6 分区, 2 副本, 保留 7 天)
tp_refund_success       (6 分区, 2 副本, 保留 7 天)
tp_chargeback           (3 分区, 2 副本, 保留 7 天)
tp_settlement_adjust    (3 分区, 2 副本, 保留 7 天)
tp_take_rate_rule_change(3 分区, 2 副本, 保留 30 天)
```

注意：Kafka 会 WARNING topic 名中有下划线和点号可能冲突，但不影响使用。

### 1.3 数据生成器灌入测试数据

```bash
cd datagen
java -jar target/payment-datagen-1.0-SNAPSHOT.jar \
  --orders=10000 --tenants=20 --tps=5000
```

结果：
```
Total events generated: 9793 (含重复/重放扰动)
Sent to Kafka: 9828 条, 0 错误

按 topic 分布:
  tp_pay_success:          9578 条 (P00 只消费这个)
  tp_refund_success:        184 条
  tp_chargeback:             11 条
  tp_settlement_adjust:      20 条
  tp_take_rate_rule_change:  35 条

真账本写入: /tmp/datagen-ground-truth/
  GMV:     2,103,508.71 元
  退款:       41,520.35 元
  拒付:        2,216.00 元
  调账:          827.88 元
  平台抽成:   72,626.97 元
```

---

## Step 2: Flink Job 开发

### 2.1 项目结构

```
projects/p00_ingest_pay_success/
├── README.md           # 需求说明（你写的）
├── OPERATION.md        # 本文件（操作记录）
├── ddl.sql             # P00 专用 Doris DDL
├── pom.xml             # Maven 工程
└── src/main/java/com/saas/flink/p00/
    └── PaySuccessIngestJob.java   # Flink 主程序
```

### 2.2 核心逻辑

```
Kafka tp_pay_success
      │
      ▼
  JSON 解析 (FlatMapFunction)
      │
      ├── 解析失败 → 侧输出 → p00_dirty_event
      │
      ▼
  字段校验 (ProcessFunction)
      │
      ├── 校验失败 → 侧输出 → p00_dirty_event
      │   校验规则：
      │   - tenant_id/app_id/order_id/payment_id/idempotency_key/event_time 非空
      │   - amount_minor > 0
      │   - pay_method 枚举合法 (wx/alipay/card/h5/quickpay)
      │
      ▼
  维度补充 (event_date, process_time)
      │
      ▼
  Doris Sink → p00_payment_detail
      (Stream Load, Unique Key 幂等覆盖写入)
```

### 2.3 关键设计决策

1. **幂等策略**：不在 Flink 侧做 State 去重，完全依赖 Doris Unique Key
   `(event_date, idempotency_key)` 覆盖写入
   - P00 是最小闭环，不需要状态
   - P02 再加 State 去重

2. **脏数据处理**：侧输出流（Side Output）
   - 解析失败 → dirty_reason = "json_parse_error"
   - 字段校验失败 → dirty_reason = 具体原因

3. **Doris 写入**：Flink Doris Connector + Stream Load
   - 攒批写入，每 10 秒或 4MB 刷一次

---

## Step 3: 提交运行

（待 Flink Job 代码完成后执行）

```bash
# 编译
cd projects/p00_ingest_pay_success
mvn clean package -q

# 拷贝到 Flink 集群
scp target/p00-ingest-pay-success-1.0-SNAPSHOT.jar root@11.18.17.7:/tmp/

# 提交 Job
ssh root@11.18.17.7 "/opt/flink-1.15.2/bin/flink run -d \
  /tmp/p00-ingest-pay-success-1.0-SNAPSHOT.jar"

# 查看 Job 状态
ssh root@11.18.17.7 "/opt/flink-1.15.2/bin/flink list"
```

---

## Step 4: 验收

### 4.1 验证明细数据存在

```sql
USE saas_payment;
SELECT event_type, COUNT(*) AS cnt, SUM(amount_minor)/100.0 AS total_yuan
FROM p00_payment_detail
WHERE event_date = '2026-03-16'
GROUP BY event_type;
-- 预期：只有 pay_success 类型
```

### 4.2 验证幂等（重复数据不翻倍）

```sql
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT idempotency_key) AS distinct_keys
FROM p00_payment_detail
WHERE event_date = '2026-03-16';
-- 预期：total_rows = distinct_keys（Unique Key 保证）
```

### 4.3 验证脏数据被捕获

```sql
SELECT dirty_reason, COUNT(*) AS cnt
FROM p00_dirty_event
WHERE event_date = '2026-03-16'
GROUP BY dirty_reason;
-- 预期：有若干条脏数据（金额为0/负数、pay_method=unknown 等）
```

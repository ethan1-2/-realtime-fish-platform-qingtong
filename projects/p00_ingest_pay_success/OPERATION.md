# P00 操作记录：pay_success 明细入库

## 概述

最小闭环：Kafka `tp_pay_success` → Flink JSON解析+校验 → Doris `p00_payment_detail` + `p00_dirty_event`

---

## Step 1: 基础设施准备

### 1.1 Doris 建表

#### 尝试 1：执行全局 DDL（失败）

```bash
mysql -h 11.99.173.11 -P 9030 -uroot -p12345678 < doris-ddl/scene1_settlement.sql
```

报错：
```
ERROR 1105 (HY000) at line 17: errCode = 2, detailMessage = Syntax error in line 21:
...e BIGINT DEFAULT 0 COMMENT '固定费用(分)',
                   ^
Encountered: INTEGER LITERAL
```

**原因**：Doris 2.1.7 的 Unique Key + `enable_unique_key_merge_on_write` 模式下，非 Key 列不支持复杂的 `DEFAULT` 值（如 `DEFAULT 0`、`DEFAULT 'CNY'`、`DEFAULT CURRENT_TIMESTAMP`）。

**解决**：为 P00 创建专用 DDL (`ddl.sql`)，去掉所有 DEFAULT 值。

#### 尝试 2：去掉 DEFAULT 后执行（失败）

```bash
mysql -h 11.99.173.11 -P 9030 -uroot -p12345678 < projects/p00_ingest_pay_success/ddl.sql
```

报错：
```
ERROR 1105 (HY000): Create unique keys table should not contain random distribution desc
```

**原因**：Doris Unique Key 表必须显式指定 `DISTRIBUTED BY HASH(列) BUCKETS N`，不能依赖动态分区默认的随机分布。

**解决**：在 DDL 中添加 `DISTRIBUTED BY HASH(idempotency_key) BUCKETS 16`。

#### 尝试 3：成功

```bash
mysql -h 11.99.173.11 -P 9030 -uroot -p12345678 < projects/p00_ingest_pay_success/ddl.sql
# 成功，无输出
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

**教训总结**：Doris Unique Key 表（尤其是 merge-on-write 模式）的两个硬约束：
1. 非 Key 列不能有 DEFAULT 值
2. 必须显式指定 `DISTRIBUTED BY HASH`

---

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

注意：Kafka 会 WARNING topic 名中有下划线，但不影响使用。

---

### 1.3 数据生成器灌入测试数据

```bash
cd datagen
export JAVA_HOME=/opt/jdk1.8.0_144
export PATH=$JAVA_HOME/bin:/opt/apache-maven-3.9.6/bin:$PATH
java -jar target/payment-datagen-1.0-SNAPSHOT.jar --orders=10000 --tenants=20 --tps=5000
```

结果：
```
Sent to Kafka: 9828 条, 0 错误
  tp_pay_success:          9578 条 (P00 只消费这个)
  tp_refund_success:        184 条
  tp_chargeback:             11 条
  tp_settlement_adjust:      20 条
  tp_take_rate_rule_change:  35 条

真账本: /tmp/datagen-ground-truth/
  GMV: 2,103,508.71 元
```

---

## Step 2: Flink Job 开发与部署

### 2.1 版本 1：使用 Doris Flink Connector（失败）

**代码方案**：
- pom.xml 引入 `flink-doris-connector-1.15:1.4.0`
- 使用 `DorisSink` 的 Builder API 配置 Stream Load
- 明细表和脏数据表各一个 DorisSink

**编译成功**，jar 大小 24MB。

#### 提交尝试 1：OOM（TaskManager heap 383MB）

```bash
ssh root@11.18.17.7 "/opt/flink-1.15.2/bin/flink run -d /tmp/p00-ingest-pay-success-1.0-SNAPSHOT.jar"
# JobID: 720b7dbfa6790a064416e0d53a2aa2dc
```

Job 状态立即变为 RESTARTING。查看 TM 日志 (s1-23273):
```
java.lang.OutOfMemoryError: Java heap space
  at org.apache.doris.flink.sink.writer.RecordBuffer.<init>(RecordBuffer.java:48)
```

**原因**：TaskManager 的 task heap 只有 383MB（总进程内存 1728MB，减去框架/网络/managed 后 task heap 很小），而 Doris Connector 的 `RecordBuffer` 在初始化时固定分配大内存块。

**解决尝试**：调小 Doris Sink 的 bufferSize 和 bufferCount 参数。

#### 提交尝试 2：还是 OOM（调小 buffer 无效）

把 bufferSize 从 4MB 降到 512KB，bufferCount 从 10000 降到 2000。重新编译提交。

结果：还是 OOM。因为 `RecordBuffer.<init>` 内部有独立于外部配置的固定内存分配。

**决定**：先增大 TaskManager 内存。

#### 增大 TaskManager 内存

当前配置：`taskmanager.memory.process.size: 1728m` → 改为 `4096m`。

**第一次修改（只改 nn1，失败）**：

```bash
ssh root@11.18.17.7 "sed -i 's/1728m/4096m/' /usr/local/flink-1.15.2/conf/flink-conf.yaml"
# 重启集群
ssh root@11.18.17.7 "/usr/local/flink-1.15.2/bin/stop-cluster.sh && sleep 3 && /usr/local/flink-1.15.2/bin/start-cluster.sh"
```

验证 TM heap：还是 383MB！

**原因**：Flink Standalone 模式下，每个 TM 节点 (s1/s2/s3) 有自己的 `flink-conf.yaml` 副本。只改 nn1 的配置没用，必须改 s1/s2/s3 上的。

**第二次修改（改所有 TM 节点，成功）**：

```bash
for ip in 11.237.80.37 11.26.164.137 11.18.17.58; do
  ssh root@$ip "sed -i 's/1728m/4096m/' /usr/local/flink-1.15.2/conf/flink-conf.yaml"
done
# 重启集群
```

验证：task heap 变成 1459MB。

#### 提交尝试 3：把 buffer 降到最小（还是 OOM）

即使 task heap 1459MB，Doris Connector 初始化 2 个 Sink 时创建 RecordBuffer 还是 OOM：
```
java.lang.OutOfMemoryError: Java heap space
  at org.apache.doris.flink.sink.writer.RecordBuffer.<init>(RecordBuffer.java:48)
  at org.apache.doris.flink.sink.writer.RecordStream.<init>(RecordStream.java:35)
  at org.apache.doris.flink.sink.writer.DorisStreamLoad.<init>(DorisStreamLoad.java:108)
```

**结论**：`flink-doris-connector-1.15:1.4.0` 的 RecordBuffer 内部内存分配与外部配置的 bufferSize 无关，在 TM heap 1459MB 的环境下两个 DorisSink 无法同时初始化。

#### 额外问题：TM 进程 OOM 后不自动恢复

OOM 发生后，TM 进程直接退出 (exit code 1)，不会自动重启。Job 状态显示 RESTARTING 但实际上没有可用的 TM。必须手动重启集群。

#### 额外问题：flink cancel 命令报错

```bash
ssh root@11.18.17.7 "/opt/flink-1.15.2/bin/flink cancel <jobId>"
# 报错：java.lang.NoSuchMethodError: org.apache.commons.cli.CommandLine.hasOption
```

**原因**：Flink 1.15.2 + Hadoop 2.7.3 的 commons-cli 版本冲突。

**解决**：改用 REST API 取消 Job：
```bash
curl -X PATCH "http://nn1-23273:8081/jobs/<jobId>?mode=cancel"
```

---

### 2.2 版本 2：自定义 HTTP Stream Load Sink（成功）

参考 [flink-learning](https://github.com/zhisheng17/flink-learning) 项目中 `RichSinkFunction` 的模式，决定**去掉 Doris Flink Connector，改为自定义轻量级 HTTP Sink**。

**核心设计**：

```java
public class DorisStreamLoadSink extends RichSinkFunction<String> {
    private List<String> buffer;  // 内存中只有一个 ArrayList

    public void invoke(String value, Context ctx) {
        buffer.add(value);
        if (buffer.size() >= batchSize || 超时) {
            flush();  // HTTP PUT 到 Doris BE Stream Load API
        }
    }

    private void flush() {
        // 拼接 JSON lines
        // HTTP PUT http://<be>:8040/api/<db>/<table>/_stream_load
        // Headers: Authorization, format=json, read_json_by_line=true
    }
}
```

**优点**：
- 内存完全可控（只有 ArrayList 和序列化的 byte[]）
- 不依赖 Doris Connector（去掉 8MB 的依赖，jar 从 24MB 降到 16MB）
- 支持 BE 节点轮询和失败重试

**关键实现细节**：
- 攒批策略：每 5000 条或每 10 秒 flush（明细表），每 1000 条或 30 秒 flush（脏数据表）
- BE 轮询：3 个 BE 节点轮流发送，某个失败自动切换到下一个
- Label 生成：`p00_<table>_<subtaskIndex>_<timestamp>_<counter>_r<retryNum>` 保证唯一
- 最多重试 3 次

**从 pom.xml 移除 Doris Connector 依赖**，只保留 Flink Streaming + Kafka Connector + Jackson。

#### 编译部署

```bash
mvn clean package -q    # 16MB jar
scp target/*.jar root@11.18.17.7:/tmp/
ssh root@11.18.17.7 "/opt/flink-1.15.2/bin/flink run -d /tmp/p00-ingest-pay-success-1.0-SNAPSHOT.jar"
# JobID: d62b306f47134777342df3cc23a98c8b
```

#### 额外问题：两个 Job 同时运行

提交新 Job 后发现有两个 P00 Job 都在 RUNNING：
```
d62b306f P00-PaySuccess-Ingest RUNNING  (新的，自定义 Sink)
68c8d076 P00-PaySuccess-Ingest RUNNING  (旧的，Doris Connector 版本)
```

**原因**：之前旧 Job 在 TM OOM 时无法 cancel 成功（TM 已退出），但 Flink HA（ZooKeeper）保存了 Job 元数据。集群重启后 HA 自动恢复了旧 Job。

**解决**：用 REST API 取消旧 Job：
```bash
curl -X PATCH "http://nn1-23273:8081/jobs/68c8d076c530fd60d3f55f2d53f42996?mode=cancel"
```

#### 成功运行

Job 状态 RUNNING，约 20 秒后 Doris 中出现数据。

---

## Step 3: 验收结果

### 3.1 明细数据

```sql
SELECT event_type, COUNT(*) AS cnt, SUM(amount_minor)/100.0 AS total_yuan
FROM p00_payment_detail GROUP BY event_type;
```
```
event_type   | cnt  | total_yuan
pay_success  | 9492 | 2,101,185.81
```

数据生成器产生 9578 条 pay_success（含重复），Doris Unique Key 去重后 9492 条。
差值 86 条 = 脏数据 15 条 + 重复被覆盖的 ~71 条（符合 0.5% 重复率预期）。

### 3.2 幂等验证

```sql
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT idempotency_key) AS distinct_keys
FROM p00_payment_detail;
```
```
total_rows | distinct_keys
9492       | 9492
```

**total_rows = distinct_keys**：证明 Unique Key 生效，同一 idempotency_key 的重复数据被自动合并。

### 3.3 脏数据

```sql
SELECT dirty_reason, COUNT(*) AS cnt FROM p00_dirty_event GROUP BY dirty_reason;
```
```
dirty_reason              | cnt
amount_le_zero:0          | 7      -- 金额为 0（数据生成器 zeroAmountRate=0.05%）
invalid_pay_method:unknown| 7      -- pay_method=unknown（unknownPayMethodRate=0.1%）
amount_le_zero:-2510      | 1      -- 负金额（negativeAmountRate=0.01%）
```

全部符合数据生成器的噪声配置比例。

### 3.4 租户倾斜

```sql
SELECT tenant_id, COUNT(*) AS cnt FROM p00_payment_detail GROUP BY tenant_id ORDER BY cnt DESC LIMIT 5;
```
```
tenant_id | cnt  | 占比
T0001     | 3319 | 35.0%   (配置 top1TenantShare=0.30, 实际 35%)
T0002     | 1451 | 15.3%
T0003     |  892 |  9.4%
T0004     |  631 |  6.6%
T0005     |  490 |  5.2%
Top5 合计 | 6783 | 71.5%   (配置 top5TenantShare=0.60, 实际 71.5%)
```

---

## 踩坑总结

| # | 问题 | 原因 | 解决方案 |
|---|------|------|----------|
| 1 | DDL 报语法错误 | Doris Unique Key + merge-on-write 不支持 DEFAULT 值 | 去掉所有 DEFAULT |
| 2 | DDL 报 random distribution | Unique Key 表必须显式 DISTRIBUTED BY HASH | 加 `DISTRIBUTED BY HASH(idempotency_key) BUCKETS 16` |
| 3 | Doris Connector OOM | RecordBuffer 内部固定分配大内存，与 bufferSize 参数无关 | 放弃 Connector，自定义 HTTP Stream Load Sink |
| 4 | 改 TM 内存不生效 | 只改了 nn1，没改 s1/s2/s3 上的 flink-conf.yaml | 所有 TM 节点都要改 |
| 5 | TM OOM 后不恢复 | OOM 导致 JVM 进程退出，Standalone 模式不自动重启 TM | 手动 stop-cluster + start-cluster |
| 6 | flink cancel 报错 | commons-cli 版本冲突（Flink 1.15 + Hadoop 2.7） | 改用 REST API: `curl -X PATCH .../jobs/{id}?mode=cancel` |
| 7 | 旧 Job 自动恢复 | Flink HA (ZooKeeper) 在集群重启后恢复了之前的 Job | REST API 手动取消重复的 Job |

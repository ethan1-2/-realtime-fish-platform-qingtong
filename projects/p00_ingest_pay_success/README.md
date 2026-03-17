# P00: pay_success 明细入仓 + 脏数据

## 目标

把 Kafka 的 `tp_pay_success` 事件稳定写入 Doris 明细表，跑通端到端链路，并把解析/校验失败的数据落到脏数据表。

这个练习不要求窗口聚合，不要求规则 join，不要求退款回补。只做”稳定入仓 + 幂等”。

---

## 快速导航

| 文件 | 说明 |
|------|------|
| `ddl.sql` | Doris 建表语句（p00_payment_detail + p00_dirty_event） |
| `examples/` | 输入输出数据示例（JSON 格式 + 字段说明） |
| `src/main/java/.../PaySuccessIngestJob.java` | Flink Job 源码（详细注释） |
| `OPERATION.md` | 完整操作流水 + 踩坑记录 |

---

## 数据结构

### 输入：Kafka tp_pay_success

详见 `examples/input_pay_success.json`（正常数据）和 `examples/input_dirty_zero_amount.json`（脏数据）。

关键字段：
- `idempotency_key`：幂等键，格式 `idem_{tenant_id}_{order_id}`
- `amount_minor`：金额（分），必须 > 0
- `event_time`：业务时间（毫秒时间戳）
- `pay_method`：支付方式，枚举 `wx/alipay/card/h5/quickpay`

### 输出 1：Doris p00_payment_detail

详见 `examples/output_to_doris_detail.json`。

字段变化：
- **新增** `event_date`：从 `event_time` 提取日期（Doris 分区键）
- **新增** `process_time`：Flink 处理时间
- **转换** `event_time`：毫秒时间戳 → `yyyy-MM-dd HH:mm:ss.SSS` 字符串

表结构：
- **UNIQUE KEY**: `(event_date, idempotency_key)` — 幂等去重
- **分区**: 按 `event_date` 动态分区（天级）
- **分桶**: 按 `idempotency_key` HASH 分桶（16 桶）

### 输出 2：Doris p00_dirty_event

详见 `examples/output_to_doris_dirty.json`。

关键字段：
- `dirty_reason`：校验失败原因（如 `amount_le_zero:0`）
- `raw_payload`：原始 JSON（截断到 2000 字符）

---

## 输入

- Kafka topic: `tp_pay_success`
- 事件字段参考：`examples/README.md`（含完整字段说明）
- 产数：复用 `datagen/`（可先用小参数跑通）

## 输出（2 张表）

建表语句见 `ddl.sql`：

- `saas_payment.p00_payment_detail`
  - 只写入 `event_type = 'pay_success'` 的行
  - 主键：`(event_date, idempotency_key)` 用于幂等覆盖
- `saas_payment.p00_dirty_event`
  - 记录校验失败的数据和原因

## Flink 要做的事

- JSON 解析
- 字段校验（建议至少覆盖）
  - `tenant_id/app_id/order_id/payment_id/idempotency_key/event_time/amount_minor` 不为空
  - `amount_minor > 0`
  - `pay_method` 枚举合法（非法的进脏表）
- 维度补齐
  - `event_date = toDate(event_time)`
  - `process_time = current_timestamp`
- 幂等策略
  - 直接依赖 Doris `UNIQUE KEY(event_date, idempotency_key)` 覆盖写入

## 验收（建议 SQL）

1. 明细表有数据

```sql
USE saas_payment;

SELECT event_date, COUNT(*) AS cnt
FROM dwd_payment_detail
WHERE event_type = 'pay_success'
GROUP BY event_date
ORDER BY event_date DESC;
```

2. 验证幂等

```sql
USE saas_payment;

SELECT idempotency_key, COUNT(*) AS cnt
FROM dwd_payment_detail
WHERE event_type = 'pay_success'
GROUP BY idempotency_key
HAVING cnt > 1
LIMIT 20;
```

3. 脏数据表有少量记录（说明校验逻辑生效）

```sql
USE saas_payment;

SELECT dirty_reason, COUNT(*) AS cnt
FROM dwd_dirty_event
GROUP BY dirty_reason
ORDER BY cnt DESC;
```

---

## 生产边界 / 本题简化点

**P00 是最小闭环练习，以下设计只适用于本题，不代表生产默认首选：**

### 1. 幂等策略的局限

- **当前做法**：完全依赖 Doris UNIQUE KEY 的 last-write-wins（最后写入覆盖）
- **适用场景**：相同幂等键的重复投递是相同 payload，或业务上允许覆盖
- **不适用场景**：
  - 退款/拒付等需要按业务时间保留"最新"的场景（乱序到达会导致旧数据覆盖新数据）
  - 需要严格 exactly-once 语义的场景（重复数据仍会发 HTTP 到 Doris，浪费网络 IO）
- **生产改进方向**：
  - P02 会加 Flink State 去重（端到端 exactly-once）
  - 或在 Doris 表中加 sequence column（按业务时间保留最新）

### 2. Sink 容错语义

- **当前做法**：自定义 HTTP Stream Load Sink，buffer 不受 checkpoint 保护
- **容错语义**：
  - Kafka offset 由 checkpoint 保护（at-least-once 消费）
  - Sink buffer 不受保护（Job 崩溃时可能丢失最后一批，最多 flushIntervalSec 秒的数据）
  - 整体是"at-least-once 消费 + 可能丢失尾部"
- **为什么这样设计**：
  - 官方 Doris Flink Connector 在本环境 OOM（详见 OPERATION.md 踩坑 #3）
  - P00 重点是跑通链路，不是严格容错
- **生产改进方向**：
  - 实现 CheckpointListener，在 notifyCheckpointComplete() 中 flush buffer
  - 或用官方 Connector（需要更大的 TaskManager 内存）
  - 或接受"最多丢失 10 秒数据"（对于秒级延迟要求的场景可接受）

### 3. 事件时间与 Watermark

- **当前做法**：`WatermarkStrategy.noWatermarks()`，不使用事件时间
- **为什么**：P00 不做窗口聚合，不需要 Watermark
- **生产边界**：P01 开始做分钟级窗口聚合时，会换成 `forBoundedOutOfOrderness()` 处理乱序

### 4. Kafka 消费进度可见性

- **当前做法**：KafkaSource 的消费进度保存在 Flink checkpoint state，不回写到 Kafka `__consumer_offsets`
- **现象**：用 `kafka-consumer-groups.sh` 查不到 consumer group
- **为什么**：Flink 1.15 的 KafkaSource 默认不回写 offset（这是正常的，不代表消费进度没保存）
- **生产边界**：是否回写 offset 取决于 connector 版本和配置，不能把"Web 工具是否可见"当作唯一判断依据

### 5. 数据倾斜

- **当前做法**：未做倾斜治理
- **现象**：数据生成器模拟了头部租户（T0001 占 35%），可能导致某些 subtask 负载高
- **生产边界**：P05 会专门练习倾斜治理（加盐、动态 rebalance 等）

---

## 学习重点

P00 的核心价值是**建立端到端链路的直觉**，而不是追求生产级完备性。

重点掌握：
- Flink 算子的生命周期（open/invoke/close）
- 侧输出流（OutputTag）的用法
- Doris Stream Load 协议
- 幂等的多种实现方式（Doris Unique Key vs Flink State）

后续练习会逐步补齐：
- P01: 事件时间 + Watermark
- P02: 多流 Join + 迟到数据处理
- P03/P04: Checkpoint + State 的深入理解
- P05: 数据倾斜治理


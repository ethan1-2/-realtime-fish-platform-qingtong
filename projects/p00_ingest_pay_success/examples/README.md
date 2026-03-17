# P00 数据示例说明

本目录包含 P00 项目的输入输出数据示例，帮助理解数据流转和字段映射。

---

## 输入：Kafka tp_pay_success

### 正常数据示例

文件：`input_pay_success.json`

```json
{
  "event_type": "pay_success",
  "tenant_id": "T0001",
  "app_id": "app_game_001",
  "order_id": "ORD20260316123456789",
  "payment_id": "PAY20260316123456789",
  "user_id": "user_12345",
  "idempotency_key": "idem_T0001_ORD20260316123456789",
  "amount_minor": 9900,                    // 99.00 元（分为单位）
  "currency": "CNY",
  "channel_id": "ch_wechat_h5",
  "pay_method": "wx",                      // 枚举：wx/alipay/card/h5/quickpay
  "psp": "wechatpay",
  "region": "CN-GD",
  "take_rate_rule_version": 1,
  "take_rate_pct": 0.03,                   // 3% 抽成
  "take_rate_fixed_fee": 0,
  "take_rate_amount": 297,                 // 平台抽成 2.97 元
  "settlement_amount": 9603,               // 租户结算 96.03 元
  "event_time": 1710576896000,             // 毫秒时间戳（业务时间）
  "ingest_time": 1710576897123,            // 进入 Kafka 的时间
  "trace_id": "trace_abc123",
  "data_quality_flag": "normal"
}
```

**字段说明**：
- `event_time`：业务时间（支付成功的时刻），毫秒时间戳
- `ingest_time`：数据生成器写入 Kafka 的时间
- `idempotency_key`：幂等键，格式 `idem_{tenant_id}_{order_id}`
- `amount_minor`：金额（分），必须 > 0
- `pay_method`：支付方式，必须在白名单中

---

### 脏数据示例

文件：`input_dirty_zero_amount.json`

```json
{
  "event_type": "pay_success",
  "tenant_id": "T0005",
  "app_id": "app_game_005",
  "order_id": "ORD20260316999999999",
  "payment_id": "PAY20260316999999999",
  "user_id": "user_99999",
  "idempotency_key": "idem_T0005_ORD20260316999999999",
  "amount_minor": 0,                       // ❌ 金额为 0，不合法
  "currency": "CNY",
  "channel_id": "ch_alipay",
  "pay_method": "alipay",
  "psp": "alipay",
  "region": "CN-BJ",
  "event_time": 1710576896000,
  "ingest_time": 1710576897123,
  "trace_id": "trace_dirty_001",
  "data_quality_flag": "suspect"
}
```

**校验失败原因**：`amount_minor <= 0`，会被路由到脏数据表。

---

## 输出 1：Doris p00_payment_detail（明细表）

文件：`output_to_doris_detail.json`

```json
{
  "event_date": "2026-03-16",              // ✨ 新增：从 event_time 派生
  "idempotency_key": "idem_T0001_ORD20260316123456789",
  "tenant_id": "T0001",
  "app_id": "app_game_001",
  "order_id": "ORD20260316123456789",
  "payment_id": "PAY20260316123456789",
  "user_id": "user_12345",
  "event_type": "pay_success",
  "amount_minor": 9900,
  "currency": "CNY",
  "channel_id": "ch_wechat_h5",
  "pay_method": "wx",
  "psp": "wechatpay",
  "region": "CN-GD",
  "take_rate_rule_version": 1,
  "take_rate_pct": 0.03,
  "take_rate_amount": 297,
  "settlement_amount": 9603,
  "trace_id": "trace_abc123",
  "event_time": "2026-03-16 12:34:56.000", // ✨ 转换：毫秒时间戳 → datetime 字符串
  "ingest_time": "2026-03-16 12:34:57.123",
  "process_time": "2026-03-16 12:35:10.456", // ✨ 新增：Flink 处理时间
  "data_quality_flag": "normal"
}
```

**字段变化**：
1. **新增 `event_date`**：从 `event_time` 提取日期，作为 Doris 分区键
2. **时间格式转换**：`event_time` 从毫秒时间戳转为 `yyyy-MM-dd HH:mm:ss.SSS` 字符串
3. **新增 `process_time`**：Flink 处理时间，用于跟踪延迟

**Doris 表结构**：
- **UNIQUE KEY**: `(event_date, idempotency_key)` — 幂等去重
- **分区**: 按 `event_date` 动态分区（天级）
- **分桶**: 按 `idempotency_key` HASH 分桶（16 桶）

---

## 输出 2：Doris p00_dirty_event（脏数据表）

文件：`output_to_doris_dirty.json`

```json
{
  "event_date": "2026-03-16",
  "receive_time": "2026-03-16 12:35:10.456",
  "dirty_reason": "amount_le_zero:0",      // 脏数据原因
  "event_type": "pay_success",
  "tenant_id": "T0005",
  "raw_payload": "{\"event_type\":\"pay_success\",\"tenant_id\":\"T0005\",...}" // 原始 JSON（截断到 2000 字符）
}
```

**字段说明**：
- `dirty_reason`：校验失败的具体原因（如 `amount_le_zero:0`、`invalid_pay_method:unknown`）
- `raw_payload`：原始 Kafka 消息（最多 2000 字符，防止超大消息）
- `receive_time`：Flink 接收到脏数据的时间

**Doris 表结构**：
- **DUPLICATE KEY**: `(event_date, receive_time)` — 允许重复
- **分区**: 按 `event_date` 动态分区（保留 7 天）

---

## 数据流转图

```
Kafka tp_pay_success (原始 JSON)
    ↓
Flink PaySuccessValidator
    ├─ 校验通过 → 补充 event_date/process_time → output_to_doris_detail.json
    │                                                ↓
    │                                          Doris p00_payment_detail
    │
    └─ 校验失败 → 构建脏数据记录 → output_to_doris_dirty.json
                                        ↓
                                  Doris p00_dirty_event
```

---

## 常见脏数据原因

| dirty_reason | 说明 | 数据生成器概率 |
|--------------|------|----------------|
| `amount_le_zero:0` | 金额为 0 | 0.05% |
| `amount_le_zero:-2510` | 负金额 | 0.01% |
| `invalid_pay_method:unknown` | pay_method 不在白名单 | 0.1% |
| `missing_tenant_id` | 缺少租户 ID | 极少（数据损坏） |
| `json_parse_error` | JSON 格式错误 | 极少（网络损坏） |

---

## 如何使用这些示例

### 1. 手动测试单条数据

```bash
# 发送正常数据到 Kafka
echo '{"event_type":"pay_success","tenant_id":"T0001",...}' | \
  /usr/local/kafka_2.13-2.6.0/bin/kafka-console-producer.sh \
  --broker-list kafka1-84239:9092 \
  --topic tp_pay_success

# 查看 Doris 明细表
mysql -h 11.99.173.11 -P 9030 -uroot -p12345678 -e "
  SELECT * FROM saas_payment.p00_payment_detail
  WHERE idempotency_key = 'idem_T0001_ORD20260316123456789';"
```

### 2. 对比输入输出字段

打开 `input_pay_success.json` 和 `output_to_doris_detail.json`，对比：
- 哪些字段是透传的（tenant_id、amount_minor 等）
- 哪些字段是新增的（event_date、process_time）
- 哪些字段做了转换（event_time 的时间戳 → 字符串）

### 3. 理解 DDL 与数据的对应

打开 `../ddl.sql`，对照 `output_to_doris_detail.json`：
- JSON 字段名必须与 Doris 列名完全匹配
- JSON 字段类型必须能转换为 Doris 列类型
- UNIQUE KEY 的两个字段（event_date、idempotency_key）在 JSON 中都存在

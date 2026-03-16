# P00: pay_success 明细入仓 + 脏数据

## 目标

把 Kafka 的 `tp_pay_success` 事件稳定写入 Doris 明细表，跑通端到端链路，并把解析/校验失败的数据落到脏数据表。

这个练习不要求窗口聚合，不要求规则 join，不要求退款回补。只做“稳定入仓 + 幂等”。

## 输入

- Kafka topic: `tp_pay_success`
- 事件字段参考：`docs/reference/desktop_codex_datagen_spec.md`
- 产数：复用 `datagen/`（可先用小参数跑通）

## 输出（至少 1 张表）

复用场景 1 DDL（`doris-ddl/scene1_settlement.sql`）：

- `saas_payment.dwd_payment_detail`
  - 只写入 `event_type = 'pay_success'` 的行
  - 主键：`(event_date, idempotency_key)` 用于幂等覆盖
- `saas_payment.dwd_dirty_event`（建议做）

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


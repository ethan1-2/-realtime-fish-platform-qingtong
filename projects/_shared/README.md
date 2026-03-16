# Shared（所有练习共用）

## 公共产数

- 数据生成器：`datagen/`
- 默认 topics（见 `datagen/src/main/java/com/saas/datagen/config/GenConfig.java`）：
  - `tp_pay_success`
  - `tp_refund_success`
  - `tp_chargeback`
  - `tp_settlement_adjust`
  - `tp_take_rate_rule_change`

## Doris DDL

场景 1 相关表已覆盖大部分练习所需的“最小落表集合”，优先复用：

- `doris-ddl/scene1_settlement.sql`
  - `saas_payment.dwd_payment_detail`
  - `saas_payment.dwd_dirty_event`
  - `saas_payment.dws_settlement_minute`
  - 以及其他场景 1 需要的表（dim/dws/对账等）

## 统一口径（建议）

- 以 `event_time` 为业务口径时间（窗口/聚合按它）
- `ingest_time` 用于分析延迟与乱序
- 幂等主键建议：`idempotency_key`


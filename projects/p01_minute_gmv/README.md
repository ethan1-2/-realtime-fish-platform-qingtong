# P01: pay_success 分钟 GMV 大盘

## 目标

只基于支付成功（`tp_pay_success`）做事件时间 1 分钟窗口聚合，落 Doris 分钟指标表，练熟：

- watermark 基本用法
- 事件时间窗口
- Doris Unique Key 覆盖写入（同一分钟同维度可更新）

## 输入

- Kafka topic: `tp_pay_success`
- 公共产数：`datagen/`

## 输出（至少 1 张表）

复用 `doris-ddl/scene1_settlement.sql`：

- `saas_payment.dws_settlement_minute`
  - 仅填充 `pay_count/pay_amount/net_amount`（其他字段可先置 0）

## Flink 要做的事

- 从 `event_time` 抽取事件时间
- Watermark（建议先从 5s 或 10s 乱序容忍开始）
- 1 分钟滚动窗口（Tumbling）
- 分组维度（建议与表主键一致）
  - `stat_date, stat_minute, tenant_id, app_id, channel_id, pay_method`
- 指标
  - `pay_count = COUNT(*)`
  - `pay_amount = SUM(amount_minor)`
  - `net_amount = pay_amount`（本练习只考虑 pay_success）

## 可选加分（不强制）

- 去重：按 `idempotency_key` 在进入窗口前做一次去重（TTL 例如 3 天），避免重复投递导致 pay_count/pay_amount 翻倍。

## 验收（建议 SQL）

1. 看最近 30 分钟的大盘

```sql
USE saas_payment;

SELECT stat_minute, SUM(pay_amount) AS gmv, SUM(pay_count) AS cnt
FROM dws_settlement_minute
WHERE stat_date = CURDATE()
GROUP BY stat_minute
ORDER BY stat_minute DESC
LIMIT 30;
```

2. 抽样看某个租户维度是否持续产出

```sql
USE saas_payment;

SELECT stat_minute, tenant_id, app_id, channel_id, pay_method, pay_count, pay_amount
FROM dws_settlement_minute
WHERE stat_date = CURDATE()
ORDER BY stat_minute DESC
LIMIT 50;
```


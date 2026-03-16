# P02: 多事件合流 + 分钟净入金 + 迟到回补

## 目标

基于 4 种业务事件（支付/退款/拒付/调账）做分钟级净入金指标，并能正确处理“迟到退款/拒付”对历史分钟指标的回补更新。

这个练习只做“净入金闭环”，暂不要求抽成规则版本化 join（那是场景 1 的后半段硬点）。

## 输入

- Kafka topics
  - `tp_pay_success`
  - `tp_refund_success`
  - `tp_chargeback`
  - `tp_settlement_adjust`
- 公共产数：`datagen/`（它会生成迟到/乱序/重复）

## 输出（至少 1 张表）

复用 `doris-ddl/scene1_settlement.sql`：

- `saas_payment.dws_settlement_minute`
  - 本练习把以下字段算完整：
    - `pay_* / refund_* / chargeback_* / adjust_*`
    - `net_amount = pay - refund - chargeback + adjust`

## Flink 要做的事

1. 多 topic 合流（统一投影成同一个“增量模型”）

- pay_success
  - `pay_count += 1`
  - `pay_amount += amount_minor`
  - `net_amount += amount_minor`
- refund_success（生成器里 `amount_minor` 通常为负数）
  - `refund_count += 1`
  - `refund_amount += ABS(amount_minor)`  (Doris 表里 refund_amount 习惯存正数)
  - `net_amount += amount_minor`
- chargeback（同 refund）
- settlement_adjust
  - `adjust_count += 1`
  - `adjust_amount += amount_minor`
  - `net_amount += amount_minor`

2. 事件时间与迟到回补

- 用 `event_time` 做 watermark
- 窗口：1 分钟滚动窗口
- allowed lateness：建议先设 6 小时（数据生成器支持更长的极端迟到，你也可以扩到 48 小时）
- 输出到 Doris 必须是“覆盖写入”语义（Unique Key），让迟到数据触发更新而不是翻倍累加

3. 强烈建议：幂等去重

因为输入是 Kafka 至少一次语义，且生成器会制造重复投递：

- 在进入窗口前，按 `idempotency_key` 做去重（State TTL 例如 3 天或 7 天）
- 否则你的 `refund_amount`、`net_amount` 会被重复投递放大

## 验收（建议 SQL）

1. 最近 30 分钟净入金趋势

```sql
USE saas_payment;

SELECT stat_minute, SUM(net_amount) AS net_in, SUM(pay_amount) AS gmv
FROM dws_settlement_minute
WHERE stat_date = CURDATE()
GROUP BY stat_minute
ORDER BY stat_minute DESC
LIMIT 30;
```

2. 验证回补更新（观察同一分钟同 key 的数据会被修正）

```sql
USE saas_payment;

SELECT *
FROM dws_settlement_minute
WHERE stat_date = CURDATE()
ORDER BY update_time DESC
LIMIT 50;
```

3. 验证“重复不翻倍”（如果你做了去重）

```sql
USE saas_payment;

SELECT stat_minute, tenant_id, app_id, channel_id, pay_method,
       pay_count, pay_amount, refund_count, refund_amount, net_amount
FROM dws_settlement_minute
WHERE stat_date = CURDATE()
ORDER BY stat_minute DESC
LIMIT 100;
```


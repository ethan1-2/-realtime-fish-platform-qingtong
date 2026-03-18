# P01: pay_success 分钟 GMV 大盘

## 目标

只基于支付成功（`tp_pay_success`）做事件时间 1 分钟窗口聚合，落 Doris 分钟指标表，练熟：

- watermark 基本用法
- 事件时间窗口
- Doris Unique Key 覆盖写入

## 输入

- Kafka topic: `tp_pay_success`
- 公共产数：`datagen/`

## 输出（至少 1 张 Doris 表）

为了方便独立验证，P01 使用专用表：

- `saas_payment.p01_settlement_minute`

这个表的字段和场景 1 的 `dws_settlement_minute` 保持同一类口径，但只保留 P01 必需字段：

- 维度：`stat_date/stat_minute/tenant_id/app_id/channel_id/pay_method`
- 指标：`pay_count/pay_amount/net_amount/update_time`

对应 DDL：`projects/p01_minute_gmv/ddl.sql`

## Flink 要做的事

- 从 `event_time` 抽取事件时间
- Watermark：`5s` 乱序容忍
- 1 分钟滚动窗口（Tumbling Event Time Window）
- 分组维度：
  - `tenant_id/app_id/channel_id/pay_method`
- 指标：
  - `pay_count = COUNT(*)`
  - `pay_amount = SUM(amount_minor)`
  - `net_amount = pay_amount`

## 交付物

- `pom.xml`
- `ddl.sql`
- `src/main/java/com/saas/flink/p01/MinuteGmvJob.java`

## 打包

```bash
cd projects/p01_minute_gmv
mvn clean package -DskipTests
```

## 验收（建议 SQL）

1. 看最近 30 分钟的大盘

```sql
USE saas_payment;

SELECT stat_minute, SUM(pay_amount) AS gmv, SUM(pay_count) AS cnt
FROM p01_settlement_minute
WHERE stat_date = CURDATE()
GROUP BY stat_minute
ORDER BY stat_minute DESC
LIMIT 30;
```

2. 抽样看某个租户维度是否持续产出

```sql
USE saas_payment;

SELECT stat_minute, tenant_id, app_id, channel_id, pay_method, pay_count, pay_amount
FROM p01_settlement_minute
WHERE stat_date = CURDATE()
ORDER BY stat_minute DESC
LIMIT 50;
```


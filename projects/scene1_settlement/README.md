# 场景 1: 多租户实时营收与结算（抽成规则可变 + 可对账）

## 为什么这是“大项目”

场景 1 把实时数仓最常见的 5 个难点一次性叠加：

- Kafka 至少一次 + Flink 重启重放：幂等去重
- 事件时间 + 迟到退款/拒付：历史窗口回补
- 抽成规则会变：规则版本化（按生效时间匹配历史订单）
- 多租户强倾斜：热点 key 治理
- 可对账可解释：明细审计 + 对账口径

如果你觉得一上来太硬，先按 P00/P01/P02 把“链路、窗口、回补、幂等”练顺。

## 推荐前置练习

- `../p00_ingest_pay_success/`
- `../p01_minute_gmv/`
- `../p02_net_minute_lateness/`

## 交付物（高层）

- Kafka 事件模型与 topic 规划（pay/refund/chargeback/adjust/rule_change）
- Flink:
  - 明细入仓（`dwd_payment_detail`），支持幂等覆盖 + 迟到更新
  - 分钟/小时聚合（`dws_settlement_minute`/`dws_settlement_hour`），支持回补
  - 抽成规则版本化计算（`take_rate_rule_change` 关联）
  - 对账（`dws_reconciliation`）
- Doris:
  - 明细层 + 聚合层 + 对账表

## 参考资料与落表

- 场景 1 DDL：`../../doris-ddl/scene1_settlement.sql`
- 场景题详述（仓库内主文档）：`../../CLAUDE.md`
- 原始题面参考：`../../docs/reference/desktop_saas_kafka_flink_doris_scenarios.md`


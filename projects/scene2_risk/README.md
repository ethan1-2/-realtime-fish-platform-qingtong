# 场景 2: 平台级资金风控与异常告警

## 目标

在同一条支付明细链路上叠加“风控特征与告警输出”，并做到：

- 规则可配置、可回溯（rule_version + evidence）
- 状态可控（TTL / state 规模可解释）
- 倾斜可控（攻击流量热点隔离）
- 可观测（lag/backpressure/checkpoint/写入失败率）

## 输入与输出（建议）

- 输入（Kafka）：
  - 支付事件（复用场景 1 的 topics）
  - 用户登录/注册/设备信息（可后续扩展 datagen）
  - 黑白名单/规则配置变更
- 输出：
  - Kafka: `tp_risk_hit`（下游封禁/二次验证/限额）
  - Doris: 风控命中明细 + 分布聚合（至少 1 张表）

## 参考资料

- 场景题原始描述：`../../docs/reference/desktop_saas_kafka_flink_doris_scenarios.md`
- 场景 2 设计建议：`../../CLAUDE.md`


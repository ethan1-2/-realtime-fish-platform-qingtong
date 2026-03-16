# P03: Checkpoint 恢复与重启语义（把“可恢复”练成肌肉记忆）

## 目标

把 checkpoint 从“概念”变成“可验证的行为”：

- 开启 checkpoint
- 人为制造失败（kill TaskManager / kill job）
- 观察恢复（从最近一次 completed checkpoint 恢复）
- 理解 checkpoint 和 Kafka offset、Flink state、sink 幂等之间的关系

## 输入

- Kafka topics: `tp_pay_success`
- 产数: 复用 `datagen/`，建议先跑小参数，便于稳定复现

## 输出（至少 1 张 Doris 表）

复用场景 1 DDL（`doris-ddl/scene1_settlement.sql`）：

- `saas_payment.dwd_payment_detail`（写 `event_type='pay_success'` 即可）

## 核心 Flink 点（本题必须覆盖）

- Checkpoint 相关配置（Flink 1.15）
  - `execution.checkpointing.interval`
  - `execution.checkpointing.mode`（AT_LEAST_ONCE / EXACTLY_ONCE）
  - `execution.checkpointing.timeout`
  - `execution.checkpointing.min-pause`
  - `execution.checkpointing.max-concurrent-checkpoints`
  - `execution.checkpointing.tolerable-failed-checkpoints`
  - 可选：unaligned checkpoints（用于反压场景）
- 重启策略（建议至少理解一种）
  - fixed-delay restart 或 failure-rate restart
- 观察点（Flink Web UI）
  - Checkpoints 列表是否持续完成
  - 失败后是否从最近一次 completed checkpoint 恢复

## 验收

1. 正常运行时 checkpoint 持续成功
- Web UI 里能看到周期性 completed checkpoint（不是一直 in progress / failed）

2. 人为制造失败并恢复
- 手动 kill 一个 TaskManager 或取消 job（看你怎么部署）
- 任务自动重启并恢复运行
- Doris 明细表持续增长且不出现“明显的大面积回退”或“暂停不动”

3. 语义理解（写在你的笔记里）

- 说明以下三者的关系
  - Kafka source offset（由 checkpoint 控制提交）
  - Flink managed state（会进入 checkpoint）
  - Doris sink（本项目通常靠 Unique Key 幂等覆盖，不等同于 2PC exactly-once sink）

## 交付物（建议）

- 把你最终使用的关键配置记录在本项目 README 末尾（或另建 `runbook.md`）
- 提交你写的 Flink job（DataStream 或 Flink SQL 均可）


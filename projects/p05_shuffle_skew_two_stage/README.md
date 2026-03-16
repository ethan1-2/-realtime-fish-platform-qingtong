# P05: Shuffle 与倾斜治理（两阶段聚合 + 加盐）

## 目标

把 Flink 的数据重分发（shuffle）从“能用”练到“能治理热点”：

- 理解 `keyBy` 会导致热点 key 把单个 subtask 打爆
- 用“两阶段聚合 + 加盐（salt）”把热点打散再收敛
- 在 Web UI 里用 backpressure / busy time / 吞吐对比验证治理效果

## 输入

- Kafka topics: `tp_pay_success`
- 产数: 复用 `datagen/`
  - 生成器默认就是强长尾（top1/top5 占比高），非常适合触发倾斜

## 输出（至少 1 张 Doris 表）

复用场景 1 DDL（`doris-ddl/scene1_settlement.sql`）：

- `saas_payment.dws_settlement_minute`（只算 pay_success 即可）

## 核心 Flink 点（本题必须覆盖）

- Shuffle/分区方式
  - `keyBy`（语义正确性依赖它）
  - `rebalance/rescale`（用于打散负载的工具，但会引入全局 shuffle 或改变局部性）
- 倾斜治理套路（必须实现）
  - 第一阶段：`keyBy(tenant_id + salt)` 预聚合
  - 第二阶段：`keyBy(tenant_id, app_id, channel_id, pay_method)` 收敛聚合

## 验收

1. 基线版本（不治理）
- 直接按最终维度 `keyBy` 后窗口聚合
- 在 Web UI 观察是否出现明显 backpressure 或某个 subtask records-in/records-out 极不均衡

2. 治理版本（两阶段聚合）
- 加入 salt 预聚合后再收敛
- 对比 Web UI
  - backpressure 是否下降
  - 最热点 subtask 是否不再成为瓶颈

3. 正确性不变
- 治理前后，分钟 GMV 总和应该一致（允许轻微时延差异，但口径不应变化）


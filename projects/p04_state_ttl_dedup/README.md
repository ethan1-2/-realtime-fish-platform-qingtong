# P04: Managed State TTL 去重 vs 本地缓存（理解“缓存”和 checkpoint 的边界）

## 目标

把“去重”做成可恢复、可解释的能力，并用一次对比实验搞清楚：

- Flink managed state（Keyed State）会进入 checkpoint，重启恢复后仍然生效
- 你在算子里自己写的本地内存 cache（例如 HashMap）默认不会进入 checkpoint，重启后会丢

## 输入

- Kafka topics: `tp_pay_success`（足够了）
- 产数: 复用 `datagen/`，并把重复投递比例调高一点更容易观察（例如 `payDuplicateRate`）

## 输出（至少 1 张 Doris 表）

复用场景 1 DDL（`doris-ddl/scene1_settlement.sql`）：

- `saas_payment.dws_settlement_minute`（只算 pay_success 的分钟 GMV）

## 核心 Flink 点（本题必须覆盖）

- Keyed State
  - 建议用 `MapState<String, Boolean>` 或 `ValueState` 记录 `idempotency_key` 是否已见
  - 开启 `State TTL`（例如 3 天或 7 天，按你的数据跨度）
- Checkpoint
  - 必须开启 checkpoint，并在重启后验证“去重仍然有效”
- 对比实验（必须做）
  - A 版本：用 managed state + TTL 去重
  - B 版本：用本地内存 HashMap 去重
  - 同样的输入、同样的 kill/restart 操作下，观察两者差异

## 建议实现（不强制）

- 先在进入窗口/聚合前做去重
  - `keyBy(idempotency_key)` 后做状态判断与过滤
  - 然后再进入按维度窗口聚合（`tenant_id/app_id/channel_id/pay_method`）

## 验收

1. A 版本（managed state）正确性
- 重复投递不会造成 `dws_settlement_minute.pay_amount/pay_count` 明显放大
- kill/restart 后仍然不放大

2. B 版本（本地 cache）对比结论
- 初始运行阶段可能看起来也能去重
- kill/restart 后会出现“去重失效”的窗口（因为 cache 丢失，重复会重新计入）

3. 写下你的结论（必须）

- “为什么 managed state 能恢复，本地 cache 不能”的一句话解释
- TTL 的意义是什么（state 不会无限增长）


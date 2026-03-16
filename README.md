# realtime-fish-platform-qingtong

SAAS 游戏支付抽水平台的实时数仓练习仓库（Kafka + Flink + Doris）。

这个仓库刻意按“项目化练习”组织：从最小闭环开始，把场景 1 的硬点拆成 3 个平滑练习题（每题至少落 1 张 Doris 表），再逐步升级到完整场景 1/2/3。

## 入口

- 文档总览: `docs/README.md`
- 练习项目列表（推荐学习顺序）: `projects/README.md`
- 公共模块:
  - `datagen/` 数据生成器（公共产数）
  - `doris-ddl/` Doris 建表脚本
  - `scripts/` Kafka topic / Doris 初始化脚本

## 快速定位

- 场景 1 Doris DDL: `doris-ddl/scene1_settlement.sql`
- 场景题原始描述（从桌面整理进来）:
  - `docs/reference/desktop_saas_kafka_flink_doris_scenarios.md`
  - `docs/reference/desktop_codex_datagen_spec.md`


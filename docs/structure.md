# 文档与练习结构

目标：让你在同一个业务背景下，从最小闭环平滑升档到完整场景，且每个练习题尽量是一个独立“项目目录”（便于单独开发/打包/运行/验收）。

## 目录约定

- `projects/`
  - 每个目录是一个练习项目（P00/P01/P02... 或 scene1/scene2/scene3）
  - 每个项目至少包含一个 `README.md`，写清楚：
    - 业务目标
    - 输入 topic
    - 输出 Doris 表（至少 1 张）
    - 验收口径（SQL / 对账抽样）
    - 实现提示（Flink SQL 或 DataStream 都可）
- `projects/_shared/`
  - 公共约定与复用说明（事件字段、topic、DDL、产数）
- `datagen/`
  - 公共产数模块（所有练习尽量复用，必要时再扩展）
- `doris-ddl/`
  - Doris 建表脚本（优先复用，不要为每题重复建一套表）
- `docs/reference/`
  - 原始参考资料（从桌面同步进来，避免丢失）

## 如何新增一个练习项目

1. 在 `projects/` 下新建目录，例如 `projects/p03_xxx/`。
2. 写 `README.md`，对齐上面的 5 点信息。
3. 如果需要新表：
   - 先尝试复用 `doris-ddl/scene1_settlement.sql` 里的表
   - 实在需要新增，再新增一个最小 DDL 文件放到 `doris-ddl/`，并在项目 README 里引用


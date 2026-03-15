# 捕鱼游戏实时数据平台

## 项目概述

基于 Kafka + Flink + Doris 架构，构建日增 20 亿条的捕鱼游戏实时数据平台。
通过 3 个递进式子项目，系统掌握大规模实时数据处理的核心能力。

## 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Kafka | 3.x | 消息队列，事件总线 |
| Flink | 1.18+ | 实时计算引擎 |
| Apache Doris | 2.1+ | 实时 OLAP 数据仓库 |
| Docker Compose | - | 本地开发环境 |
| Java 11 | - | Flink Job 开发语言 |
| Maven | - | 构建工具 |

## 业务背景

捕鱼游戏核心事件流：

| Kafka Topic | 说明 | 日增量级 |
|-------------|------|----------|
| tp_battle_fire | 开炮事件 | 20亿+（量最大） |
| tp_fish_kill | 击杀鱼事件 | 5亿 |
| tp_coin_change | 金币变动（充值/消耗/奖励） | 10亿 |
| tp_pay_success | 充值成功 | 500万 |
| tp_room_enter_exit | 进出房间 | 2亿 |
| tp_config_change | 配置变更（鱼种/掉落/活动） | 低频 |

---

## 项目1：实时金币对账系统（基础）

### 目标
跑通 Kafka→Flink→Doris 全链路，解决端到端一致性问题。

### 核心学习点
- [ ] Kafka 生产消费、分区策略
- [ ] Flink Checkpoint、State、窗口聚合
- [ ] Doris Unique Key + Sequence Column 幂等写入
- [ ] Stream Load 调优
- [ ] 对账表设计 + 差异定位 SQL
- [ ] 侧输出流处理脏数据

### 要做的事

#### 1.1 基础设施搭建
- [ ] docker-compose 编排 Kafka(3节点) + Flink(JobManager+TaskManager) + Doris(FE+BE)
- [ ] 数据生成器：模拟 coin_change 事件（充值/投注/奖励/消耗），支持可调 TPS

#### 1.2 Doris 表设计
- [ ] `player_coin_balance` — Unique Key，玩家金币余额表，Sequence Column 保证幂等
- [ ] `coin_change_detail` — Unique Key(event_date, change_id)，金币变动明细表
- [ ] `reconciliation_coin` — Unique Key，金币对账表（源头统计 vs 落地统计）

#### 1.3 Flink Job 开发
- [ ] Job-1: CoinChangeETL — 消费 tp_coin_change，清洗 + 写入 coin_change_detail
  - JSON 解析、数据校验、脏数据侧输出
  - Doris Flink Connector Stream Load 写入
  - 幂等 ID 生成策略：业务字段哈希
- [ ] Job-2: CoinBalanceUpdate — 消费 tp_coin_change，更新 player_coin_balance
  - KeyedState 维护余额
  - Sequence Column 版本号 = event_time_ms
- [ ] Job-3: CoinReconciliation — 实时对账
  - 按 player_id + hour 窗口聚合，输出源头统计到对账表
  - 定时 Doris SQL 任务回填落地统计，计算差异

#### 1.4 验证与调优
- [ ] 模拟 Flink 重启，验证 Checkpoint 恢复 + Doris 幂等写入无重复
- [ ] 模拟脏数据（缺字段、金额为负），验证侧输出流捕获
- [ ] 对账 SQL：查出差异玩家及原因分类
- [ ] Stream Load 参数调优（buffer size、interval、并行度）

---

## 项目2：实时房间大盘（进阶）

### 目标
解决数据倾斜和维表 Join 两大核心难题。

### 核心学习点
- [ ] 两阶段聚合（Key 加盐）解决热点房间倾斜
- [ ] BroadcastState 广播流维表关联
- [ ] 配置版本管理 + 时间线匹配（时序维表）
- [ ] Doris Aggregate Key / Bitmap 去重
- [ ] 多流 Join（开炮流 + 击杀流 + 配置流）

### 要做的事

#### 2.1 数据生成器扩展
- [ ] 模拟 tp_battle_fire 事件（含热点房间：room_888 数据量是普通房间的 500 倍）
- [ ] 模拟 tp_fish_kill 事件
- [ ] 模拟 tp_config_change 事件（鱼种配置变更，含 effective_time 字段）

#### 2.2 Doris 表设计
- [ ] `room_stats_1min` — Unique Key(stat_date, stat_minute, room_id)，房间分钟级聚合
  - fire_count, kill_count, total_bet, total_reward, player_bitmap(Bitmap UV)
- [ ] `fish_kill_with_config` — Unique Key(event_date, kill_id)，带配置快照的击杀明细
- [ ] `dim_fish_config_history` — Unique Key，鱼种配置变更历史表

#### 2.3 Flink Job 开发
- [ ] Job-4: RoomStatsAgg — 两阶段聚合
  - 第一阶段：keyBy(roomId + salt) → 预聚合（64 路打散）
  - 第二阶段：keyBy(roomId) → 最终聚合（合并 64 份预聚合结果）
  - UV 使用 RoaringBitmap 合并
  - 写入 room_stats_1min
- [ ] Job-5: FishKillEnrich — 广播流维表 Join
  - 主流：tp_fish_kill
  - 广播流：tp_config_change（鱼种配置）
  - BroadcastProcessFunction 关联配置
  - 输出带配置快照的明细到 fish_kill_with_config
- [ ] (可选) Job-6: TemporalConfigJoin — 时序维表 Join
  - 活动规则按 effective_time 匹配事件时间
  - floorEntry 实现时间线查找

#### 2.4 验证
- [ ] 对比加盐前后：倾斜 Task 的处理延迟变化
- [ ] 对比加盐前后：统计结果一致性（明细直算 vs Flink 聚合）
- [ ] 配置变更后，验证新旧事件使用了正确版本的配置

---

## 项目3：数据补算 + 全链路可观测性（高阶）

### 目标
建立生产级故障恢复能力和全链路监控体系。

### 核心学习点
- [ ] 独立补算 Job 设计（隔离 + 时间范围过滤）
- [ ] 补算校验三步法（条数 + 连续性 + 口径）
- [ ] Prometheus + Grafana 监控大盘
- [ ] Kafka Lag / Flink 反压 / Doris 写入延动分析
- [ ] 分级降级策略

### 要做的事

#### 3.1 补算流程
- [ ] BackfillJob — 独立 Flink 补算作业
  - 独立 Consumer Group
  - 按 timestamp 指定消费起止范围
  - 写入临时表 `xxx_backfill`，不直接写主表
  - 复用实时 Job 的处理逻辑（同一套 Function）
- [ ] 补算合并脚本
  - 校验通过后 INSERT INTO SELECT 合并到主表
  - Unique Key 保证幂等，不会翻倍
- [ ] 补算校验 SQL
  - Step 1：条数校验（Kafka offset 差值 vs Doris 条数）
  - Step 2：时间连续性校验（每分钟有数据，无空洞）
  - Step 3：口径一致性校验（补算时段 vs 相邻正常时段指标分布对比）

#### 3.2 可观测性
- [ ] Prometheus + Grafana docker 容器
- [ ] Kafka 指标采集：Consumer Lag、分区消息量、生产 TPS
- [ ] Flink 指标采集：Backpressure、Checkpoint 耗时/失败数、各算子 TPS
- [ ] Doris 指标采集：Stream Load 成功率/延迟、查询 P95、Compaction 积压
- [ ] 自定义指标：端到端延迟（event_time → 可查询 的时间差）
- [ ] 告警规则：Lag > 50万、Checkpoint > 3min、Load 失败率 > 1%、端到端延迟 > 2min

#### 3.3 降级策略实现
- [ ] 通过广播流下发降级指令
- [ ] Level 1：丢弃低价值维度（city, device 等）
- [ ] Level 2：只写聚合表，暂停明细表
- [ ] Level 3：写本地文件兜底，事后批量导入

#### 3.4 故障演练
- [ ] 模拟 Flink Job 停止 3 小时 → 走补算流程 → 验证数据完整
- [ ] 模拟 Doris BE 下线 → 观察写入降级 → BE 恢复后验证数据
- [ ] 模拟 Kafka 分区不均 → 观察 Flink 反压 → 验证告警触发

---

## 项目目录结构

```
realtime-fish-platform/
├── CLAUDE.md                          # 本文件
├── docker/
│   └── docker-compose.yml             # Kafka + Flink + Doris + Prometheus + Grafana
├── datagen/                           # 数据生成器
│   └── src/
├── flink-jobs/                        # Flink 作业代码
│   ├── pom.xml
│   └── src/main/java/com/fish/
│       ├── common/                    # 公共工具（JSON解析、ID生成等）
│       ├── project1/                  # 金币对账相关 Job
│       ├── project2/                  # 房间大盘相关 Job
│       └── project3/                  # 补算 + 可观测性相关 Job
├── doris-ddl/                         # Doris 建表语句
│   ├── project1_coin.sql
│   ├── project2_room.sql
│   └── project3_backfill.sql
├── scripts/                           # 运维脚本
│   ├── backfill.sh                    # 补算启动脚本
│   └── reconciliation.sh             # 对账检查脚本
└── monitoring/                        # 监控配置
    ├── prometheus/
    └── grafana/
```

## 开发顺序

1. **项目1**（2-3周）→ 跑通全链路 + 一致性
2. **项目2**（2-3周）→ 倾斜治理 + 维表Join
3. **项目3**（2周）→ 补算 + 监控 + 降级

## 面试话术

> "我做过一个捕鱼游戏的实时数据平台，日增 20 亿条。
>
> 第一个问题是金币一致性——Flink 重启会重复消费，我用 Doris Unique Key + Sequence Column 做幂等，再配合对账表做双源校验，差异率控制在万分之一以下。
>
> 第二个问题是头部房间数据倾斜——最大的房间数据量是平均值的 500 倍，我用两阶段聚合加盐打散，同时用广播流做鱼种配置的实时关联。
>
> 第三个问题是故障恢复——有一次 Flink 异常停了 3 小时，我设计了独立补算流程，写临时表校验后合并，同时建了全链路监控，从 Kafka Lag 到 Doris 写入延迟，端到端 SLA 控制在 2 分钟以内。"

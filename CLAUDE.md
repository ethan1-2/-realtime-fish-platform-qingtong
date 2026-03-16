# SAAS 游戏支付抽水平台：Kafka + Flink + Doris

## 项目概述

基于 Kafka + Flink + Doris 架构，构建 SAAS 游戏支付抽水平台的实时数仓。
平台服务多个游戏公司（多租户），提供充值/代收/支付回调/补单/退款等能力，通过抽成（抽水）盈利。
数据规模：多表日增十亿级以上，存在乱序、迟到、重复投递、热点租户/热点游戏。

通过 3 个递进式场景，一次练透实时数仓最核心的难点：
幂等去重、事件时间/迟到回补、规则版本化、多租户隔离、倾斜治理、可观测与对账可解释。

## 文档入口（推荐）

- 练习项目列表（从易到难，场景 1 拆成 3 个平滑练习）：`projects/README.md`
- 文档总览与结构说明：`docs/README.md` / `docs/structure.md`
- 桌面场景题与生成器规范的“原始版本”已同步：`docs/reference/`

## 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Kafka | 2.6.0 (Scala 2.13) | 事件总线，至少一次投递，PLAINTEXT 无认证 |
| Flink | 1.15.2 Standalone | 实时计算引擎 |
| Apache Doris | 2.1.7 | 实时 OLAP 数据仓库（从 1.2.2 升级而来） |
| Hadoop | 2.7.3 | 底层存储（HDFS） |
| Java | 1.8.0_144 | Flink Job 开发语言 |
| Maven | 待安装 | 构建工具 |

## 集群信息

### Kafka 集群 (3 Broker)

| 节点 | hostname | IP | CPU | 内存 | 安装路径 |
|------|----------|-----|-----|------|----------|
| kafka1 (操作节点) | kafka1-84239 | 11.26.164.164 | 1核 | 2.67G | /usr/local/kafka_2.13-2.6.0/ |
| kafka2 | kafka2-84239 | 11.99.173.36 | 1核 | 2.67G | /usr/local/kafka_2.13-2.6.0/ |
| kafka3 | kafka3-84239 | 11.99.173.4 | 1核 | 2.67G | /usr/local/kafka_2.13-2.6.0/ |

- Bootstrap Servers: `kafka1-84239:9092,kafka2-84239:9092,kafka3-84239:9092`
- ZooKeeper: `kafka1-84239:2181,kafka2-84239:2181,kafka3-84239:2181`
- 认证：无（PLAINTEXT）
- 日志目录：/opt/kafka_2.13-2.6.0/kafka-logs

### Flink 集群 (Standalone)

| 节点 | 角色 | hostname | IP | CPU | 内存 | 安装路径 |
|------|------|----------|-----|-----|------|----------|
| nn1 (操作节点) | JobManager | nn1-23273 | 11.18.17.7 | 3核 | 3G | /usr/local/flink-1.15.2/ (符号链接), /opt/flink-1.15.2/ |
| nn2 | - | nn2-23273 | 11.26.164.186 | 3核 | 3G | - |
| s1 | TaskManager | s1-23273 | 11.237.80.37 | 2核 | 9.17G | - |
| s2 | TaskManager | s2-23273 | 11.26.164.137 | 2核 | 9.17G | - |
| s3 | TaskManager | s3-23273 | 11.18.17.58 | 2核 | 9.17G | - |

- JobManager RPC: `nn1-23273:6123`
- REST API / Web UI: `http://nn1-23273:8081`
- TaskManager Slots: 3（每个 TM 1 slot）
- TaskManager 内存: 1728m/TM
- JobManager 内存: 1600m

### Doris 集群 (3FE + 3BE)

| 节点 | 角色 | hostname | IP | CPU | 内存 | 安装路径 |
|------|------|----------|-----|-----|------|----------|
| fe1 (Master/操作节点) | FE Master | fe1-47460 | 11.99.173.11 | 3核 | 3G | /usr/local/apache-doris-fe-2.1.7-bin-x64/ |
| fe2 | FE Follower | fe2-47460 | 11.237.80.49 | 3核 | 3G | /usr/local/apache-doris-fe-2.1.7-bin-x64/ |
| fe3 | FE Follower | fe3-47460 | 11.99.173.28 | 3核 | 3G | /usr/local/apache-doris-fe-2.1.7-bin-x64/ |
| be1 | BE | be1-47460 | 11.26.164.150 | 3核 | 9.67G | /usr/local/apache-doris-be-2.1.7-bin-x64/ |
| be2 | BE | be2-47460 | 11.99.173.51 | 3核 | 9.67G | /usr/local/apache-doris-be-2.1.7-bin-x64/ |
| be3 | BE | be3-47460 | 11.26.164.162 | 3核 | 9.67G | /usr/local/apache-doris-be-2.1.7-bin-x64/ |

- FE MySQL 协议: `11.99.173.11:9030` (用户: root, 密码: 12345678)
- FE HTTP: `11.99.173.11:8030`
- BE HTTP: `11.26.164.150:8040`, `11.99.173.51:8040`, `11.26.164.162:8040`
- 元数据目录: /data/doris-meta (FE), 存储目录: /data/storage1 (BE)
- 副本数: 3

### 开发环境（本机）

- 操作系统: CentOS 7 (x86_64)
- JDK: 待安装
- Maven: 待安装
- 开发流程: 本机编写代码 → 本机编译打包 → 提交到 Flink 集群(nn1)运行

## 统一事件字段规范

所有事件必须包含以下公共字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| tenant_id | VARCHAR(32) | 租户ID（游戏公司） |
| event_time | DATETIME(3) | 事件发生时间（业务时间） |
| trace_id | VARCHAR(64) | 全链路追踪ID |
| idempotency_key | VARCHAR(64) | 幂等键（去重主键） |
| rule_version | INT | 规则版本号（抽成/风控规则） |

## Kafka Topic 规划

| Topic | 说明 | 分区键 | 日增量级 |
|-------|------|--------|----------|
| tp_pay_success | 支付成功 | tenant_id + order_id | 10亿+ |
| tp_refund_success | 退款成功 | tenant_id + refund_id | 5000万 |
| tp_chargeback | 拒付/争议 | tenant_id + chargeback_id | 100万 |
| tp_settlement_adjust | 人工调账/补偿 | tenant_id + adjust_id | 10万 |
| tp_take_rate_rule_change | 抽成规则变更 | tenant_id | 低频 |
| tp_user_login | 用户登录/注册 | user_id | 5亿 |
| tp_risk_hit | 风险命中事件（Flink输出） | tenant_id + user_id | 视规则命中量 |
| tp_tenant_throttle | 租户限流控制指令（Flink输出） | tenant_id | 低频 |

---

## 场景 1：多租户实时营收与结算（今天做这个）

### 业务故事

平台接入 N 个租户（游戏公司），每个租户有多个游戏/区服/渠道。平台代收支付后按规则抽成：
- 抽成按租户等级、支付方式、渠道、活动期、阶梯费率等组合计算
- 支付成功后可能发生退款/拒付/补单，回调可能迟到数小时甚至更久
- 运营与财务要求：分钟级看大盘；结算时能逐笔对账，解释"为什么这个租户今天应结算金额是 X"

### 核心学习点

- [ ] Kafka 至少一次 + Flink 重启重放 → Doris 幂等去重
- [ ] 迟到退款/拒付的 watermark + allowed lateness 策略
- [ ] 抽成规则版本化（广播流 vs 时态维表 join）
- [ ] 多租户倾斜治理（头部租户数据量远超其他）
- [ ] 对账表设计：双源校验 + 差异原因可解释

### 要做的事

#### 1.1 基础设施搭建
- [ ] 本机安装 JDK 8 + Maven（编译打包用）
- [ ] 在 Kafka 集群创建所需 Topic（tp_pay_success, tp_refund_success, tp_chargeback, tp_settlement_adjust, tp_take_rate_rule_change）
- [ ] 在 Doris 中创建数据库和表
- [ ] 数据生成器：模拟多租户支付事件流，支持可调 TPS
  - 支持生成：pay_success、refund_success、chargeback、settlement_adjust
  - 支持模拟：迟到退款（event_time 比 kafka produce time 早数小时）
  - 支持模拟：重复投递（同一 idempotency_key 多次发送）
  - 支持模拟：热点租户（tenant_001 数据量是普通租户的 500 倍）

#### 1.2 Doris 表设计

##### 明细审计层（DWD）

**`dwd_payment_detail`** — 支付结算明细表（Unique Key，核心对账表）
```
主键：(event_date, idempotency_key)
必要字段：
  tenant_id, game_id, channel_id, order_id, payment_id
  event_time, event_type (pay/refund/chargeback/adjust)
  amount, currency
  pay_method, pay_channel
  take_rate_rule_version    -- 抽成规则版本号
  take_rate_pct             -- 当时的抽成比例快照
  take_rate_amount          -- 平台抽成金额
  settlement_amount         -- 租户应结算金额 (amount - take_rate_amount)
  trace_id
```
- Unique Key 保证幂等：同一 idempotency_key 写多次只保留一条
- Sequence Column = event_time_ms，迟到的退款/拒付能覆盖更新

**`dwd_dirty_event`** — 脏数据归档表（Duplicate Key）
```
记录所有校验失败的事件，含失败原因字段 dirty_reason
```

##### 聚合指标层（DWS）

**`dws_settlement_minute`** — 分钟级结算聚合（Unique Key）
```
主键：(stat_date, stat_minute, tenant_id, game_id, channel_id, pay_method)
指标字段：
  pay_count, pay_amount (GMV)
  refund_count, refund_amount
  chargeback_count, chargeback_amount
  net_amount (净入金 = pay - refund - chargeback + adjust)
  take_rate_total (平台抽成总额)
  settlement_total (租户应结算总额)
```
- Unique Key：同一分钟+维度组合覆盖写入 → 迟到数据回补时更新而非翻倍

**`dws_reconciliation`** — 对账表（Unique Key）
```
主键：(recon_date, recon_hour, tenant_id)
字段：
  src_pay_count, src_pay_amount       -- 源头统计（Flink 从 Kafka 消费时聚合）
  dst_pay_count, dst_pay_amount       -- 落地统计（Doris 明细表回查聚合）
  diff_count, diff_amount             -- 差异
  diff_reason                         -- 差异原因分类
  recon_status (0-未对账/1-一致/2-有差异/3-已修正)
```

#### 1.3 Flink Job 开发

**Job-1: PaymentDetailETL** — 支付明细清洗入库
- 消费 tp_pay_success, tp_refund_success, tp_chargeback, tp_settlement_adjust
- 多源合并（UNION）
- JSON 解析 → 字段校验（脏数据走侧输出流 → dwd_dirty_event）
- 幂等 ID：使用业务 idempotency_key（由上游支付系统生成）
- 广播流 Join：关联 tp_take_rate_rule_change 获取当前抽成规则
  - BroadcastProcessFunction
  - 规则按 tenant_id + effective_time 维护时间线
  - 事件按 event_time 匹配对应版本规则（floorEntry）
  - 输出：带规则快照的结算明细 → dwd_payment_detail
- Doris Sink：Stream Load，Unique Key 幂等写入

**Job-2: SettlementAggregation** — 分钟级结算聚合
- 消费 dwd_payment_detail（或直接从 Kafka 消费后聚合）
- Watermark：forBoundedOutOfOrderness(5 seconds)
- 窗口：TumblingEventTimeWindows(1 minute)
- Allowed Lateness：6 hours（退款/拒付可能迟到数小时）
  - 迟到数据触发窗口更新 → 覆盖写入 dws_settlement_minute（Unique Key 不翻倍）
- 多租户倾斜治理：两阶段聚合
  - 第一阶段：keyBy(tenant_id + salt) → 预聚合
  - 第二阶段：keyBy(tenant_id, game_id, channel_id, pay_method) → 最终聚合
- 输出 → dws_settlement_minute

**Job-3: SettlementReconciliation** — 实时对账
- 按 tenant_id + hour 窗口聚合 Kafka 源头统计
- 写入 dws_reconciliation 的 src 列
- 定时 Doris SQL 任务回填 dst 列（从 dwd_payment_detail 聚合）
- 计算差异，标记异常租户

#### 1.4 验证与调优

- [ ] **幂等验证**：模拟 Flink 重启（kill TaskManager），验证 Checkpoint 恢复后 Doris 无重复数据
- [ ] **迟到回补验证**：发送 event_time 为 3 小时前的退款事件，验证：
  - dwd_payment_detail 中该订单状态被更新
  - dws_settlement_minute 中对应分钟的指标被修正（净入金减少）
  - 指标不翻倍（Unique Key 覆盖写入）
- [ ] **规则版本验证**：变更某租户抽成规则，验证变更前后的订单使用了正确版本的规则
- [ ] **对账验证**：挑 100 笔订单，在 Doris 查到完整链路（原始金额→抽成规则版本→抽成金额→应结算金额→退款修正）
- [ ] **脏数据验证**：发送缺字段/金额为负的事件，验证进入 dwd_dirty_event
- [ ] **Stream Load 调优**：buffer size、interval、并行度调参

---

## 场景 2：平台级资金风控与异常告警（第二个做）

### 业务故事

平台最大风险来自资金侧异常：盗刷、羊毛党、异常退款率、某渠道洗量、某租户短时间内交易激增。
平台提供统一实时风控能力，不同租户可有不同阈值、黑白名单与处置策略。

### 核心学习点

- [ ] 多流关联：支付流 + 登录/设备流的 Interval Join
- [ ] 状态规模控制：特征窗口 State TTL 设计
- [ ] 倾斜与攻击流量：黑产流量热点隔离
- [ ] 风控规则可回溯：每条命中记录 rule_version + evidence
- [ ] 可观测性：区分"规则命中上升"与"数据延迟/重复导致假峰值"

### 要做的事

#### 2.1 数据生成器扩展
- [ ] 模拟 tp_user_login 事件（含 device_id, ip, geo）
- [ ] 模拟攻击流量：同 IP 多账号、同设备跨租户、短时间密集支付

#### 2.2 Doris 表设计
- [ ] `dwd_risk_hit_detail` — 风险命中明细（Unique Key）
  - risk_type, rule_version, evidence(JSON), hit_time, dispose_action
- [ ] `dws_risk_distribution` — 风险分布聚合（Unique Key，按小时/天/租户/渠道）

#### 2.3 Flink Job 开发
- [ ] Job-4: RiskDetection — 实时风控
  - 3 条规则（频次类 + 金额类 + 关联类）
  - 每条规则输出 evidence 字段
  - 支付流 + 登录流 Interval Join
  - 黑白名单广播流
  - 输出 risk_hit 事件到 tp_risk_hit
- [ ] Job-5: RiskAggregation — 风险聚合入库
  - 消费 tp_risk_hit → dwd_risk_hit_detail + dws_risk_distribution

#### 2.4 验证
- [ ] 构造重复事件 + 迟到事件，风控命中数不因重放翻倍
- [ ] 按单用户/单设备回放最近 24 小时风险证据链
- [ ] State TTL 验证：过期状态被清理，内存不爆

---

## 场景 3：SAAS 平台计费与配额治理（第三个做）

### 业务故事

除了抽成，平台对租户收 SAAS 服务费：按 API 调用量、成功支付笔数、风控调用次数等计费。
同时必须做配额与隔离：某租户流量洪峰不能拖垮整个平台，超配额要限流或降级。

### 核心学习点

- [ ] 计费口径与幂等：重试/补单如何不重复计费
- [ ] Doris 分区冷热分层：近 7 天高频查询 vs 历史低频
- [ ] 实时配额检测 + 限流控制指令输出
- [ ] 端到端对账：证明账单不重不漏

### 要做的事

#### 3.1 Doris 表设计
- [ ] `dws_tenant_daily_bill` — 租户日账单聚合（Unique Key）
  - 各计费项累计（支付笔数、API 调用量、风控调用量等）
  - 支持出月账单 + 明细追溯
- [ ] `dwd_billing_detail` — 计费明细（Unique Key，用于争议处理）

#### 3.2 Flink Job 开发
- [ ] Job-6: TenantBilling — 租户计费
  - 实时累计各计费项
  - 幂等去重：同一请求不重复计费
- [ ] Job-7: QuotaEnforcement — 配额检测
  - 实时累计租户用量
  - 超限 → 输出 tp_tenant_throttle 控制事件
  - 分钟级发现 + 触发

#### 3.3 验证
- [ ] 制造租户突发流量，分钟级触发限流控制事件
- [ ] 账单累计不丢不重（抽样对账）
- [ ] Doris 冷热分层验证

---

## 项目目录结构

```
realtime-fish-platform/
├── CLAUDE.md                              # 本文件（项目规划 + 集群信息）
├── datagen/                               # 数据生成器（独立 jar，可在任意节点运行）
│   ├── pom.xml
│   └── src/main/java/com/saas/datagen/
│       ├── PaymentEventGenerator.java     # 支付事件生成（含迟到/重复/热点模拟）
│       ├── UserLoginGenerator.java        # 登录事件生成
│       └── RuleChangeGenerator.java       # 规则变更事件生成
├── flink-jobs/                            # Flink 作业代码
│   ├── pom.xml
│   └── src/main/java/com/saas/flink/
│       ├── common/                        # 公共工具
│       │   ├── JsonParser.java
│       │   ├── IdempotencyKeyGenerator.java
│       │   └── DorisStreamLoadSink.java
│       ├── scene1/                        # 场景1：营收与结算
│       │   ├── PaymentDetailETL.java
│       │   ├── SettlementAggregation.java
│       │   └── SettlementReconciliation.java
│       ├── scene2/                        # 场景2：风控与告警
│       │   ├── RiskDetection.java
│       │   └── RiskAggregation.java
│       └── scene3/                        # 场景3：计费与配额
│           ├── TenantBilling.java
│           └── QuotaEnforcement.java
├── doris-ddl/                             # Doris 建表语句
│   ├── scene1_settlement.sql
│   ├── scene2_risk.sql
│   └── scene3_billing.sql
├── scripts/                               # 运维与部署脚本
│   ├── deploy-flink-job.sh               # 编译并提交 Flink Job 到集群
│   ├── backfill.sh
│   └── reconciliation_check.sql
└── monitoring/                            # 监控配置（场景3使用）
    ├── prometheus/
    └── grafana/
```

## 部署流程

```
本机 (编译打包)                     nn1-23273 (Flink JobManager)
┌──────────────┐   scp jar    ┌──────────────────────────┐
│ mvn package  │ ───────────→ │ flink run -d xxx.jar     │
│ 生成 fat jar │              │ 提交到 Standalone 集群     │
└──────────────┘              └──────────────────────────┘
                                        │
                              ┌─────────┼─────────┐
                              ▼         ▼         ▼
                           s1-23273  s2-23273  s3-23273
                           (TM)      (TM)      (TM)
```

## 开发顺序

1. **场景 1**（2-3周）→ 幂等去重 + 迟到回补 + 规则版本化（实时数仓硬骨头）
2. **场景 2**（2-3周）→ 在同一条支付明细上叠加风控特征与告警输出（状态与倾斜治理）
3. **场景 3**（2周）→ 补齐多租户治理（计费、配额、隔离、成本控制）

## 面试话术

> "我做过一个 SAAS 游戏支付抽水平台的实时数仓，服务数百个游戏公司租户，日增十亿级事件。
>
> 第一个问题是结算一致性——Kafka 至少一次投递加上 Flink 重启重放，支付回调和补单也可能重复上报。我用 Doris Unique Key + Sequence Column 做幂等写入，退款/拒付迟到数小时通过 allowed lateness 回补窗口聚合，Unique Key 覆盖写入保证不翻倍。抽成规则版本化用广播流时间线匹配，每笔订单都能追溯用了哪版规则。配合双源对账表，差异率控制在万分之一以下。
>
> 第二个问题是实时风控——多流关联（支付+登录+设备指纹），State TTL 控制内存，黑产热点流量用加盐打散。每条风险命中都带 evidence 和 rule_version，运营能按用户/设备回放 24 小时证据链。
>
> 第三个问题是多租户治理——实时计费+配额检测，头部租户突发流量分钟级限流，账单不重不漏可审计。Doris 冷热分层控制存储成本。"

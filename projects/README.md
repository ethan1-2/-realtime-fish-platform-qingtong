# Projects（练习项目）

推荐顺序：先把场景 1 拆成 3 个“平滑升档”练习（P00/P01/P02），再进入完整场景 1/2/3。

## P00-P02：场景 1 的平滑练习

- P00: `p00_ingest_pay_success/`  
  最小闭环：单 topic 明细入仓 + 脏数据归档（至少落 1 张 Doris 表）
- P01: `p01_minute_gmv/`  
  小升级：事件时间 1 分钟窗口 GMV 大盘（至少落 1 张 Doris 表）
- P02: `p02_net_minute_lateness/`  
  再升级：多 topic 合流 + allowed lateness 回补净入金（至少落 1 张 Doris 表）

## 场景 1-3：大项目

- 场景 1: `scene1_settlement/`  
  幂等去重 + 迟到回补 + 抽成规则版本化 + 多租户倾斜 + 可对账解释
- 场景 2: `scene2_risk/`  
  平台级资金风控与异常告警（状态/多流关联/可观测）
- 场景 3: `scene3_billing/`  
  SAAS 计费与配额治理（计费口径/幂等/隔离/控制指令）

## 共同依赖

所有项目默认复用：

- 公共产数：`../datagen/`
- Doris DDL：`../doris-ddl/scene1_settlement.sql`
- 共享约定：`_shared/README.md`


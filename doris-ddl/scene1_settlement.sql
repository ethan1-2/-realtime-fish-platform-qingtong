-- ============================================================
-- SAAS 游戏支付抽水平台 - 场景1 Doris DDL
-- 数据库: saas_payment
-- Doris 版本: 2.1.7
-- ============================================================

CREATE DATABASE IF NOT EXISTS saas_payment;
USE saas_payment;

-- ============================================================
-- DWD 层：明细审计层
-- ============================================================

-- 核心结算明细表
-- 每一笔 支付/退款/拒付/调账 都是一行
-- Unique Key 保证幂等：同一 idempotency_key 写多次只保留一条
CREATE TABLE IF NOT EXISTS dwd_payment_detail (
    event_date          DATE            NOT NULL    COMMENT '事件日期(分区键)',
    idempotency_key     VARCHAR(64)     NOT NULL    COMMENT '幂等键(去重主键)',
    tenant_id           VARCHAR(32)     NOT NULL    COMMENT '租户ID',
    app_id              VARCHAR(32)     NOT NULL    COMMENT '应用/游戏ID',
    order_id            VARCHAR(64)     NOT NULL    COMMENT '业务订单ID',
    payment_id          VARCHAR(64)                 COMMENT '支付ID(成功支付才有)',
    refund_id           VARCHAR(64)                 COMMENT '退款ID',
    chargeback_id       VARCHAR(64)                 COMMENT '拒付ID',
    adjust_id           VARCHAR(64)                 COMMENT '调账ID',
    user_id             VARCHAR(64)     NOT NULL    COMMENT '用户ID',
    event_type          VARCHAR(32)     NOT NULL    COMMENT '事件类型: pay_success/refund_success/chargeback/settlement_adjust',
    amount_minor        BIGINT          NOT NULL    COMMENT '金额(分), 退款/拒付为负数',
    currency            VARCHAR(8)      DEFAULT 'CNY' COMMENT '币种',
    channel_id          VARCHAR(32)                 COMMENT '渠道ID',
    pay_method          VARCHAR(32)                 COMMENT '支付方式: wx/alipay/card/h5/quickpay',
    psp                 VARCHAR(32)                 COMMENT '第三方支付通道',
    region              VARCHAR(32)                 COMMENT '地区',
    take_rate_rule_version INT                      COMMENT '抽成规则版本号',
    take_rate_pct       DECIMAL(10,6)               COMMENT '抽成比例快照(如0.03=3%)',
    take_rate_fixed_fee BIGINT          DEFAULT 0   COMMENT '固定费用(分)',
    take_rate_amount    BIGINT                      COMMENT '平台抽成金额(分)',
    settlement_amount   BIGINT                      COMMENT '租户应结算金额(分)',
    ref_type            VARCHAR(32)                 COMMENT '关联类型(payment/refund/none)',
    ref_id              VARCHAR(64)                 COMMENT '关联ID',
    adjust_reason       VARCHAR(256)                COMMENT '调账原因',
    refund_reason       VARCHAR(256)                COMMENT '退款原因',
    trace_id            VARCHAR(64)                 COMMENT '全链路追踪ID',
    event_time          DATETIME(3)     NOT NULL    COMMENT '事件发生时间(业务时间)',
    ingest_time         DATETIME(3)                 COMMENT '进入Kafka的时间',
    process_time        DATETIME(3)                 COMMENT 'Flink处理时间',
    data_quality_flag   VARCHAR(32)     DEFAULT 'normal' COMMENT '数据质量标记: normal/dirty/suspect'
)
UNIQUE KEY(event_date, idempotency_key)
PARTITION BY RANGE(event_date) ()
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16",
    "replication_num" = "3",
    "enable_unique_key_merge_on_write" = "true"
);

-- 脏数据归档表 (Duplicate Key，允许重复，只是存档)
CREATE TABLE IF NOT EXISTS dwd_dirty_event (
    event_date          DATE            NOT NULL    COMMENT '事件日期',
    receive_time        DATETIME(3)     NOT NULL    COMMENT '接收时间',
    dirty_reason        VARCHAR(256)    NOT NULL    COMMENT '脏数据原因',
    event_type          VARCHAR(32)                 COMMENT '事件类型',
    tenant_id           VARCHAR(32)                 COMMENT '租户ID',
    raw_payload         TEXT                        COMMENT '原始JSON'
)
DUPLICATE KEY(event_date, receive_time)
PARTITION BY RANGE(event_date) ()
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "replication_num" = "3"
);

-- 抽成规则历史维表
CREATE TABLE IF NOT EXISTS dim_take_rate_rule (
    tenant_id           VARCHAR(32)     NOT NULL    COMMENT '租户ID',
    rule_version        INT             NOT NULL    COMMENT '规则版本号(单调递增)',
    rule_id             VARCHAR(64)     NOT NULL    COMMENT '规则ID',
    effective_time      DATETIME(3)     NOT NULL    COMMENT '规则生效时间',
    pay_method          VARCHAR(32)     DEFAULT '*' COMMENT '支付方式(*=全部)',
    channel_id          VARCHAR(32)     DEFAULT '*' COMMENT '渠道(*=全部)',
    rate_type           VARCHAR(16)     NOT NULL    COMMENT '规则类型: percent/fixed/tiered',
    rate_pct            DECIMAL(10,6)               COMMENT '比例抽成(如0.03=3%)',
    fixed_fee           BIGINT          DEFAULT 0   COMMENT '固定费用(分)',
    cap_amount          BIGINT                      COMMENT '抽成上限(分,封顶)',
    floor_amount        BIGINT                      COMMENT '抽成下限(分,封底)',
    publish_time        DATETIME(3)                 COMMENT '规则发布时间(可能晚于effective_time)',
    operator            VARCHAR(64)                 COMMENT '操作人',
    create_time         DATETIME(3)     DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间'
)
UNIQUE KEY(tenant_id, rule_version)
DISTRIBUTED BY HASH(tenant_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3",
    "enable_unique_key_merge_on_write" = "true"
);


-- ============================================================
-- DWS 层：汇总指标层
-- ============================================================

-- 分钟级结算聚合表 (运营实时大盘)
-- Unique Key: 迟到数据覆盖写入不翻倍
CREATE TABLE IF NOT EXISTS dws_settlement_minute (
    stat_date           DATE            NOT NULL    COMMENT '统计日期',
    stat_minute         DATETIME        NOT NULL    COMMENT '统计分钟(截断到分钟)',
    tenant_id           VARCHAR(32)     NOT NULL    COMMENT '租户ID',
    app_id              VARCHAR(32)     NOT NULL    COMMENT '应用ID',
    channel_id          VARCHAR(32)     NOT NULL    COMMENT '渠道ID',
    pay_method          VARCHAR(32)     NOT NULL    COMMENT '支付方式',
    pay_count           BIGINT          DEFAULT 0   COMMENT '支付笔数',
    pay_amount          BIGINT          DEFAULT 0   COMMENT '支付金额(分)',
    refund_count        BIGINT          DEFAULT 0   COMMENT '退款笔数',
    refund_amount       BIGINT          DEFAULT 0   COMMENT '退款金额(分,正数)',
    chargeback_count    BIGINT          DEFAULT 0   COMMENT '拒付笔数',
    chargeback_amount   BIGINT          DEFAULT 0   COMMENT '拒付金额(分,正数)',
    adjust_count        BIGINT          DEFAULT 0   COMMENT '调账笔数',
    adjust_amount       BIGINT          DEFAULT 0   COMMENT '调账金额(分,可正可负)',
    net_amount          BIGINT          DEFAULT 0   COMMENT '净入金(分)=pay-refund-chargeback+adjust',
    take_rate_total     BIGINT          DEFAULT 0   COMMENT '平台抽成总额(分)',
    settlement_total    BIGINT          DEFAULT 0   COMMENT '租户应结算总额(分)',
    update_time         DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '最后更新时间'
)
UNIQUE KEY(stat_date, stat_minute, tenant_id, app_id, channel_id, pay_method)
PARTITION BY RANGE(stat_date) ()
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16",
    "replication_num" = "3",
    "enable_unique_key_merge_on_write" = "true"
);

-- 小时级结算聚合表 (财务报表)
CREATE TABLE IF NOT EXISTS dws_settlement_hour (
    stat_date           DATE            NOT NULL    COMMENT '统计日期',
    stat_hour           TINYINT         NOT NULL    COMMENT '统计小时(0-23)',
    tenant_id           VARCHAR(32)     NOT NULL    COMMENT '租户ID',
    app_id              VARCHAR(32)     NOT NULL    COMMENT '应用ID',
    pay_count           BIGINT          DEFAULT 0,
    pay_amount          BIGINT          DEFAULT 0,
    refund_count        BIGINT          DEFAULT 0,
    refund_amount       BIGINT          DEFAULT 0,
    chargeback_count    BIGINT          DEFAULT 0,
    chargeback_amount   BIGINT          DEFAULT 0,
    adjust_count        BIGINT          DEFAULT 0,
    adjust_amount       BIGINT          DEFAULT 0,
    net_amount          BIGINT          DEFAULT 0,
    take_rate_total     BIGINT          DEFAULT 0,
    settlement_total    BIGINT          DEFAULT 0,
    update_time         DATETIME        DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(stat_date, stat_hour, tenant_id, app_id)
PARTITION BY RANGE(stat_date) ()
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-90",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8",
    "replication_num" = "3",
    "enable_unique_key_merge_on_write" = "true"
);

-- 对账表 (双源校验)
CREATE TABLE IF NOT EXISTS dws_reconciliation (
    recon_date          DATE            NOT NULL    COMMENT '对账日期',
    recon_hour          TINYINT         NOT NULL    COMMENT '对账小时',
    tenant_id           VARCHAR(32)     NOT NULL    COMMENT '租户ID',
    src_pay_count       BIGINT          DEFAULT 0   COMMENT '源头-支付笔数(Flink从Kafka消费时统计)',
    src_pay_amount      BIGINT          DEFAULT 0   COMMENT '源头-支付金额',
    src_refund_count    BIGINT          DEFAULT 0   COMMENT '源头-退款笔数',
    src_refund_amount   BIGINT          DEFAULT 0   COMMENT '源头-退款金额',
    src_event_count     BIGINT          DEFAULT 0   COMMENT '源头-总事件数',
    dst_pay_count       BIGINT                      COMMENT '落地-支付笔数(Doris回查)',
    dst_pay_amount      BIGINT                      COMMENT '落地-支付金额',
    dst_refund_count    BIGINT                      COMMENT '落地-退款笔数',
    dst_refund_amount   BIGINT                      COMMENT '落地-退款金额',
    dst_event_count     BIGINT                      COMMENT '落地-总事件数',
    diff_pay_count      BIGINT                      COMMENT '差异-支付笔数',
    diff_pay_amount     BIGINT                      COMMENT '差异-支付金额',
    diff_reason         VARCHAR(128)                COMMENT '差异原因分类',
    recon_status        TINYINT         DEFAULT 0   COMMENT '0-未对账 1-一致 2-有差异 3-已修正',
    update_time         DATETIME        DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(recon_date, recon_hour, tenant_id)
PARTITION BY RANGE(recon_date) ()
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-90",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8",
    "replication_num" = "3",
    "enable_unique_key_merge_on_write" = "true"
);


-- ============================================================
-- ADS 层：应用视图（不建表，用 View）
-- ============================================================

-- 租户日结算汇总视图
CREATE VIEW IF NOT EXISTS v_tenant_daily_summary AS
SELECT
    stat_date,
    tenant_id,
    SUM(pay_count) AS pay_count,
    SUM(pay_amount) AS pay_amount,
    SUM(refund_count) AS refund_count,
    SUM(refund_amount) AS refund_amount,
    SUM(chargeback_count) AS chargeback_count,
    SUM(chargeback_amount) AS chargeback_amount,
    SUM(adjust_count) AS adjust_count,
    SUM(adjust_amount) AS adjust_amount,
    SUM(net_amount) AS net_amount,
    SUM(take_rate_total) AS take_rate_total,
    SUM(settlement_total) AS settlement_total
FROM dws_settlement_minute
GROUP BY stat_date, tenant_id;

-- 对账差异报告视图
CREATE VIEW IF NOT EXISTS v_reconciliation_diff AS
SELECT
    recon_date,
    recon_hour,
    tenant_id,
    src_pay_count,
    dst_pay_count,
    src_pay_amount,
    dst_pay_amount,
    diff_pay_count,
    diff_pay_amount,
    diff_reason,
    recon_status,
    CASE
        WHEN diff_pay_count > 0 AND diff_pay_amount > 0 THEN 'data_loss'
        WHEN diff_pay_count < 0 AND diff_pay_amount < 0 THEN 'data_duplicate'
        WHEN diff_pay_count = 0 AND diff_pay_amount != 0 THEN 'amount_mismatch'
        WHEN recon_status = 0 THEN 'pending'
        ELSE 'unknown'
    END AS diff_analysis
FROM dws_reconciliation
WHERE recon_status = 2;

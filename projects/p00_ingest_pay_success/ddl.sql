-- ============================================================
-- P00: pay_success 明细入库 - 专用 DDL
-- 数据库: saas_payment
-- 表前缀: p00_
-- ============================================================

CREATE DATABASE IF NOT EXISTS saas_payment;
USE saas_payment;

-- P00 支付明细表 (只写 pay_success)
-- Unique Key 保证幂等: 同一 idempotency_key 写多次只保留一条
CREATE TABLE IF NOT EXISTS p00_payment_detail (
    event_date          DATE            NOT NULL    COMMENT '事件日期(分区键)',
    idempotency_key     VARCHAR(64)     NOT NULL    COMMENT '幂等键(去重主键)',
    tenant_id           VARCHAR(32)     NOT NULL    COMMENT '租户ID',
    app_id              VARCHAR(32)     NOT NULL    COMMENT '应用/游戏ID',
    order_id            VARCHAR(64)     NOT NULL    COMMENT '业务订单ID',
    payment_id          VARCHAR(64)                 COMMENT '支付ID',
    user_id             VARCHAR(64)     NOT NULL    COMMENT '用户ID',
    event_type          VARCHAR(32)     NOT NULL    COMMENT '事件类型(此表只有pay_success)',
    amount_minor        BIGINT          NOT NULL    COMMENT '金额(分)',
    currency            VARCHAR(8)                  COMMENT '币种',
    channel_id          VARCHAR(32)                 COMMENT '渠道ID',
    pay_method          VARCHAR(32)                 COMMENT '支付方式: wx/alipay/card/h5/quickpay',
    psp                 VARCHAR(32)                 COMMENT '第三方支付通道',
    region              VARCHAR(32)                 COMMENT '地区',
    take_rate_rule_version INT                      COMMENT '抽成规则版本号',
    take_rate_pct       DECIMAL(10,6)               COMMENT '抽成比例快照',
    take_rate_amount    BIGINT                      COMMENT '平台抽成金额(分)',
    settlement_amount   BIGINT                      COMMENT '租户应结算金额(分)',
    trace_id            VARCHAR(64)                 COMMENT '全链路追踪ID',
    event_time          DATETIME(3)     NOT NULL    COMMENT '事件发生时间(业务时间)',
    ingest_time         DATETIME(3)                 COMMENT '进入Kafka的时间',
    process_time        DATETIME(3)                 COMMENT 'Flink处理时间',
    data_quality_flag   VARCHAR(32)                 COMMENT '数据质量标记'
)
UNIQUE KEY(event_date, idempotency_key)
PARTITION BY RANGE(event_date) ()
DISTRIBUTED BY HASH(idempotency_key) BUCKETS 16
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

-- P00 脏数据归档表 (Duplicate Key, 允许重复)
CREATE TABLE IF NOT EXISTS p00_dirty_event (
    event_date          DATE            NOT NULL    COMMENT '事件日期',
    receive_time        DATETIME(3)     NOT NULL    COMMENT '接收时间',
    dirty_reason        VARCHAR(256)    NOT NULL    COMMENT '脏数据原因',
    event_type          VARCHAR(32)                 COMMENT '事件类型',
    tenant_id           VARCHAR(32)                 COMMENT '租户ID',
    raw_payload         TEXT                        COMMENT '原始JSON'
)
DUPLICATE KEY(event_date, receive_time)
PARTITION BY RANGE(event_date) ()
DISTRIBUTED BY HASH(event_date) BUCKETS 4
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "4",
    "replication_num" = "3"
);

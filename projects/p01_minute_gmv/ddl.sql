CREATE DATABASE IF NOT EXISTS saas_payment;
USE saas_payment;

CREATE TABLE IF NOT EXISTS p01_settlement_minute (
    stat_date        DATE        NOT NULL COMMENT '统计日期',
    stat_minute      DATETIME    NOT NULL COMMENT '统计分钟',
    tenant_id        VARCHAR(32) NOT NULL COMMENT '租户ID',
    app_id           VARCHAR(32) NOT NULL COMMENT '应用ID',
    channel_id       VARCHAR(32) NOT NULL COMMENT '渠道ID',
    pay_method       VARCHAR(32) NOT NULL COMMENT '支付方式',
    pay_count        BIGINT      NOT NULL COMMENT '支付笔数',
    pay_amount       BIGINT      NOT NULL COMMENT '支付金额(分)',
    net_amount       BIGINT      NOT NULL COMMENT '净入金(分)',
    update_time      DATETIME    NOT NULL COMMENT '最后更新时间'
)
UNIQUE KEY(stat_date, stat_minute, tenant_id, app_id, channel_id, pay_method)
PARTITION BY RANGE(stat_date) ()
DISTRIBUTED BY HASH(tenant_id, app_id, channel_id, pay_method) BUCKETS 16
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


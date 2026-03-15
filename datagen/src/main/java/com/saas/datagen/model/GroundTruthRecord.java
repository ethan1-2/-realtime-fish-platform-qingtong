package com.saas.datagen.model;

/**
 * 真账本记录 - 用于事后对账验收
 * 每笔订单生成后记录到真账本,最终按 tenant_id+date 汇总
 */
public class GroundTruthRecord {
    public String tenantId;
    public String appId;
    public String orderId;
    public String paymentId;
    public String eventType;            // pay_success / refund_success / chargeback / settlement_adjust
    public long amountMinor;            // 原始金额(正数),退款拒付也是正数
    public int ruleVersion;             // 匹配到的规则版本
    public double ratePct;              // 抽成比例
    public long platformFee;            // 平台抽成
    public long settlementAmount;       // 租户结算
    public long eventTimeMs;            // 事件时间
    public String date;                 // yyyy-MM-dd

    public GroundTruthRecord() {}
}

package com.saas.datagen.model;

import java.util.Map;
import java.util.LinkedHashMap;

/**
 * 通用事件模型 - 所有事件共用此结构,序列化为 JSON 写入 Kafka
 */
public class PaymentEvent {
    // 公共字段
    public String eventType;          // pay_success / refund_success / chargeback / settlement_adjust / take_rate_rule_change
    public String tenantId;
    public String appId;
    public String orderId;
    public String paymentId;
    public String refundId;
    public String chargebackId;
    public String adjustId;
    public String userId;
    public String idempotencyKey;

    // 金额
    public Long amountMinor;          // 金额(分)
    public String currency;

    // 维度
    public String channelId;
    public String payMethod;
    public String psp;
    public String region;

    // 抽成
    public Integer takeRateRuleVersion;
    public Double takeRatePct;
    public Long takeRateFixedFee;
    public Long takeRateAmount;       // 平台抽成(分)
    public Long settlementAmount;     // 租户应结算(分)

    // 退款/调账
    public String refType;
    public String refId;
    public String adjustReason;
    public String refundReason;

    // 规则变更专用
    public String ruleId;
    public Integer ruleVersion;
    public Long effectiveTime;
    public Map<String, Object> rulePayload;

    // 时间
    public Long eventTime;            // 业务时间(ms)
    public Long ingestTime;           // 进入 Kafka 的时间(ms)
    public String traceId;

    // 数据质量
    public String dataQualityFlag;    // normal / dirty / suspect

    public PaymentEvent() {
        this.currency = "CNY";
        this.dataQualityFlag = "normal";
    }

    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        putIfNotNull(m, "event_type", eventType);
        putIfNotNull(m, "tenant_id", tenantId);
        putIfNotNull(m, "app_id", appId);
        putIfNotNull(m, "order_id", orderId);
        putIfNotNull(m, "payment_id", paymentId);
        putIfNotNull(m, "refund_id", refundId);
        putIfNotNull(m, "chargeback_id", chargebackId);
        putIfNotNull(m, "adjust_id", adjustId);
        putIfNotNull(m, "user_id", userId);
        putIfNotNull(m, "idempotency_key", idempotencyKey);
        putIfNotNull(m, "amount_minor", amountMinor);
        putIfNotNull(m, "currency", currency);
        putIfNotNull(m, "channel_id", channelId);
        putIfNotNull(m, "pay_method", payMethod);
        putIfNotNull(m, "psp", psp);
        putIfNotNull(m, "region", region);
        putIfNotNull(m, "take_rate_rule_version", takeRateRuleVersion);
        putIfNotNull(m, "take_rate_pct", takeRatePct);
        putIfNotNull(m, "take_rate_fixed_fee", takeRateFixedFee);
        putIfNotNull(m, "take_rate_amount", takeRateAmount);
        putIfNotNull(m, "settlement_amount", settlementAmount);
        putIfNotNull(m, "ref_type", refType);
        putIfNotNull(m, "ref_id", refId);
        putIfNotNull(m, "adjust_reason", adjustReason);
        putIfNotNull(m, "refund_reason", refundReason);
        putIfNotNull(m, "rule_id", ruleId);
        putIfNotNull(m, "rule_version", ruleVersion);
        putIfNotNull(m, "effective_time", effectiveTime);
        putIfNotNull(m, "rule_payload", rulePayload);
        putIfNotNull(m, "event_time", eventTime);
        putIfNotNull(m, "ingest_time", ingestTime);
        putIfNotNull(m, "trace_id", traceId);
        putIfNotNull(m, "data_quality_flag", dataQualityFlag);
        return m;
    }

    private void putIfNotNull(Map<String, Object> m, String key, Object value) {
        if (value != null) {
            m.put(key, value);
        }
    }
}

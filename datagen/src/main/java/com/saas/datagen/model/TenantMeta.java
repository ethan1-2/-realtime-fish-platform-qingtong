package com.saas.datagen.model;

/**
 * 租户元数据
 */
public class TenantMeta {
    public String tenantId;
    public String[] appIds;
    public String[] allowedPayMethods;   // 该租户开通的支付方式(子集)
    public double refundRateMultiplier;  // 退款率倍率(>1 表示退款率偏高)
    public double trafficShare;          // 该租户的流量占比
    public int initialRuleVersion;       // 初始规则版本

    public TenantMeta(String tenantId, String[] appIds, String[] allowedPayMethods,
                      double refundRateMultiplier, double trafficShare, int initialRuleVersion) {
        this.tenantId = tenantId;
        this.appIds = appIds;
        this.allowedPayMethods = allowedPayMethods;
        this.refundRateMultiplier = refundRateMultiplier;
        this.trafficShare = trafficShare;
        this.initialRuleVersion = initialRuleVersion;
    }
}

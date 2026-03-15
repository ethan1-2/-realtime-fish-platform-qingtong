package com.saas.datagen.model;

/**
 * 抽成规则快照
 */
public class TakeRateRule {
    public String tenantId;
    public int ruleVersion;
    public String ruleId;
    public long effectiveTimeMs;         // 生效时间
    public String payMethod;             // * = 全部
    public String channelId;             // * = 全部
    public String rateType;              // percent / fixed / tiered
    public double ratePct;               // 比例(如 0.03)
    public long fixedFee;                // 固定费(分)
    public long capAmount;               // 封顶(分)
    public long floorAmount;             // 封底(分)
    public long publishTimeMs;           // 发布时间(可能 > effectiveTime)

    /**
     * 计算抽成金额(分)
     */
    public long calculateFee(long amountMinor) {
        long fee;
        if ("fixed".equals(rateType)) {
            fee = fixedFee;
        } else {
            // percent 或 tiered (简化: 都按比例)
            fee = Math.round(amountMinor * ratePct);
            fee += fixedFee;  // 可能有比例+固定费组合
        }
        // 封底
        if (floorAmount > 0 && fee < floorAmount) {
            fee = floorAmount;
        }
        // 封顶
        if (capAmount > 0 && fee > capAmount) {
            fee = capAmount;
        }
        return fee;
    }
}

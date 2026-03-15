package com.saas.datagen.config;

import java.util.*;

/**
 * 数据生成器全局配置 - 所有比例与分布参数化
 * 可通过 YAML/JSON 覆盖默认值
 */
public class GenConfig {

    // === 随机种子(可重复) ===
    public long seed = 42L;

    // === 规模 ===
    public int tenantCount = 100;                    // 租户数
    public int maxAppsPerTenant = 10;                // 每租户最大游戏/应用数
    public int totalOrdersPerDay = 500_000;          // 每天总订单数(调试用,生产可调大)
    public int daysToGenerate = 1;                   // 生成天数

    // === 租户倾斜(强长尾) ===
    public double top1TenantShare = 0.30;            // Top1 租户占 30%
    public double top5TenantShare = 0.60;            // Top5 租户占 60%
    public double top1AppShareInTenant = 0.45;       // 租户内 Top1 app 占 45%

    // === 金额分布(分) ===
    public long[] commonAmounts = {
        600, 3000, 6800, 12800, 32800, 64800, 9800, 19800, 4800, 1200, 30000, 50000
    };
    public double commonAmountRatio = 0.93;          // 93% 落在常见档位
    public long customAmountMin = 100;               // 自定义金额下限(1元)
    public long customAmountMax = 100000;            // 自定义金额上限(1000元)

    // === 支付方式/渠道/PSP ===
    public String[] payMethods = {"wx", "alipay", "card", "h5", "quickpay"};
    public double[] payMethodWeights = {0.40, 0.30, 0.10, 0.12, 0.08};

    public String[] psps = {"psp_a", "psp_b", "psp_c"};
    public double[] pspWeights = {0.50, 0.30, 0.20};

    public String[] channels = {"ch_organic", "ch_google", "ch_tiktok", "ch_apple", "ch_huawei", "ch_new_promo"};
    public double[] channelWeights = {0.35, 0.20, 0.15, 0.12, 0.10, 0.08};

    public String[] regions = {"CN-BJ", "CN-SH", "CN-GD", "CN-SC", "CN-ZJ", "SEA-VN", "SEA-TH"};

    // === PSP 差异化 ===
    // psp_c 延迟更大,失败率更高
    public Map<String, Double> pspFailRate = new LinkedHashMap<String, Double>() {{
        put("psp_a", 0.03); put("psp_b", 0.05); put("psp_c", 0.10);
    }};

    // === 渠道差异化: ch_new_promo 退款率更高 ===
    public Map<String, Double> channelRefundRateMultiplier = new LinkedHashMap<String, Double>() {{
        put("ch_organic", 1.0); put("ch_google", 1.2); put("ch_tiktok", 1.5);
        put("ch_apple", 0.8); put("ch_huawei", 0.9); put("ch_new_promo", 3.0);
    }};

    // === 退款/拒付/调账比例 ===
    public double baseRefundRate = 0.015;            // 基线退款率 1.5%
    public double fullRefundRatio = 0.85;            // 85% 全额退款
    public double partialRefundRatio = 0.12;         // 12% 部分退款
    public double multiRefundRatio = 0.03;           // 3% 多次部分退款
    public double chargebackRate = 0.001;            // 拒付率 0.1%
    public double adjustRate = 0.002;                // 调账率 0.2%

    // === 迟到/延迟分布(毫秒) ===
    public double delayTier1Ratio = 0.93;            // 93%: 0-30秒
    public long delayTier1MaxMs = 30_000;
    public double delayTier2Ratio = 0.05;            // 5%: 30秒-10分钟
    public long delayTier2MaxMs = 600_000;
    public double delayTier3Ratio = 0.015;           // 1.5%: 10分钟-6小时
    public long delayTier3MaxMs = 21_600_000;
    // 剩余 0.5%: 6-48小时
    public long delayTier4MaxMs = 172_800_000;

    // === 退款迟到(相对于支付成功的时间偏移) ===
    public long refundMinDelayMs = 60_000;           // 最快1分钟后退款
    public long refundMaxDelayMs = 86_400_000 * 3;   // 最迟3天后退款
    public long chargebackMinDelayMs = 3_600_000;    // 拒付最快1小时后
    public long chargebackMaxDelayMs = 86_400_000 * 7; // 拒付最迟7天

    // === 重复投递 ===
    public double payDuplicateRate = 0.005;          // 0.5% 支付重复
    public double refundDuplicateRate = 0.01;        // 1% 退款重复
    public double duplicateExactRatio = 0.7;         // 70% 完全相同payload
    // 30% 同主键但部分字段不同

    // === 数据缺失 ===
    public double missingInitiatedRate = 0.001;      // 0.1% pay_initiated丢失
    public double orphanRefundRate = 0.0001;         // 0.01% 退款找不到支付

    // === 重放(模拟分区重放) ===
    public double replayRate = 0.002;                // 0.2% 的数据属于重放段
    public long replayWindowMs = 600_000;            // 重放窗口10分钟

    // === 数据质量噪声 ===
    public double missingChannelRate = 0.003;        // 0.3% 缺channel_id
    public double missingRegionRate = 0.005;         // 0.5% 缺region
    public double unknownPayMethodRate = 0.001;      // 0.1% pay_method=unknown
    public double zeroAmountRate = 0.0005;           // 0.05% 金额为0
    public double negativeAmountRate = 0.0001;       // 0.01% 负金额
    public double hugeAmountRate = 0.0001;           // 0.01% 超大金额
    public long hugeAmountValue = 99_999_999;        // 超大金额值(999,999.99元)
    public double timezoneOffsetRate = 0.002;        // 0.2% 时区偏移(+8h误标UTC)

    // === 抽成规则 ===
    public int maxRuleChangesPerTenantPerDay = 3;    // 每租户每天最多3次规则变更
    public double ruleChangeDelayRatio = 0.3;        // 30% 规则变更事件迟到
    public long ruleChangeMaxDelayMs = 1_800_000;    // 规则最迟迟到30分钟
    public double baseRateMin = 0.01;                // 最低抽成1%
    public double baseRateMax = 0.05;                // 最高抽成5%
    public long fixedFeeMin = 0;                     // 固定费最低0
    public long fixedFeeMax = 500;                   // 固定费最高5元
    public long capAmountDefault = 500000;           // 封顶默认5000元

    // === Kafka ===
    public String bootstrapServers = "kafka1-84239:9092,kafka2-84239:9092,kafka3-84239:9092";
    public String topicPaySuccess = "tp_pay_success";
    public String topicRefundSuccess = "tp_refund_success";
    public String topicChargeback = "tp_chargeback";
    public String topicSettlementAdjust = "tp_settlement_adjust";
    public String topicTakeRateRuleChange = "tp_take_rate_rule_change";

    // === 真账本输出路径 ===
    public String groundTruthDir = "/tmp/datagen-ground-truth";
}

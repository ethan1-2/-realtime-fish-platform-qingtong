package com.saas.datagen.generator;

import com.saas.datagen.config.GenConfig;
import com.saas.datagen.model.*;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 核心数据生成器
 *
 * 生成流程(按规范):
 * 1. 初始化租户元数据(倾斜分布)
 * 2. 生成抽成规则及变更
 * 3. 生成订单 + 支付结果(真账本)
 * 4. 派生退款/拒付/调账
 * 5. 投影成事件流 + 加扰动(迟到/乱序/重复/缺失/重放/噪声)
 * 6. 输出真账本汇总
 */
public class DataGenerator {

    private final GenConfig config;
    private final Random random;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private final SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    // 生成的元数据
    private List<TenantMeta> tenants;
    // 租户 -> 规则时间线 (sorted by effectiveTime)
    private Map<String, TreeMap<Long, TakeRateRule>> ruleTimelines;
    // 真账本
    private List<GroundTruthRecord> groundTruth;
    // 最终事件流(排序后发送)
    private List<PaymentEvent> allEvents;
    // 规则变更事件
    private List<PaymentEvent> ruleChangeEvents;

    // ID 计数器
    private long orderSeq = 0;
    private long paymentSeq = 0;
    private long refundSeq = 0;
    private long chargebackSeq = 0;
    private long adjustSeq = 0;
    private int globalRuleVersion = 0;

    public DataGenerator(GenConfig config) {
        this.config = config;
        this.random = new Random(config.seed);
        this.tenants = new ArrayList<>();
        this.ruleTimelines = new HashMap<>();
        this.groundTruth = new ArrayList<>();
        this.allEvents = new ArrayList<>();
        this.ruleChangeEvents = new ArrayList<>();

        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        datetimeFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
    }

    // ==================== 1. 租户初始化 ====================

    public void initTenants() {
        // 生成倾斜的流量分布: Zipf-like
        double[] shares = generateZipfShares(config.tenantCount, 1.2);
        // 确保 top1 和 top5 满足约束
        normalizeShares(shares);

        String[] allPayMethods = config.payMethods;

        for (int i = 0; i < config.tenantCount; i++) {
            String tenantId = String.format("T%04d", i + 1);
            int appCount = 1 + random.nextInt(config.maxAppsPerTenant);
            String[] appIds = new String[appCount];
            for (int j = 0; j < appCount; j++) {
                appIds[j] = tenantId + "_APP" + (j + 1);
            }

            // 某些租户只开通部分支付方式
            int methodCount = 2 + random.nextInt(allPayMethods.length - 1);
            methodCount = Math.min(methodCount, allPayMethods.length);
            Set<String> methods = new LinkedHashSet<>();
            methods.add("wx");  // 所有人都有微信
            while (methods.size() < methodCount) {
                methods.add(allPayMethods[random.nextInt(allPayMethods.length)]);
            }

            double refundMultiplier = 0.5 + random.nextDouble() * 1.5;
            if (i >= config.tenantCount - 5) {
                refundMultiplier = 2.0 + random.nextDouble() * 3.0; // 尾部租户退款率高
            }

            TenantMeta meta = new TenantMeta(
                tenantId, appIds,
                methods.toArray(new String[0]),
                refundMultiplier,
                shares[i],
                1
            );
            tenants.add(meta);
        }
    }

    // ==================== 2. 规则生成 ====================

    public void generateRules(long dayStartMs) {
        for (TenantMeta tenant : tenants) {
            TreeMap<Long, TakeRateRule> timeline = new TreeMap<>();

            // 初始规则(当天 00:00 生效)
            TakeRateRule initialRule = createRule(tenant.tenantId, dayStartMs, 0);
            timeline.put(dayStartMs, initialRule);

            // 当天内的规则变更(0-N次)
            int changes = random.nextInt(config.maxRuleChangesPerTenantPerDay + 1);
            for (int c = 0; c < changes; c++) {
                long effectiveTime = dayStartMs + (long)(random.nextDouble() * 86_400_000L);
                TakeRateRule rule = createRule(tenant.tenantId, effectiveTime, c + 1);
                timeline.put(effectiveTime, rule);

                // 生成规则变更事件
                PaymentEvent ruleEvent = new PaymentEvent();
                ruleEvent.eventType = "take_rate_rule_change";
                ruleEvent.tenantId = tenant.tenantId;
                ruleEvent.ruleId = rule.ruleId;
                ruleEvent.ruleVersion = rule.ruleVersion;
                ruleEvent.effectiveTime = effectiveTime;
                ruleEvent.eventTime = rule.publishTimeMs;

                // 规则变更迟到: 30% 的概率迟到
                if (random.nextDouble() < config.ruleChangeDelayRatio) {
                    long delay = (long)(random.nextDouble() * config.ruleChangeMaxDelayMs);
                    ruleEvent.ingestTime = effectiveTime + delay;
                } else {
                    ruleEvent.ingestTime = rule.publishTimeMs + randomDelay(0);
                }

                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("rate_type", rule.rateType);
                payload.put("rate_pct", rule.ratePct);
                payload.put("fixed_fee", rule.fixedFee);
                payload.put("cap_amount", rule.capAmount);
                payload.put("floor_amount", rule.floorAmount);
                payload.put("pay_method", rule.payMethod);
                payload.put("channel_id", rule.channelId);
                ruleEvent.rulePayload = payload;

                ruleChangeEvents.add(ruleEvent);
            }

            ruleTimelines.put(tenant.tenantId, timeline);
        }
    }

    private TakeRateRule createRule(String tenantId, long effectiveTime, int changeIndex) {
        TakeRateRule rule = new TakeRateRule();
        rule.tenantId = tenantId;
        rule.ruleVersion = ++globalRuleVersion;
        rule.ruleId = "RULE_" + tenantId + "_V" + rule.ruleVersion;
        rule.effectiveTimeMs = effectiveTime;

        // 两种规则形态
        if (random.nextDouble() < 0.7) {
            // 70% 比例抽成
            rule.rateType = "percent";
            rule.ratePct = config.baseRateMin + random.nextDouble() * (config.baseRateMax - config.baseRateMin);
            rule.ratePct = Math.round(rule.ratePct * 10000.0) / 10000.0; // 保留4位小数
            rule.fixedFee = 0;
        } else {
            // 30% 比例 + 固定费
            rule.rateType = "percent";
            rule.ratePct = config.baseRateMin + random.nextDouble() * (config.baseRateMax - config.baseRateMin) * 0.5;
            rule.ratePct = Math.round(rule.ratePct * 10000.0) / 10000.0;
            rule.fixedFee = config.fixedFeeMin + (long)(random.nextDouble() * (config.fixedFeeMax - config.fixedFeeMin));
        }

        rule.capAmount = config.capAmountDefault;
        rule.floorAmount = random.nextDouble() < 0.3 ? 100 : 0; // 30% 有封底1元
        rule.payMethod = "*";
        rule.channelId = "*";

        // 发布时间 = 生效时间 - 随机提前量(大部分提前发布)
        long leadTime = (long)(random.nextDouble() * 3600_000); // 0-1小时提前
        rule.publishTimeMs = effectiveTime - leadTime;
        if (rule.publishTimeMs < effectiveTime - 86_400_000L) {
            rule.publishTimeMs = effectiveTime;
        }

        return rule;
    }

    // ==================== 3. 订单 + 支付生成 ====================

    public void generateOrders(long dayStartMs) {
        long dayEndMs = dayStartMs + 86_400_000L;

        for (TenantMeta tenant : tenants) {
            int orderCount = (int)(config.totalOrdersPerDay * tenant.trafficShare);
            if (orderCount < 1) orderCount = 1;

            // app 内也有倾斜
            double[] appShares = generateZipfShares(tenant.appIds.length, 1.5);

            for (int i = 0; i < orderCount; i++) {
                // 选 app
                String appId = tenant.appIds[weightedChoice(appShares)];
                // 选维度
                String payMethod = tenant.allowedPayMethods[random.nextInt(tenant.allowedPayMethods.length)];
                String psp = weightedChoiceStr(config.psps, config.pspWeights);
                String channel = weightedChoiceStr(config.channels, config.channelWeights);
                String region = config.regions[random.nextInt(config.regions.length)];
                String userId = "U_" + tenant.tenantId + "_" + (random.nextInt(10000) + 1);

                // 生成事件时间(均匀分布在一天内)
                long eventTime = dayStartMs + (long)(random.nextDouble() * 86_400_000L);

                // 生成金额
                long amount = generateAmount();

                // PSP 失败率
                double failRate = config.pspFailRate.getOrDefault(psp, 0.05);
                if (random.nextDouble() < failRate) {
                    // 支付失败,不生成 pay_success (可选生成 pay_failed)
                    continue;
                }

                // 生成订单
                String orderId = "ORD" + String.format("%012d", ++orderSeq);
                String paymentId = "PAY" + String.format("%012d", ++paymentSeq);
                String traceId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);

                // 查找当前生效的规则
                TakeRateRule rule = findEffectiveRule(tenant.tenantId, eventTime);
                long platformFee = rule.calculateFee(amount);
                long settlementAmt = amount - platformFee;

                // === 生成 pay_success 事件 ===
                PaymentEvent payEvent = new PaymentEvent();
                payEvent.eventType = "pay_success";
                payEvent.tenantId = tenant.tenantId;
                payEvent.appId = appId;
                payEvent.orderId = orderId;
                payEvent.paymentId = paymentId;
                payEvent.userId = userId;
                payEvent.idempotencyKey = "PAY_" + paymentId;
                payEvent.amountMinor = amount;
                payEvent.channelId = channel;
                payEvent.payMethod = payMethod;
                payEvent.psp = psp;
                payEvent.region = region;
                payEvent.takeRateRuleVersion = rule.ruleVersion;
                payEvent.takeRatePct = rule.ratePct;
                payEvent.takeRateFixedFee = rule.fixedFee;
                payEvent.takeRateAmount = platformFee;
                payEvent.settlementAmount = settlementAmt;
                payEvent.eventTime = eventTime;
                payEvent.ingestTime = eventTime + randomDelay(0);
                payEvent.traceId = traceId;

                // 数据噪声
                applyNoise(payEvent);

                allEvents.add(payEvent);

                // 真账本
                GroundTruthRecord gt = new GroundTruthRecord();
                gt.tenantId = tenant.tenantId;
                gt.appId = appId;
                gt.orderId = orderId;
                gt.paymentId = paymentId;
                gt.eventType = "pay_success";
                gt.amountMinor = amount;
                gt.ruleVersion = rule.ruleVersion;
                gt.ratePct = rule.ratePct;
                gt.platformFee = platformFee;
                gt.settlementAmount = settlementAmt;
                gt.eventTimeMs = eventTime;
                gt.date = dateFormat.format(new Date(eventTime));
                groundTruth.add(gt);

                // === 派生退款 ===
                double refundRate = config.baseRefundRate * tenant.refundRateMultiplier;
                Double channelMult = config.channelRefundRateMultiplier.get(channel);
                if (channelMult != null) refundRate *= channelMult;
                refundRate = Math.min(refundRate, 0.15); // 上限15%

                if (random.nextDouble() < refundRate) {
                    generateRefund(tenant, appId, orderId, paymentId, userId, amount,
                                   rule, eventTime, channel, payMethod, psp, region, traceId);
                }

                // === 派生拒付(极低概率,且更晚) ===
                if (random.nextDouble() < config.chargebackRate) {
                    generateChargeback(tenant, appId, orderId, paymentId, userId, amount,
                                      rule, eventTime, channel, payMethod, psp, region, traceId);
                }
            }
        }

        // === 生成少量调账 ===
        generateAdjustments(dayStartMs);
    }

    private void generateRefund(TenantMeta tenant, String appId, String orderId,
                                String paymentId, String userId, long payAmount,
                                TakeRateRule rule, long payEventTime,
                                String channel, String payMethod, String psp,
                                String region, String traceId) {
        // 退款时间
        long refundDelay = config.refundMinDelayMs +
            (long)(random.nextDouble() * (config.refundMaxDelayMs - config.refundMinDelayMs));
        long refundTime = payEventTime + refundDelay;

        // 退款金额
        long refundAmount;
        double r = random.nextDouble();
        int refundCount = 1;
        if (r < config.fullRefundRatio) {
            refundAmount = payAmount;
        } else if (r < config.fullRefundRatio + config.partialRefundRatio) {
            refundAmount = (long)(payAmount * (0.3 + random.nextDouble() * 0.6));
        } else {
            // 多次部分退款
            refundCount = 2 + random.nextInt(2);
            refundAmount = payAmount / refundCount;
        }

        for (int k = 0; k < refundCount; k++) {
            String refundId = "REF" + String.format("%012d", ++refundSeq);
            long thisRefundAmount = (k < refundCount - 1) ? refundAmount :
                (payAmount - refundAmount * (refundCount - 1)); // 最后一笔取余

            if (refundCount > 1 && k > 0) {
                refundTime += (long)(random.nextDouble() * 3600_000); // 多次退款间隔
            }

            // 退款抽成退还
            long refundFee = rule.calculateFee(thisRefundAmount);

            PaymentEvent refundEvent = new PaymentEvent();
            refundEvent.eventType = "refund_success";
            refundEvent.tenantId = tenant.tenantId;
            refundEvent.appId = appId;
            refundEvent.orderId = orderId;
            refundEvent.paymentId = paymentId;
            refundEvent.refundId = refundId;
            refundEvent.userId = userId;
            refundEvent.idempotencyKey = "REF_" + refundId;
            refundEvent.amountMinor = -thisRefundAmount; // 退款为负
            refundEvent.channelId = channel;
            refundEvent.payMethod = payMethod;
            refundEvent.psp = psp;
            refundEvent.region = region;
            refundEvent.takeRateRuleVersion = rule.ruleVersion;
            refundEvent.takeRatePct = rule.ratePct;
            refundEvent.takeRateAmount = -refundFee;
            refundEvent.settlementAmount = -(thisRefundAmount - refundFee);
            refundEvent.eventTime = refundTime;
            refundEvent.ingestTime = refundTime + randomDelay(1); // 退款可能迟到更多
            refundEvent.traceId = traceId;
            refundEvent.refundReason = randomRefundReason();

            applyNoise(refundEvent);
            allEvents.add(refundEvent);

            // 真账本
            GroundTruthRecord gt = new GroundTruthRecord();
            gt.tenantId = tenant.tenantId;
            gt.appId = appId;
            gt.orderId = orderId;
            gt.paymentId = paymentId;
            gt.eventType = "refund_success";
            gt.amountMinor = thisRefundAmount;
            gt.ruleVersion = rule.ruleVersion;
            gt.ratePct = rule.ratePct;
            gt.platformFee = refundFee;
            gt.settlementAmount = thisRefundAmount - refundFee;
            gt.eventTimeMs = refundTime;
            gt.date = dateFormat.format(new Date(refundTime));
            groundTruth.add(gt);
        }
    }

    private void generateChargeback(TenantMeta tenant, String appId, String orderId,
                                    String paymentId, String userId, long payAmount,
                                    TakeRateRule rule, long payEventTime,
                                    String channel, String payMethod, String psp,
                                    String region, String traceId) {
        long cbDelay = config.chargebackMinDelayMs +
            (long)(random.nextDouble() * (config.chargebackMaxDelayMs - config.chargebackMinDelayMs));
        long cbTime = payEventTime + cbDelay;

        String cbId = "CB" + String.format("%012d", ++chargebackSeq);
        long cbFee = rule.calculateFee(payAmount);

        PaymentEvent cbEvent = new PaymentEvent();
        cbEvent.eventType = "chargeback";
        cbEvent.tenantId = tenant.tenantId;
        cbEvent.appId = appId;
        cbEvent.orderId = orderId;
        cbEvent.paymentId = paymentId;
        cbEvent.chargebackId = cbId;
        cbEvent.userId = userId;
        cbEvent.idempotencyKey = "CB_" + cbId;
        cbEvent.amountMinor = -payAmount;
        cbEvent.channelId = channel;
        cbEvent.payMethod = payMethod;
        cbEvent.psp = psp;
        cbEvent.region = region;
        cbEvent.takeRateRuleVersion = rule.ruleVersion;
        cbEvent.takeRatePct = rule.ratePct;
        cbEvent.takeRateAmount = -cbFee;
        cbEvent.settlementAmount = -(payAmount - cbFee);
        cbEvent.eventTime = cbTime;
        cbEvent.ingestTime = cbTime + randomDelay(2); // 拒付迟到更严重
        cbEvent.traceId = traceId;

        allEvents.add(cbEvent);

        GroundTruthRecord gt = new GroundTruthRecord();
        gt.tenantId = tenant.tenantId;
        gt.appId = appId;
        gt.orderId = orderId;
        gt.paymentId = paymentId;
        gt.eventType = "chargeback";
        gt.amountMinor = payAmount;
        gt.ruleVersion = rule.ruleVersion;
        gt.ratePct = rule.ratePct;
        gt.platformFee = cbFee;
        gt.settlementAmount = payAmount - cbFee;
        gt.eventTimeMs = cbTime;
        gt.date = dateFormat.format(new Date(cbTime));
        groundTruth.add(gt);
    }

    private void generateAdjustments(long dayStartMs) {
        int adjustCount = (int)(config.totalOrdersPerDay * config.adjustRate);
        for (int i = 0; i < adjustCount; i++) {
            TenantMeta tenant = tenants.get(random.nextInt(tenants.size()));
            String appId = tenant.appIds[random.nextInt(tenant.appIds.length)];
            long eventTime = dayStartMs + (long)(random.nextDouble() * 86_400_000L);
            String adjId = "ADJ" + String.format("%012d", ++adjustSeq);

            // 调账金额可正可负
            long amount = (random.nextBoolean() ? 1 : -1) *
                (100 + (long)(random.nextDouble() * 50000));

            PaymentEvent adjEvent = new PaymentEvent();
            adjEvent.eventType = "settlement_adjust";
            adjEvent.tenantId = tenant.tenantId;
            adjEvent.appId = appId;
            adjEvent.adjustId = adjId;
            adjEvent.idempotencyKey = "ADJ_" + adjId;
            adjEvent.amountMinor = amount;
            adjEvent.eventTime = eventTime;
            adjEvent.ingestTime = eventTime + randomDelay(0);
            adjEvent.adjustReason = randomAdjustReason();
            adjEvent.refType = random.nextDouble() < 0.5 ? "none" : "payment";
            adjEvent.userId = "U_" + tenant.tenantId + "_" + (random.nextInt(10000) + 1);

            allEvents.add(adjEvent);

            GroundTruthRecord gt = new GroundTruthRecord();
            gt.tenantId = tenant.tenantId;
            gt.appId = appId;
            gt.eventType = "settlement_adjust";
            gt.amountMinor = amount;
            gt.ruleVersion = 0;
            gt.ratePct = 0;
            gt.platformFee = 0;
            gt.settlementAmount = amount; // 调账直接给租户
            gt.eventTimeMs = eventTime;
            gt.date = dateFormat.format(new Date(eventTime));
            groundTruth.add(gt);
        }
    }

    // ==================== 4. 扰动: 重复/重放 ====================

    public void applyDisturbances() {
        List<PaymentEvent> duplicates = new ArrayList<>();

        for (PaymentEvent event : allEvents) {
            // 重复投递
            double dupRate = "refund_success".equals(event.eventType) ?
                config.refundDuplicateRate : config.payDuplicateRate;

            if (random.nextDouble() < dupRate) {
                if (random.nextDouble() < config.duplicateExactRatio) {
                    // 完全相同 payload
                    duplicates.add(event);
                } else {
                    // 同主键但部分字段变化
                    PaymentEvent dup = cloneEvent(event);
                    dup.traceId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
                    dup.ingestTime = event.ingestTime + (long)(random.nextDouble() * 5000);
                    duplicates.add(dup);
                }
            }
        }
        allEvents.addAll(duplicates);

        // 重放段: 模拟某分区从 N 分钟前重放
        if (config.replayRate > 0 && !allEvents.isEmpty()) {
            int replayCount = (int)(allEvents.size() * config.replayRate);
            List<PaymentEvent> replayBatch = new ArrayList<>();
            for (int i = 0; i < replayCount && i < allEvents.size(); i++) {
                int idx = random.nextInt(allEvents.size());
                PaymentEvent original = allEvents.get(idx);
                PaymentEvent replay = cloneEvent(original);
                replay.ingestTime = original.ingestTime + config.replayWindowMs;
                replayBatch.add(replay);
            }
            allEvents.addAll(replayBatch);
        }
    }

    // ==================== 5. 排序(按 ingestTime 模拟 Kafka 到达顺序) ====================

    public void sortByIngestTime() {
        allEvents.sort(Comparator.comparingLong(e -> e.ingestTime != null ? e.ingestTime : 0L));
        ruleChangeEvents.sort(Comparator.comparingLong(e -> e.ingestTime != null ? e.ingestTime : 0L));
    }

    // ==================== 工具方法 ====================

    private TakeRateRule findEffectiveRule(String tenantId, long eventTime) {
        TreeMap<Long, TakeRateRule> timeline = ruleTimelines.get(tenantId);
        if (timeline == null || timeline.isEmpty()) {
            // 返回默认规则
            TakeRateRule def = new TakeRateRule();
            def.tenantId = tenantId;
            def.rateType = "percent";
            def.ratePct = 0.03;
            def.fixedFee = 0;
            def.capAmount = 500000;
            def.floorAmount = 0;
            def.ruleVersion = 0;
            return def;
        }
        Map.Entry<Long, TakeRateRule> entry = timeline.floorEntry(eventTime);
        if (entry == null) {
            return timeline.firstEntry().getValue();
        }
        return entry.getValue();
    }

    private long generateAmount() {
        // 数据质量噪声
        double r = random.nextDouble();
        if (r < config.zeroAmountRate) return 0;
        if (r < config.zeroAmountRate + config.negativeAmountRate) return -(100 + random.nextInt(10000));
        if (r < config.zeroAmountRate + config.negativeAmountRate + config.hugeAmountRate) return config.hugeAmountValue;

        if (random.nextDouble() < config.commonAmountRatio) {
            return config.commonAmounts[random.nextInt(config.commonAmounts.length)];
        } else {
            return config.customAmountMin + (long)(random.nextDouble() * (config.customAmountMax - config.customAmountMin));
        }
    }

    /**
     * 延迟分布
     * @param severityLevel 0=normal, 1=退款(稍重), 2=拒付(更重)
     */
    private long randomDelay(int severityLevel) {
        double r = random.nextDouble();
        double shift = severityLevel * 0.03; // 退款/拒付更容易迟到

        if (r < config.delayTier1Ratio - shift) {
            return (long)(random.nextDouble() * config.delayTier1MaxMs);
        } else if (r < config.delayTier1Ratio + config.delayTier2Ratio - shift * 0.5) {
            return config.delayTier1MaxMs + (long)(random.nextDouble() * (config.delayTier2MaxMs - config.delayTier1MaxMs));
        } else if (r < config.delayTier1Ratio + config.delayTier2Ratio + config.delayTier3Ratio) {
            return config.delayTier2MaxMs + (long)(random.nextDouble() * (config.delayTier3MaxMs - config.delayTier2MaxMs));
        } else {
            return config.delayTier3MaxMs + (long)(random.nextDouble() * (config.delayTier4MaxMs - config.delayTier3MaxMs));
        }
    }

    private void applyNoise(PaymentEvent event) {
        if (random.nextDouble() < config.missingChannelRate) {
            event.channelId = null;
        }
        if (random.nextDouble() < config.missingRegionRate) {
            event.region = null;
        }
        if (random.nextDouble() < config.unknownPayMethodRate) {
            event.payMethod = "unknown";
        }
        if (random.nextDouble() < config.timezoneOffsetRate) {
            // 误标 UTC: event_time 偏移 +8h
            event.eventTime = event.eventTime + 8 * 3600_000L;
            event.dataQualityFlag = "suspect";
        }
    }

    private double[] generateZipfShares(int n, double exponent) {
        double[] weights = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            weights[i] = 1.0 / Math.pow(i + 1, exponent);
            sum += weights[i];
        }
        for (int i = 0; i < n; i++) weights[i] /= sum;
        return weights;
    }

    private void normalizeShares(double[] shares) {
        // 确保 top1 >= config.top1TenantShare
        if (shares.length > 0 && shares[0] < config.top1TenantShare) {
            double deficit = config.top1TenantShare - shares[0];
            shares[0] = config.top1TenantShare;
            // 从其他分摊
            for (int i = 1; i < shares.length; i++) {
                double reduction = deficit * shares[i] / (1.0 - shares[0] + deficit);
                shares[i] -= reduction;
                if (shares[i] < 0.0001) shares[i] = 0.0001;
            }
        }
    }

    private int weightedChoice(double[] weights) {
        double r = random.nextDouble();
        double cum = 0;
        for (int i = 0; i < weights.length; i++) {
            cum += weights[i];
            if (r < cum) return i;
        }
        return weights.length - 1;
    }

    private String weightedChoiceStr(String[] items, double[] weights) {
        return items[weightedChoice(weights)];
    }

    private String randomRefundReason() {
        String[] reasons = {"user_request", "duplicate_charge", "product_issue", "fraud_suspect", "service_not_delivered"};
        return reasons[random.nextInt(reasons.length)];
    }

    private String randomAdjustReason() {
        String[] reasons = {"fee_correction", "promotion_credit", "dispute_resolution", "system_error_compensation", "manual_adjustment"};
        return reasons[random.nextInt(reasons.length)];
    }

    private PaymentEvent cloneEvent(PaymentEvent src) {
        PaymentEvent dup = new PaymentEvent();
        dup.eventType = src.eventType;
        dup.tenantId = src.tenantId;
        dup.appId = src.appId;
        dup.orderId = src.orderId;
        dup.paymentId = src.paymentId;
        dup.refundId = src.refundId;
        dup.chargebackId = src.chargebackId;
        dup.adjustId = src.adjustId;
        dup.userId = src.userId;
        dup.idempotencyKey = src.idempotencyKey;
        dup.amountMinor = src.amountMinor;
        dup.currency = src.currency;
        dup.channelId = src.channelId;
        dup.payMethod = src.payMethod;
        dup.psp = src.psp;
        dup.region = src.region;
        dup.takeRateRuleVersion = src.takeRateRuleVersion;
        dup.takeRatePct = src.takeRatePct;
        dup.takeRateFixedFee = src.takeRateFixedFee;
        dup.takeRateAmount = src.takeRateAmount;
        dup.settlementAmount = src.settlementAmount;
        dup.refType = src.refType;
        dup.refId = src.refId;
        dup.adjustReason = src.adjustReason;
        dup.refundReason = src.refundReason;
        dup.ruleId = src.ruleId;
        dup.ruleVersion = src.ruleVersion;
        dup.effectiveTime = src.effectiveTime;
        dup.rulePayload = src.rulePayload;
        dup.eventTime = src.eventTime;
        dup.ingestTime = src.ingestTime;
        dup.traceId = src.traceId;
        dup.dataQualityFlag = src.dataQualityFlag;
        return dup;
    }

    // ==================== Getters ====================

    public List<TenantMeta> getTenants() { return tenants; }
    public Map<String, TreeMap<Long, TakeRateRule>> getRuleTimelines() { return ruleTimelines; }
    public List<GroundTruthRecord> getGroundTruth() { return groundTruth; }
    public List<PaymentEvent> getAllEvents() { return allEvents; }
    public List<PaymentEvent> getRuleChangeEvents() { return ruleChangeEvents; }
}

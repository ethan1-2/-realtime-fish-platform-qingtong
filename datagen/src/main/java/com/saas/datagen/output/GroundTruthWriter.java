package com.saas.datagen.output;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.saas.datagen.model.GroundTruthRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * 真账本输出 - 按 tenant_id + date 汇总,写入文件用于事后对账验收
 */
public class GroundTruthWriter {
    private static final Logger LOG = LoggerFactory.getLogger(GroundTruthWriter.class);
    private final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    /**
     * 写出真账本: 明细 + 汇总
     */
    public void write(List<GroundTruthRecord> records, String outputDir) throws IOException {
        File dir = new File(outputDir);
        if (!dir.exists()) dir.mkdirs();

        // 1. 写明细
        File detailFile = new File(dir, "ground_truth_detail.json");
        mapper.writeValue(detailFile, records);
        LOG.info("Ground truth detail written: {} records -> {}", records.size(), detailFile.getPath());

        // 2. 按 tenant_id + date 汇总
        Map<String, TenantDaySummary> summaries = new LinkedHashMap<>();
        for (GroundTruthRecord r : records) {
            String key = r.tenantId + "|" + r.date;
            TenantDaySummary s = summaries.computeIfAbsent(key, k -> {
                TenantDaySummary ns = new TenantDaySummary();
                ns.tenantId = r.tenantId;
                ns.date = r.date;
                return ns;
            });

            switch (r.eventType) {
                case "pay_success":
                    s.payCount++;
                    s.gmv += r.amountMinor;
                    s.platformFee += r.platformFee;
                    break;
                case "refund_success":
                    s.refundCount++;
                    s.refundAmount += r.amountMinor; // 正数
                    s.platformFeeRefund += r.platformFee;
                    break;
                case "chargeback":
                    s.chargebackCount++;
                    s.chargebackAmount += r.amountMinor;
                    s.platformFeeChargeback += r.platformFee;
                    break;
                case "settlement_adjust":
                    s.adjustCount++;
                    s.adjustAmount += r.amountMinor; // 可正可负
                    break;
            }
        }

        // 计算衍生指标
        for (TenantDaySummary s : summaries.values()) {
            s.netIn = s.gmv - s.refundAmount - s.chargebackAmount + s.adjustAmount;
            s.totalPlatformFee = s.platformFee - s.platformFeeRefund - s.platformFeeChargeback;
            s.tenantSettle = s.netIn - s.totalPlatformFee;
        }

        File summaryFile = new File(dir, "ground_truth_summary.json");
        mapper.writeValue(summaryFile, new ArrayList<>(summaries.values()));
        LOG.info("Ground truth summary written: {} tenant-days -> {}", summaries.size(), summaryFile.getPath());

        // 3. 打印汇总
        LOG.info("=== Ground Truth Summary ===");
        long totalGmv = 0, totalRefund = 0, totalCb = 0, totalAdj = 0, totalFee = 0;
        for (TenantDaySummary s : summaries.values()) {
            totalGmv += s.gmv;
            totalRefund += s.refundAmount;
            totalCb += s.chargebackAmount;
            totalAdj += s.adjustAmount;
            totalFee += s.totalPlatformFee;
        }
        LOG.info("Total GMV: {} ({}元)", totalGmv, totalGmv / 100.0);
        LOG.info("Total Refund: {} ({}元)", totalRefund, totalRefund / 100.0);
        LOG.info("Total Chargeback: {} ({}元)", totalCb, totalCb / 100.0);
        LOG.info("Total Adjust: {} ({}元)", totalAdj, totalAdj / 100.0);
        LOG.info("Total Platform Fee: {} ({}元)", totalFee, totalFee / 100.0);
        LOG.info("Total Net In: {} ({}元)", totalGmv - totalRefund - totalCb + totalAdj,
            (totalGmv - totalRefund - totalCb + totalAdj) / 100.0);
    }

    public static class TenantDaySummary {
        public String tenantId;
        public String date;
        public long payCount;
        public long gmv;                   // sum(pay_success.amount)
        public long refundCount;
        public long refundAmount;          // sum(refund.amount) 正数
        public long chargebackCount;
        public long chargebackAmount;      // sum(chargeback.amount) 正数
        public long adjustCount;
        public long adjustAmount;          // sum(adjust.amount) 可正可负
        public long netIn;                 // gmv - refund - chargeback + adjust
        public long platformFee;           // 支付抽成
        public long platformFeeRefund;     // 退款退还抽成
        public long platformFeeChargeback; // 拒付退还抽成
        public long totalPlatformFee;      // 净平台抽成
        public long tenantSettle;          // 租户应结算 = netIn - totalPlatformFee
    }
}

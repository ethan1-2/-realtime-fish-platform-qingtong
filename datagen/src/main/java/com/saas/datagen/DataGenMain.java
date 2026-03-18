package com.saas.datagen;

import com.saas.datagen.config.GenConfig;
import com.saas.datagen.generator.DataGenerator;
import com.saas.datagen.output.GroundTruthWriter;
import com.saas.datagen.output.KafkaSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 数据生成器主入口
 *
 * 两种模式：
 *
 * 1. 批量模式（默认）：一次性生成 N 天的数据，发完退出
 *    java -jar payment-datagen.jar --orders=10000 --tenants=20
 *
 * 2. 流式模式（--streaming）：持续产生数据，模拟无界流，Ctrl+C 停止
 *    java -jar payment-datagen.jar --streaming --tps=1000 --tenants=20
 *
 * 全部选项：
 *   --orders=N         批量模式：每天总订单数 (默认 500000)
 *   --tenants=N        租户数 (默认 100)
 *   --days=N           批量模式：生成天数 (默认 1)
 *   --tps=N            每个 topic 的目标 TPS (默认 10000)
 *   --seed=N           随机种子 (默认 42)
 *   --bootstrap=hosts  Kafka bootstrap servers
 *   --producers=N      Kafka producer 线程数 (默认 4)
 *   --streaming        流式模式：持续产生数据不停止
 *   --batch-interval=N 流式模式：每批生成的时间间隔(秒, 默认 10)
 *   --batch-size=N     流式模式：每批生成的订单数(默认 500)
 *   --dry-run          不发送 Kafka，只生成真账本
 */
public class DataGenMain {
    private static final Logger LOG = LoggerFactory.getLogger(DataGenMain.class);

    public static void main(String[] args) throws Exception {
        GenConfig config = new GenConfig();
        boolean dryRun = false;
        boolean streaming = false;
        int targetTps = 10000;
        int numProducers = 4;
        int batchIntervalSec = 10;
        int streamingBatchSize = 500;

        for (String arg : args) {
            if (arg.startsWith("--orders=")) config.totalOrdersPerDay = Integer.parseInt(arg.split("=")[1]);
            else if (arg.startsWith("--tenants=")) config.tenantCount = Integer.parseInt(arg.split("=")[1]);
            else if (arg.startsWith("--days=")) config.daysToGenerate = Integer.parseInt(arg.split("=")[1]);
            else if (arg.startsWith("--tps=")) targetTps = Integer.parseInt(arg.split("=")[1]);
            else if (arg.startsWith("--seed=")) config.seed = Long.parseLong(arg.split("=")[1]);
            else if (arg.startsWith("--bootstrap=")) config.bootstrapServers = arg.split("=", 2)[1];
            else if (arg.startsWith("--producers=")) numProducers = Integer.parseInt(arg.split("=")[1]);
            else if (arg.equals("--streaming")) streaming = true;
            else if (arg.startsWith("--batch-interval=")) batchIntervalSec = Integer.parseInt(arg.split("=")[1]);
            else if (arg.startsWith("--batch-size=")) streamingBatchSize = Integer.parseInt(arg.split("=")[1]);
            else if (arg.equals("--dry-run")) dryRun = true;
        }

        LOG.info("==============================================");
        LOG.info("SAAS Payment Data Generator");
        LOG.info("==============================================");
        LOG.info("Mode: {}", streaming ? "STREAMING (Ctrl+C to stop)" : "BATCH");
        LOG.info("Tenants: {}", config.tenantCount);
        LOG.info("Kafka: {}", config.bootstrapServers);
        LOG.info("TPS per topic: {}", targetTps);
        LOG.info("Producer threads: {}", numProducers);
        if (streaming) {
            LOG.info("Batch interval: {}s", batchIntervalSec);
            LOG.info("Batch size: {} orders/batch", streamingBatchSize);
        } else {
            LOG.info("Orders/day: {}", config.totalOrdersPerDay);
            LOG.info("Days: {}", config.daysToGenerate);
            LOG.info("Seed: {}", config.seed);
        }
        LOG.info("Dry run: {}", dryRun);
        LOG.info("==============================================");

        if (streaming) {
            runStreaming(config, targetTps, numProducers, batchIntervalSec, streamingBatchSize, dryRun);
        } else {
            runBatch(config, targetTps, numProducers, dryRun);
        }
    }

    /**
     * 批量模式：一次性生成 → 加扰动 → 发送 → 退出（原有逻辑）
     */
    private static void runBatch(GenConfig config, int targetTps, int numProducers, boolean dryRun) throws Exception {
        DataGenerator generator = new DataGenerator(config);

        LOG.info("Step 1: Initializing tenants...");
        generator.initTenants();
        LOG.info("Created {} tenants, top1 share: {}%",
            generator.getTenants().size(),
            String.format("%.1f", generator.getTenants().get(0).trafficShare * 100));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));

        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        for (int day = 0; day < config.daysToGenerate; day++) {
            long dayStartMs = cal.getTimeInMillis();
            String dateStr = sdf.format(cal.getTime());
            LOG.info("=== Generating day {}: {} ===", day + 1, dateStr);

            LOG.info("Step 2: Generating take rate rules...");
            generator.generateRules(dayStartMs);
            LOG.info("Rule change events: {}", generator.getRuleChangeEvents().size());

            LOG.info("Step 3: Generating orders and derived events...");
            generator.generateOrders(dayStartMs);
            LOG.info("Total events: {}, Ground truth records: {}",
                generator.getAllEvents().size(), generator.getGroundTruth().size());

            cal.add(Calendar.DAY_OF_MONTH, 1);
        }

        LOG.info("Step 4: Applying disturbances (duplicates, replays)...");
        generator.applyDisturbances();
        LOG.info("After disturbances: {} events", generator.getAllEvents().size());

        LOG.info("Step 5: Sorting by ingest time...");
        generator.sortByIngestTime();

        LOG.info("Step 6: Writing ground truth...");
        GroundTruthWriter gtWriter = new GroundTruthWriter();
        gtWriter.write(generator.getGroundTruth(), config.groundTruthDir);

        if (!dryRun) {
            LOG.info("Step 7: Sending to Kafka...");
            KafkaSender sender = new KafkaSender(config, numProducers);
            try {
                LOG.info("Sending {} rule change events...", generator.getRuleChangeEvents().size());
                sender.send(generator.getRuleChangeEvents(), targetTps);
                Thread.sleep(2000);

                LOG.info("Sending {} payment events...", generator.getAllEvents().size());
                sender.send(generator.getAllEvents(), targetTps);
            } finally {
                sender.close();
            }
            LOG.info("Kafka send complete! Sent: {}, Errors: {}", sender.getSentCount(), sender.getErrorCount());
        } else {
            LOG.info("Dry run mode - skipping Kafka send");
        }

        LOG.info("==============================================");
        LOG.info("Data generation complete!");
        LOG.info("Ground truth at: {}", config.groundTruthDir);
        LOG.info("==============================================");
    }

    /**
     * 流式模式：持续循环生成数据，模拟无界流
     *
     * 每轮循环：
     *   1. 生成一小批订单（batch-size 条），event_time = 当前真实时间
     *   2. 加扰动（重复、迟到）
     *   3. 发到 Kafka
     *   4. 等 batch-interval 秒
     *   5. 回到 1
     *
     * Ctrl+C 停止（JVM shutdown hook 做清理）
     */
    private static void runStreaming(GenConfig config, int targetTps, int numProducers,
                                     int batchIntervalSec, int batchSize, boolean dryRun) throws Exception {
        // streaming 模式用当前时间做 seed，每次结果不同
        config.seed = System.currentTimeMillis();

        DataGenerator generator = new DataGenerator(config);

        LOG.info("Step 1: Initializing tenants...");
        generator.initTenants();
        LOG.info("Created {} tenants", generator.getTenants().size());

        // 先生成一批规则（用今天 0 点做基准）
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        long todayStartMs = cal.getTimeInMillis();

        LOG.info("Step 2: Generating initial take rate rules...");
        generator.generateRules(todayStartMs);

        KafkaSender sender = null;
        if (!dryRun) {
            sender = new KafkaSender(config, numProducers);

            // 先发规则变更
            if (!generator.getRuleChangeEvents().isEmpty()) {
                LOG.info("Sending {} initial rule change events...", generator.getRuleChangeEvents().size());
                sender.send(generator.getRuleChangeEvents(), targetTps);
                Thread.sleep(1000);
            }
        }

        // Shutdown hook：Ctrl+C 时优雅关闭
        final KafkaSender finalSender = sender;
        final boolean[] running = {true};
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received, stopping...");
            running[0] = false;
            if (finalSender != null) {
                finalSender.close();
            }
        }));

        // 覆盖 totalOrdersPerDay 为每批的 batchSize
        config.totalOrdersPerDay = batchSize;

        long totalSent = 0;
        long batchNum = 0;
        long seqOffset = 0; // 全局 ID 偏移量，确保跨批次 orderId/paymentId 唯一

        LOG.info("==============================================");
        LOG.info("Streaming started! Generating {} orders every {}s", batchSize, batchIntervalSec);
        LOG.info("Press Ctrl+C to stop");
        LOG.info("==============================================");

        while (running[0]) {
            batchNum++;
            long batchStartMs = System.currentTimeMillis();

            // 每一轮用一个新的 generator（清空上轮的事件列表）
            DataGenerator batchGen = new DataGenerator(config);
            batchGen.setSeqOffset(seqOffset); // 设置 ID 起始偏移，避免 idempotency_key 重复
            batchGen.initTenants();

            // 用今天的 0 点做基准生成规则时间线（复用 generator 的规则）
            batchGen.generateRules(todayStartMs);

            // 生成订单，event_time 基于当前真实时间前后浮动
            // 用当前时间所在的"虚拟日"生成订单
            long nowMs = System.currentTimeMillis();
            long virtualDayStart = nowMs - (nowMs % 86_400_000L); // 当天 0 点 UTC
            batchGen.generateOrders(virtualDayStart);

            // 修正 event_time：把生成的事件时间偏移到"当前时间附近"
            // 原始 event_time 均匀分布在一天内，现在压缩到 batchInterval 内
            long windowMs = batchIntervalSec * 1000L;
            for (com.saas.datagen.model.PaymentEvent event : batchGen.getAllEvents()) {
                if (event.eventTime != null) {
                    // 把 [dayStart, dayStart+86400s] 映射到 [now, now+windowMs]
                    double progress = (event.eventTime - virtualDayStart) / 86_400_000.0;
                    event.eventTime = nowMs + (long)(progress * windowMs);
                    // ingest_time 也相应调整
                    if (event.ingestTime != null) {
                        long delay = event.ingestTime - event.eventTime + (long)(progress * windowMs);
                        event.ingestTime = event.eventTime + Math.max(0, event.ingestTime - event.eventTime);
                    }
                }
            }

            // 加扰动
            batchGen.applyDisturbances();
            batchGen.sortByIngestTime();

            int eventCount = batchGen.getAllEvents().size();

            // 发到 Kafka
            if (!dryRun && sender != null && running[0]) {
                try {
                    sender.send(batchGen.getAllEvents(), targetTps);
                    totalSent = sender.getSentCount(); // 累计值，直接赋值
                } catch (Exception e) {
                    LOG.error("Batch #{} send error: {}", batchNum, e.getMessage());
                }
            }

            // 更新全局 ID 偏移量（下一批从这里开始）
            seqOffset = batchGen.getOrderSeq();

            long batchDurationMs = System.currentTimeMillis() - batchStartMs;

            LOG.info("Batch #{}: {} events, {}ms, total sent: {}, nextSeqOffset: {}",
                batchNum, eventCount, batchDurationMs, totalSent, seqOffset);

            // 等到下一个 batch interval
            long sleepMs = (batchIntervalSec * 1000L) - batchDurationMs;
            if (sleepMs > 0 && running[0]) {
                Thread.sleep(sleepMs);
            }
        }

        LOG.info("==============================================");
        LOG.info("Streaming stopped. Total batches: {}, Total sent: {}", batchNum, totalSent);
        LOG.info("==============================================");
    }
}

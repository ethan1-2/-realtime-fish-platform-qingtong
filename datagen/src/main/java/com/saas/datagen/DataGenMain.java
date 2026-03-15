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
 * 用法:
 *   java -jar payment-datagen.jar [options]
 *
 * 选项:
 *   --orders=N         每天总订单数 (默认 500000)
 *   --tenants=N        租户数 (默认 100)
 *   --days=N           生成天数 (默认 1)
 *   --tps=N            目标 TPS (默认 0=不限速)
 *   --seed=N           随机种子 (默认 42)
 *   --bootstrap=hosts  Kafka bootstrap servers
 *   --dry-run          不发送 Kafka，只生成真账本
 */
public class DataGenMain {
    private static final Logger LOG = LoggerFactory.getLogger(DataGenMain.class);

    public static void main(String[] args) throws Exception {
        GenConfig config = new GenConfig();
        boolean dryRun = false;
        int targetTps = 0;

        // 解析命令行参数
        for (String arg : args) {
            if (arg.startsWith("--orders=")) config.totalOrdersPerDay = Integer.parseInt(arg.split("=")[1]);
            else if (arg.startsWith("--tenants=")) config.tenantCount = Integer.parseInt(arg.split("=")[1]);
            else if (arg.startsWith("--days=")) config.daysToGenerate = Integer.parseInt(arg.split("=")[1]);
            else if (arg.startsWith("--tps=")) targetTps = Integer.parseInt(arg.split("=")[1]);
            else if (arg.startsWith("--seed=")) config.seed = Long.parseLong(arg.split("=")[1]);
            else if (arg.startsWith("--bootstrap=")) config.bootstrapServers = arg.split("=", 2)[1];
            else if (arg.equals("--dry-run")) dryRun = true;
        }

        LOG.info("==============================================");
        LOG.info("SAAS Payment Data Generator");
        LOG.info("==============================================");
        LOG.info("Tenants: {}", config.tenantCount);
        LOG.info("Orders/day: {}", config.totalOrdersPerDay);
        LOG.info("Days: {}", config.daysToGenerate);
        LOG.info("Seed: {}", config.seed);
        LOG.info("Kafka: {}", config.bootstrapServers);
        LOG.info("Dry run: {}", dryRun);
        LOG.info("Target TPS: {}", targetTps > 0 ? targetTps : "unlimited");
        LOG.info("==============================================");

        DataGenerator generator = new DataGenerator(config);

        // Step 1: 初始化租户
        LOG.info("Step 1: Initializing tenants...");
        generator.initTenants();
        LOG.info("Created {} tenants, top1 share: {}%",
            generator.getTenants().size(),
            String.format("%.1f", generator.getTenants().get(0).trafficShare * 100));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));

        // 用今天作为基准日期
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        for (int day = 0; day < config.daysToGenerate; day++) {
            long dayStartMs = cal.getTimeInMillis();
            String dateStr = sdf.format(cal.getTime());
            LOG.info("=== Generating day {}: {} ===", day + 1, dateStr);

            // Step 2: 生成规则
            LOG.info("Step 2: Generating take rate rules...");
            generator.generateRules(dayStartMs);
            LOG.info("Rule change events: {}", generator.getRuleChangeEvents().size());

            // Step 3: 生成订单和派生事件
            LOG.info("Step 3: Generating orders and derived events...");
            generator.generateOrders(dayStartMs);
            LOG.info("Total events: {}, Ground truth records: {}",
                generator.getAllEvents().size(), generator.getGroundTruth().size());

            cal.add(Calendar.DAY_OF_MONTH, 1);
        }

        // Step 4: 加扰动
        LOG.info("Step 4: Applying disturbances (duplicates, replays)...");
        generator.applyDisturbances();
        LOG.info("After disturbances: {} events", generator.getAllEvents().size());

        // Step 5: 排序
        LOG.info("Step 5: Sorting by ingest time...");
        generator.sortByIngestTime();

        // Step 6: 输出真账本
        LOG.info("Step 6: Writing ground truth...");
        GroundTruthWriter gtWriter = new GroundTruthWriter();
        gtWriter.write(generator.getGroundTruth(), config.groundTruthDir);

        // Step 7: 发送到 Kafka
        if (!dryRun) {
            LOG.info("Step 7: Sending to Kafka...");
            KafkaSender sender = new KafkaSender(config);
            try {
                // 先发规则变更(让 Flink 先收到规则)
                LOG.info("Sending {} rule change events...", generator.getRuleChangeEvents().size());
                sender.send(generator.getRuleChangeEvents(), targetTps);
                Thread.sleep(2000); // 等规则先到

                // 再发业务事件
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
}

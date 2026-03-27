package com.saas.flink.p03;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class CheckpointRecoveryJob {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointRecoveryJob.class);

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = "kafka1-84239:9092,kafka2-84239:9092,kafka3-84239:9092";
        String kafkaTopic = "tp_pay_success";
        String kafkaGroupId = "flink-p03-checkpoint-recovery-v6";
        String dorisFe = "11.99.173.11:8030";
        long startupLookbackMs = 2 * 60 * 1000L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ========== Checkpoint 配置 ==========
        env.enableCheckpointing(30000); // 30s 一次
        CheckpointConfig ckConfig = env.getCheckpointConfig();
        ckConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        ckConfig.setCheckpointTimeout(60000); // 1min 超时
        ckConfig.setMinPauseBetweenCheckpoints(10000); // 两次 checkpoint 间隔至少 10s
        ckConfig.setMaxConcurrentCheckpoints(1);
        ckConfig.setTolerableCheckpointFailureNumber(3); // 容忍 3 次失败
        ckConfig.setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // 取消时保留

        // ========== 重启策略：固定延迟重启 ==========
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
            5,  // 最多重启 5 次
            Time.of(10, TimeUnit.SECONDS) // 每次间隔 10s
        ));

        // Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaBootstrap)
            .setTopics(kafkaTopic)
            .setGroupId(kafkaGroupId)
            // 重部署后回看最近 10 分钟，既能快速看到数据，也避免从 topic 最早位点全量回扫。
            .setStartingOffsets(OffsetsInitializer.timestamp(System.currentTimeMillis() - startupLookbackMs))
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> payStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-pay-success")
            .process(new PaySuccessToDetail())
            .name("parse-to-detail");

        // Doris Sink
        Properties sinkProps = new Properties();
        sinkProps.setProperty("format", "json");
        sinkProps.setProperty("read_json_by_line", "true");
        sinkProps.setProperty("columns", "event_id,event_type,event_time,ingest_time,tenant_id,app_id,channel_id,pay_method,amount_minor,update_time");

        DorisSink.Builder<String> sinkBuilder = DorisSink.builder();
        sinkBuilder.setDorisReadOptions(DorisReadOptions.builder().build())
            .setDorisExecutionOptions(DorisExecutionOptions.builder()
                .setLabelPrefix("p03_checkpoint_recovery")
                .setBufferCount(200)
                .setBufferSize(256 * 1024)
                .setCheckInterval(10000)
                .setMaxRetries(3)
                .setStreamLoadProp(sinkProps)
                .build())
            .setDorisOptions(DorisOptions.builder()
                .setFenodes(dorisFe)
                .setTableIdentifier("saas_payment.dwd_payment_detail")
                .setUsername("root")
                .setPassword("12345678")
                .build())
            .setSerializer(new SimpleStringSerializer());

        payStream.sinkTo(sinkBuilder.build()).name("doris-sink-detail");

        env.execute("P03-Checkpoint-Recovery");
    }

    public static class PaySuccessToDetail extends ProcessFunction<String, String> {
        private transient ObjectMapper mapper;
        private transient SimpleDateFormat dtFmt;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
            dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            dtFmt.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        }

        @Override
        public void processElement(String rawJson, Context ctx, Collector<String> out) {
            try {
                JsonNode node = mapper.readTree(rawJson);
                String eventId = firstNonBlank(getText(node, "event_id"), getText(node, "payment_id"), getText(node, "order_id"));
                Long eventTime = getLong(node, "event_time");
                String tenantId = getText(node, "tenant_id");
                String appId = getText(node, "app_id");
                Long amountMinor = getLong(node, "amount_minor");

                if (isBlank(eventId) || isBlank(tenantId) || isBlank(appId) || eventTime == null || amountMinor == null) {
                    return;
                }

                ObjectNode row = mapper.createObjectNode();
                row.put("event_id", eventId);
                row.put("event_type", "pay_success");
                row.put("event_time", dtFmt.format(new Date(eventTime)));
                row.put("ingest_time", dtFmt.format(new Date()));
                row.put("tenant_id", tenantId);
                row.put("app_id", appId);
                row.put("channel_id", normalize(getText(node, "channel_id"), "unknown"));
                row.put("pay_method", normalize(getText(node, "pay_method"), "unknown"));
                row.put("amount_minor", amountMinor);
                row.put("update_time", dtFmt.format(new Date()));
                out.collect(mapper.writeValueAsString(row));
            } catch (Exception e) {
                LOG.warn("Skip invalid json: {}", e.getMessage());
            }
        }

        private static String getText(JsonNode node, String field) {
            JsonNode v = node.get(field);
            return v != null && !v.isNull() ? v.asText() : null;
        }

        private static Long getLong(JsonNode node, String field) {
            JsonNode v = node.get(field);
            if (v == null || v.isNull()) return null;
            if (v.isNumber()) return v.asLong();
            if (v.isTextual()) {
                try { return Long.parseLong(v.asText()); } catch (NumberFormatException ignored) { return null; }
            }
            return null;
        }

        private static boolean isBlank(String s) { return s == null || s.trim().isEmpty(); }
        private static String firstNonBlank(String... values) {
            for (String value : values) {
                if (!isBlank(value)) return value.trim();
            }
            return null;
        }
        private static String normalize(String s, String fallback) { return isBlank(s) ? fallback : s.trim(); }
    }
}

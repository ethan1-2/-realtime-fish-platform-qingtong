package com.saas.flink.p00;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * ===================================================================
 * P00: pay_success 明细入库 — 使用官方 Doris Flink Connector 1.6.2
 * ===================================================================
 *
 * 【版本变更历史】
 *   v1: 自定义 HTTP Stream Load Sink（绕过 Doris Connector 1.4.0 的 OOM）
 *   v2: 换回官方 Doris Connector 1.6.2（修正 bufferCount=200 避免 OOM）
 *
 * 【v1 OOM 根因回顾】
 *   Doris Connector 内部 RecordBuffer 在构造时预分配 bufferCount 个 ByteBuffer：
 *     内存 = bufferSize × bufferCount
 *   v1 时用默认 bufferCount=2000，即使 bufferSize=512KB：
 *     512KB × 2000 = 1GB/Sink × 2 Sinks = 2GB > taskHeap 1.4GB → OOM
 *   修正：bufferCount=200, bufferSize=256KB → 256KB × 200 = 50MB/Sink × 2 = 100MB，安全。
 *
 * 【v2 相比 v1 的改进】
 *   1. Sink buffer 受 checkpoint 保护（DorisSink 实现了两阶段提交）
 *      - checkpoint 时：preCommit flush buffer → Doris 返回 txn_id
 *      - checkpoint 成功后：commit txn_id → 数据可见
 *      - 失败回滚：abort txn_id → 数据不可见
 *      → 严格 at-least-once（配合 Doris Unique Key 去重 → 效果接近 exactly-once）
 *   2. 不用自己管 HTTP 连接、重试、label 生成、BE 轮询
 *   3. 更好的错误处理和监控指标
 *
 * 【阅读顺序】
 *   1. main()              — Pipeline 骨架
 *   2. PaySuccessValidator  — 解析+校验+转换（和 v1 完全一样）
 *   3. DorisSink 配置       — 重点看 bufferCount/bufferSize 参数
 */
public class PaySuccessIngestJob {

    private static final Logger LOG = LoggerFactory.getLogger(PaySuccessIngestJob.class);

    private static final Set<String> VALID_PAY_METHODS = new HashSet<>(
        Arrays.asList("wx", "alipay", "card", "h5", "quickpay")
    );

    private static final OutputTag<String> DIRTY_TAG =
        new OutputTag<String>("dirty-event", TypeInformation.of(String.class)){};

    // ===================================================================
    // 【1】main() — Pipeline 骨架
    // ===================================================================

    public static void main(String[] args) throws Exception {

        // [1.1] 连接配置
        String kafkaBootstrap = "kafka1-84239:9092,kafka2-84239:9092,kafka3-84239:9092";
        String kafkaTopic     = "tp_pay_success";
        String kafkaGroupId   = "flink-p00-pay-ingest";
        // 【v2 变化】DorisSink 通过 FE 地址自动发现 BE（v1 是直接连 BE）
        String dorisFe        = "11.99.173.11:8030";
        String dorisUser      = "root";
        String dorisPassword  = "12345678";

        // [1.2] Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 【v2 关键】DorisSink 的两阶段提交依赖 Checkpoint：
        //   checkpoint 触发 → preCommit flush → commit txn → 数据可见
        //   数据可见延迟 = checkpoint interval（60 秒）
        env.enableCheckpointing(60000);

        // [1.3] Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaBootstrap)
            .setTopics(kafkaTopic)
            .setGroupId(kafkaGroupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> rawStream = env.fromSource(
            kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-pay-success");

        // [1.4] 解析 + 校验
        SingleOutputStreamOperator<String> validStream = rawStream
            .process(new PaySuccessValidator())
            .name("validate-and-enrich");

        DataStream<String> dirtyStream = validStream.getSideOutput(DIRTY_TAG);

        // [1.5] DorisSink：明细表
        Properties detailProps = new Properties();
        detailProps.setProperty("format", "json");
        detailProps.setProperty("read_json_by_line", "true");

        DorisSink<String> detailSink = DorisSink.<String>builder()
            .setDorisReadOptions(DorisReadOptions.builder().build())
            .setDorisExecutionOptions(DorisExecutionOptions.builder()
                .setLabelPrefix("p00-detail-" + UUID.randomUUID().toString().substring(0, 8))
                .setStreamLoadProp(detailProps)
                .setDeletable(false)
                // 【OOM 修正】bufferCount × bufferSize = 预分配内存
                //   200 × 256KB = 50MB/Sink，2 Sinks = 100MB，安全
                .setBufferCount(200)
                .setBufferSize(256 * 1024)
                .setCheckInterval(10000)
                .setMaxRetries(3)
                .build())
            .setDorisOptions(DorisOptions.builder()
                .setFenodes(dorisFe)
                .setTableIdentifier("saas_payment.p00_payment_detail")
                .setUsername(dorisUser)
                .setPassword(dorisPassword)
                .build())
            .setSerializer(new SimpleStringSerializer())
            .build();

        validStream.sinkTo(detailSink).name("doris-sink-detail");

        // [1.6] DorisSink：脏数据表
        Properties dirtyProps = new Properties();
        dirtyProps.setProperty("format", "json");
        dirtyProps.setProperty("read_json_by_line", "true");

        DorisSink<String> dirtySink = DorisSink.<String>builder()
            .setDorisReadOptions(DorisReadOptions.builder().build())
            .setDorisExecutionOptions(DorisExecutionOptions.builder()
                .setLabelPrefix("p00-dirty-" + UUID.randomUUID().toString().substring(0, 8))
                .setStreamLoadProp(dirtyProps)
                .setDeletable(false)
                .setBufferCount(50)
                .setBufferSize(128 * 1024)
                .setCheckInterval(30000)
                .setMaxRetries(3)
                .build())
            .setDorisOptions(DorisOptions.builder()
                .setFenodes(dorisFe)
                .setTableIdentifier("saas_payment.p00_dirty_event")
                .setUsername(dorisUser)
                .setPassword(dorisPassword)
                .build())
            .setSerializer(new SimpleStringSerializer())
            .build();

        dirtyStream.sinkTo(dirtySink).name("doris-sink-dirty");

        // [1.7] 启动
        env.execute("P00-PaySuccess-Ingest");
    }

    // ===================================================================
    // 【2】PaySuccessValidator — 解析+校验+转换（和 v1 完全一样）
    // ===================================================================

    public static class PaySuccessValidator extends ProcessFunction<String, String> {
        private transient ObjectMapper mapper;
        private transient SimpleDateFormat dateFormat;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
            dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        }

        @Override
        public void processElement(String rawJson, Context ctx, Collector<String> out) throws Exception {
            JsonNode node;
            try {
                node = mapper.readTree(rawJson);
            } catch (Exception e) {
                emitDirty(ctx, "json_parse_error", null, null, rawJson);
                return;
            }

            String tenantId       = getText(node, "tenant_id");
            String appId          = getText(node, "app_id");
            String orderId        = getText(node, "order_id");
            String paymentId      = getText(node, "payment_id");
            String idempotencyKey = getText(node, "idempotency_key");
            String userId         = getText(node, "user_id");
            Long   eventTimeMs    = getLong(node, "event_time");
            Long   amountMinor    = getLong(node, "amount_minor");
            String payMethod      = getText(node, "pay_method");

            if (isBlank(tenantId))       { emitDirty(ctx, "missing_tenant_id",       null,          null,     rawJson); return; }
            if (isBlank(appId))          { emitDirty(ctx, "missing_app_id",          "pay_success", tenantId, rawJson); return; }
            if (isBlank(orderId))        { emitDirty(ctx, "missing_order_id",        "pay_success", tenantId, rawJson); return; }
            if (isBlank(paymentId))      { emitDirty(ctx, "missing_payment_id",      "pay_success", tenantId, rawJson); return; }
            if (isBlank(idempotencyKey)) { emitDirty(ctx, "missing_idempotency_key", "pay_success", tenantId, rawJson); return; }
            if (isBlank(userId))         { emitDirty(ctx, "missing_user_id",         "pay_success", tenantId, rawJson); return; }
            if (eventTimeMs == null)     { emitDirty(ctx, "missing_event_time",      "pay_success", tenantId, rawJson); return; }
            if (amountMinor == null)     { emitDirty(ctx, "missing_amount_minor",    "pay_success", tenantId, rawJson); return; }
            if (amountMinor <= 0)        { emitDirty(ctx, "amount_le_zero:" + amountMinor, "pay_success", tenantId, rawJson); return; }
            if (payMethod != null && !payMethod.isEmpty() && !VALID_PAY_METHODS.contains(payMethod)) {
                emitDirty(ctx, "invalid_pay_method:" + payMethod, "pay_success", tenantId, rawJson); return;
            }

            String eventDate = dateFormat.format(new Date(eventTimeMs));
            SimpleDateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            dtFmt.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));

            ObjectNode output = mapper.createObjectNode();
            output.put("event_date", eventDate);
            output.put("idempotency_key", idempotencyKey);
            output.put("tenant_id", tenantId);
            output.put("app_id", appId);
            output.put("order_id", orderId);
            output.put("payment_id", paymentId);
            output.put("user_id", userId);
            output.put("event_type", "pay_success");
            output.put("amount_minor", amountMinor);
            copyField(output, node, "currency");
            copyField(output, node, "channel_id");
            output.put("pay_method", payMethod != null ? payMethod : "");
            copyField(output, node, "psp");
            copyField(output, node, "region");
            copyLong(output, node, "take_rate_rule_version");
            copyDouble(output, node, "take_rate_pct");
            copyLong(output, node, "take_rate_amount");
            copyLong(output, node, "settlement_amount");
            copyField(output, node, "trace_id");
            output.put("event_time", dtFmt.format(new Date(eventTimeMs)));
            Long ingestTimeMs = getLong(node, "ingest_time");
            if (ingestTimeMs != null) output.put("ingest_time", dtFmt.format(new Date(ingestTimeMs)));
            output.put("process_time", dtFmt.format(new Date()));
            String flag = getText(node, "data_quality_flag");
            output.put("data_quality_flag", flag != null ? flag : "normal");

            out.collect(mapper.writeValueAsString(output));
        }

        private void emitDirty(Context ctx, String reason, String eventType, String tenantId, String raw) {
            try {
                ObjectNode dirty = mapper.createObjectNode();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                SimpleDateFormat dtf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                dirty.put("event_date", df.format(new Date()));
                dirty.put("receive_time", dtf.format(new Date()));
                dirty.put("dirty_reason", reason);
                dirty.put("event_type", eventType != null ? eventType : "");
                dirty.put("tenant_id", tenantId != null ? tenantId : "");
                dirty.put("raw_payload", raw.length() > 2000 ? raw.substring(0, 2000) : raw);
                ctx.output(DIRTY_TAG, mapper.writeValueAsString(dirty));
            } catch (Exception e) {
                LOG.error("Failed to emit dirty: {}", e.getMessage());
            }
        }

        private boolean isBlank(String s) { return s == null || s.isEmpty(); }
        private String getText(JsonNode n, String f) { JsonNode v = n.get(f); return v != null && !v.isNull() ? v.asText() : null; }
        private Long getLong(JsonNode n, String f) { JsonNode v = n.get(f); return v != null && !v.isNull() && v.isNumber() ? v.asLong() : null; }
        private void copyField(ObjectNode o, JsonNode s, String f) { JsonNode v = s.get(f); if (v != null && !v.isNull()) o.put(f, v.asText()); }
        private void copyLong(ObjectNode o, JsonNode s, String f) { JsonNode v = s.get(f); if (v != null && !v.isNull() && v.isNumber()) o.put(f, v.asLong()); }
        private void copyDouble(ObjectNode o, JsonNode s, String f) { JsonNode v = s.get(f); if (v != null && !v.isNull() && v.isNumber()) o.put(f, v.asDouble()); }
    }
}

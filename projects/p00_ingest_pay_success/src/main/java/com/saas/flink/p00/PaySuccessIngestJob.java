package com.saas.flink.p00;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * P00: pay_success 明细入库
 *
 * Kafka tp_pay_success → JSON解析 → 字段校验 → Doris p00_payment_detail
 * 脏数据 → 侧输出 → Doris p00_dirty_event
 *
 * 写入方式: 自定义 Stream Load HTTP Sink (轻量, 不依赖 Doris Flink Connector)
 * 幂等策略: Doris UNIQUE KEY(event_date, idempotency_key) 覆盖写入
 */
public class PaySuccessIngestJob {

    private static final Logger LOG = LoggerFactory.getLogger(PaySuccessIngestJob.class);

    private static final Set<String> VALID_PAY_METHODS = new HashSet<>(
        Arrays.asList("wx", "alipay", "card", "h5", "quickpay")
    );

    private static final OutputTag<String> DIRTY_TAG =
        new OutputTag<String>("dirty-event", TypeInformation.of(String.class)){};

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = "kafka1-84239:9092,kafka2-84239:9092,kafka3-84239:9092";
        String kafkaTopic = "tp_pay_success";
        String kafkaGroupId = "flink-p00-pay-ingest";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaBootstrap)
            .setTopics(kafkaTopic)
            .setGroupId(kafkaGroupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> rawStream = env.fromSource(
            kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-pay-success");

        SingleOutputStreamOperator<String> validStream = rawStream
            .process(new PaySuccessValidator())
            .name("validate-and-enrich");

        DataStream<String> dirtyStream = validStream.getSideOutput(DIRTY_TAG);

        // 明细表 Sink
        validStream.addSink(new DorisStreamLoadSink(
            "11.26.164.150:8040,11.99.173.51:8040,11.26.164.162:8040",
            "saas_payment", "p00_payment_detail",
            "root", "12345678",
            5000, 10  // 每 5000 条或 10 秒 flush 一次
        )).name("doris-sink-detail");

        // 脏数据表 Sink
        dirtyStream.addSink(new DorisStreamLoadSink(
            "11.26.164.150:8040,11.99.173.51:8040,11.26.164.162:8040",
            "saas_payment", "p00_dirty_event",
            "root", "12345678",
            1000, 30
        )).name("doris-sink-dirty");

        env.execute("P00-PaySuccess-Ingest");
    }

    /**
     * 轻量级 Doris Stream Load Sink
     * 攒批后通过 HTTP PUT 调 Doris BE 的 Stream Load API
     */
    public static class DorisStreamLoadSink extends RichSinkFunction<String> {
        private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoadSink.class);

        private final String beNodes;
        private final String database;
        private final String table;
        private final String user;
        private final String password;
        private final int batchSize;
        private final int flushIntervalSec;

        private transient List<String> buffer;
        private transient String[] beList;
        private transient int beIndex;
        private transient long lastFlushTime;
        private transient long labelCounter;

        public DorisStreamLoadSink(String beNodes, String database, String table,
                                    String user, String password,
                                    int batchSize, int flushIntervalSec) {
            this.beNodes = beNodes;
            this.database = database;
            this.table = table;
            this.user = user;
            this.password = password;
            this.batchSize = batchSize;
            this.flushIntervalSec = flushIntervalSec;
        }

        @Override
        public void open(Configuration parameters) {
            buffer = new ArrayList<>(batchSize);
            beList = beNodes.split(",");
            beIndex = 0;
            lastFlushTime = System.currentTimeMillis();
            labelCounter = 0;
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            buffer.add(value);

            boolean sizeReached = buffer.size() >= batchSize;
            boolean timeReached = (System.currentTimeMillis() - lastFlushTime) > flushIntervalSec * 1000L;

            if (sizeReached || timeReached) {
                flush();
            }
        }

        @Override
        public void close() throws Exception {
            if (buffer != null && !buffer.isEmpty()) {
                flush();
            }
        }

        private void flush() throws Exception {
            if (buffer.isEmpty()) return;

            List<String> toSend = new ArrayList<>(buffer);
            buffer.clear();
            lastFlushTime = System.currentTimeMillis();

            // 拼接成 JSON lines
            StringBuilder sb = new StringBuilder();
            for (String line : toSend) {
                sb.append(line).append("\n");
            }
            byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);

            // 轮询 BE 节点
            String be = beList[beIndex % beList.length];
            beIndex++;

            String label = "p00_" + table + "_" + getRuntimeContext().getIndexOfThisSubtask()
                + "_" + System.currentTimeMillis() + "_" + (labelCounter++);

            String urlStr = "http://" + be + "/api/" + database + "/" + table + "/_stream_load";

            int retries = 0;
            int maxRetries = 3;
            while (retries < maxRetries) {
                try {
                    doStreamLoad(urlStr, label + "_r" + retries, data);
                    LOG.info("[{}] Flushed {} rows ({} bytes) to {}", table, toSend.size(), data.length, be);
                    return;
                } catch (Exception e) {
                    retries++;
                    if (retries >= maxRetries) {
                        LOG.error("[{}] Stream load failed after {} retries: {}", table, maxRetries, e.getMessage());
                        throw e;
                    }
                    LOG.warn("[{}] Stream load retry {}: {}", table, retries, e.getMessage());
                    // 切换 BE
                    be = beList[beIndex % beList.length];
                    beIndex++;
                    urlStr = "http://" + be + "/api/" + database + "/" + table + "/_stream_load";
                    Thread.sleep(1000);
                }
            }
        }

        private void doStreamLoad(String urlStr, String label, byte[] data) throws Exception {
            URL url = new URL(urlStr);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("PUT");
            conn.setDoOutput(true);
            conn.setRequestProperty("Authorization", "Basic " +
                Base64.getEncoder().encodeToString((user + ":" + password).getBytes()));
            conn.setRequestProperty("Expect", "100-continue");
            conn.setRequestProperty("label", label);
            conn.setRequestProperty("format", "json");
            conn.setRequestProperty("strip_outer_array", "false");
            conn.setRequestProperty("read_json_by_line", "true");
            conn.setConnectTimeout(10000);
            conn.setReadTimeout(60000);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(data);
                os.flush();
            }

            int responseCode = conn.getResponseCode();
            String response;
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(
                        responseCode >= 400 ? conn.getErrorStream() : conn.getInputStream()))) {
                StringBuilder respSb = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    respSb.append(line);
                }
                response = respSb.toString();
            }

            if (responseCode != 200) {
                throw new RuntimeException("Stream load HTTP " + responseCode + ": " + response);
            }

            // 检查 Doris 返回状态
            if (response.contains("\"Status\":\"Fail\"") || response.contains("\"Status\": \"Fail\"")) {
                throw new RuntimeException("Stream load failed: " + response);
            }
        }
    }

    /**
     * JSON 解析 + 字段校验 + 维度补充
     */
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

            String tenantId = getText(node, "tenant_id");
            String appId = getText(node, "app_id");
            String orderId = getText(node, "order_id");
            String paymentId = getText(node, "payment_id");
            String idempotencyKey = getText(node, "idempotency_key");
            String userId = getText(node, "user_id");
            Long eventTimeMs = getLong(node, "event_time");
            Long amountMinor = getLong(node, "amount_minor");
            String payMethod = getText(node, "pay_method");

            if (isBlank(tenantId)) { emitDirty(ctx, "missing_tenant_id", null, null, rawJson); return; }
            if (isBlank(appId)) { emitDirty(ctx, "missing_app_id", "pay_success", tenantId, rawJson); return; }
            if (isBlank(orderId)) { emitDirty(ctx, "missing_order_id", "pay_success", tenantId, rawJson); return; }
            if (isBlank(paymentId)) { emitDirty(ctx, "missing_payment_id", "pay_success", tenantId, rawJson); return; }
            if (isBlank(idempotencyKey)) { emitDirty(ctx, "missing_idempotency_key", "pay_success", tenantId, rawJson); return; }
            if (isBlank(userId)) { emitDirty(ctx, "missing_user_id", "pay_success", tenantId, rawJson); return; }
            if (eventTimeMs == null) { emitDirty(ctx, "missing_event_time", "pay_success", tenantId, rawJson); return; }
            if (amountMinor == null) { emitDirty(ctx, "missing_amount_minor", "pay_success", tenantId, rawJson); return; }
            if (amountMinor <= 0) { emitDirty(ctx, "amount_le_zero:" + amountMinor, "pay_success", tenantId, rawJson); return; }
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

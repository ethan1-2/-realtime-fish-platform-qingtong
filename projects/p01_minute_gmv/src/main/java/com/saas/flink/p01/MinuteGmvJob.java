package com.saas.flink.p01;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class MinuteGmvJob {

    private static final Logger LOG = LoggerFactory.getLogger(MinuteGmvJob.class);

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = "kafka1-84239:9092,kafka2-84239:9092,kafka3-84239:9092";
        String kafkaTopic = "tp_pay_success";
        String kafkaGroupId = "flink-p01-minute-gmv";
        String dorisBeNodes = "11.26.164.150:8040,11.99.173.51:8040,11.26.164.162:8040";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaBootstrap)
            .setTopics(kafkaTopic)
            .setGroupId(kafkaGroupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        WatermarkStrategy<PaySuccessEvent> watermarkStrategy =
            WatermarkStrategy.<PaySuccessEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<PaySuccessEvent>() {
                    @Override
                    public long extractTimestamp(PaySuccessEvent element, long recordTimestamp) {
                        return element.eventTimeMs;
                    }
                });

        SingleOutputStreamOperator<PaySuccessEvent> payStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-pay-success")
            .process(new PaySuccessParser())
            .name("parse-pay-success")
            .assignTimestampsAndWatermarks(watermarkStrategy)
            .name("assign-watermark");

        DataStream<String> minuteAggStream = payStream
            .keyBy(event -> Tuple4.of(event.tenantId, event.appId, event.channelId, event.payMethod))
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(new CountAmountAggregate(), new MinuteWindowResult())
            .name("minute-gmv-window");

        minuteAggStream.addSink(new DorisStreamLoadSink(
            dorisBeNodes,
            "saas_payment", "p01_settlement_minute",
            "root", "12345678",
            2000, 10
        )).name("doris-sink-minute-gmv");

        env.execute("P01-Minute-GMV");
    }

    public static class PaySuccessParser extends ProcessFunction<String, PaySuccessEvent> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
        }

        @Override
        public void processElement(String rawJson, Context ctx, Collector<PaySuccessEvent> out) throws Exception {
            JsonNode node;
            try {
                node = mapper.readTree(rawJson);
            } catch (Exception e) {
                LOG.warn("Skip invalid json: {}", e.getMessage());
                return;
            }

            String tenantId = getText(node, "tenant_id");
            String appId = getText(node, "app_id");
            String channelId = getText(node, "channel_id");
            String payMethod = getText(node, "pay_method");
            Long eventTimeMs = getLong(node, "event_time");
            Long amountMinor = getLong(node, "amount_minor");

            if (isBlank(tenantId) || isBlank(appId) || eventTimeMs == null || amountMinor == null || amountMinor <= 0) {
                return;
            }

            PaySuccessEvent event = new PaySuccessEvent();
            event.tenantId = tenantId;
            event.appId = appId;
            event.channelId = normalize(channelId, "unknown_channel");
            event.payMethod = normalize(payMethod, "unknown_pay_method");
            event.eventTimeMs = eventTimeMs;
            event.amountMinor = amountMinor;
            out.collect(event);
        }

        private static String getText(JsonNode node, String field) {
            JsonNode value = node.get(field);
            return value != null && !value.isNull() ? value.asText() : null;
        }

        private static Long getLong(JsonNode node, String field) {
            JsonNode value = node.get(field);
            if (value == null || value.isNull()) {
                return null;
            }
            if (value.isNumber()) {
                return value.asLong();
            }
            if (value.isTextual()) {
                try {
                    return Long.parseLong(value.asText());
                } catch (NumberFormatException ignored) {
                    return null;
                }
            }
            return null;
        }

        private static boolean isBlank(String value) {
            return value == null || value.trim().isEmpty();
        }

        private static String normalize(String value, String fallback) {
            return isBlank(value) ? fallback : value.trim();
        }
    }

    public static class CountAmountAggregate implements org.apache.flink.api.common.functions.AggregateFunction<PaySuccessEvent, CountAmountAccumulator, CountAmountAccumulator> {
        @Override
        public CountAmountAccumulator createAccumulator() {
            return new CountAmountAccumulator();
        }

        @Override
        public CountAmountAccumulator add(PaySuccessEvent value, CountAmountAccumulator accumulator) {
            accumulator.payCount += 1L;
            accumulator.payAmount += value.amountMinor;
            return accumulator;
        }

        @Override
        public CountAmountAccumulator getResult(CountAmountAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public CountAmountAccumulator merge(CountAmountAccumulator a, CountAmountAccumulator b) {
            a.payCount += b.payCount;
            a.payAmount += b.payAmount;
            return a;
        }
    }

    public static class MinuteWindowResult extends ProcessWindowFunction<CountAmountAccumulator, String, Tuple4<String, String, String, String>, TimeWindow> {
        private transient ObjectMapper mapper;
        private transient SimpleDateFormat minuteFormat;
        private transient SimpleDateFormat dateFormat;
        private transient SimpleDateFormat datetimeFormat;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
            minuteFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:00");
            dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            TimeZone tz = TimeZone.getTimeZone("Asia/Shanghai");
            minuteFormat.setTimeZone(tz);
            dateFormat.setTimeZone(tz);
            datetimeFormat.setTimeZone(tz);
        }

        @Override
        public void process(
            Tuple4<String, String, String, String> key,
            Context context,
            Iterable<CountAmountAccumulator> elements,
            Collector<String> out
        ) throws Exception {
            CountAmountAccumulator acc = elements.iterator().next();
            long minuteStart = context.window().getStart();
            Date minuteDate = new Date(minuteStart);

            ObjectNode row = mapper.createObjectNode();
            row.put("stat_date", dateFormat.format(minuteDate));
            row.put("stat_minute", minuteFormat.format(minuteDate));
            row.put("tenant_id", key.f0);
            row.put("app_id", key.f1);
            row.put("channel_id", key.f2);
            row.put("pay_method", key.f3);
            row.put("pay_count", acc.payCount);
            row.put("pay_amount", acc.payAmount);
            row.put("net_amount", acc.payAmount);
            row.put("update_time", datetimeFormat.format(new Date()));
            out.collect(mapper.writeValueAsString(row));
        }
    }

    public static class CountAmountAccumulator {
        public long payCount;
        public long payAmount;
    }

    public static class PaySuccessEvent {
        public String tenantId;
        public String appId;
        public String channelId;
        public String payMethod;
        public long eventTimeMs;
        public long amountMinor;
    }

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
            if (buffer.isEmpty()) {
                return;
            }

            List<String> toSend = new ArrayList<>(buffer);
            buffer.clear();
            lastFlushTime = System.currentTimeMillis();

            StringBuilder sb = new StringBuilder();
            for (String line : toSend) {
                sb.append(line).append("\n");
            }
            byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);

            String be = beList[beIndex % beList.length];
            beIndex++;

            String label = "p01_" + table + "_" + getRuntimeContext().getIndexOfThisSubtask()
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
                Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8)));
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
                new InputStreamReader(responseCode >= 400 ? conn.getErrorStream() : conn.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder resp = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    resp.append(line);
                }
                response = resp.toString();
            }

            if (responseCode != 200) {
                throw new RuntimeException("Stream load HTTP " + responseCode + ": " + response);
            }
            if (response.contains("\"Status\":\"Fail\"") || response.contains("\"Status\": \"Fail\"")) {
                throw new RuntimeException("Stream load failed: " + response);
            }
        }
    }
}


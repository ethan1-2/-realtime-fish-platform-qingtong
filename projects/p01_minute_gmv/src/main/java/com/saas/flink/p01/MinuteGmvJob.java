package com.saas.flink.p01;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

public class MinuteGmvJob {

    private static final Logger LOG = LoggerFactory.getLogger(MinuteGmvJob.class);

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = "kafka1-84239:9092,kafka2-84239:9092,kafka3-84239:9092";
        String kafkaTopic = "tp_pay_success";
        String kafkaGroupId = "flink-p01-minute-gmv";
        String dorisFe = "11.99.173.11:8030";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);

        // 1. Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaBootstrap)
            .setTopics(kafkaTopic)
            .setGroupId(kafkaGroupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // 2. Watermark: 5s 乱序容忍
        WatermarkStrategy<PaySuccessEvent> watermarkStrategy =
            WatermarkStrategy.<PaySuccessEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.eventTimeMs);

        // 3. Source -> Parse -> Watermark
        SingleOutputStreamOperator<PaySuccessEvent> payStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-pay-success")
            .process(new PaySuccessParser())
            .name("parse-pay-success")
            .assignTimestampsAndWatermarks(watermarkStrategy)
            .name("assign-watermark");

        // 4. 1分钟滚动窗口聚合
        DataStream<String> minuteAggStream = payStream
            .keyBy(new KeySelector<PaySuccessEvent, Tuple4<String, String, String, String>>() {
                @Override
                public Tuple4<String, String, String, String> getKey(PaySuccessEvent event) {
                    return Tuple4.of(event.tenantId, event.appId, event.channelId, event.payMethod);
                }
            })
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(new CountAmountAggregate(), new MinuteWindowResult())
            .name("minute-gmv-window");

        // 5. Doris Sink (官方 Connector, bufferCount=200 防 OOM)
        Properties sinkProps = new Properties();
        sinkProps.setProperty("format", "json");
        sinkProps.setProperty("read_json_by_line", "true");
        sinkProps.setProperty("columns", "stat_date,stat_minute,tenant_id,app_id,channel_id,pay_method,pay_count,pay_amount,net_amount,update_time");

        DorisSink.Builder<String> sinkBuilder = DorisSink.builder();
        sinkBuilder.setDorisReadOptions(DorisReadOptions.builder().build())
            .setDorisExecutionOptions(DorisExecutionOptions.builder()
                .setLabelPrefix("p01_minute_gmv")
                .setBufferCount(200)
                .setBufferSize(256 * 1024)
                .setCheckInterval(10000)
                .setMaxRetries(3)
                .setStreamLoadProp(sinkProps)
                .build())
            .setDorisOptions(DorisOptions.builder()
                .setFenodes(dorisFe)
                .setTableIdentifier("saas_payment.p01_settlement_minute")
                .setUsername("root")
                .setPassword("12345678")
                .build())
            .setSerializer(new SimpleStringSerializer());

        minuteAggStream.sinkTo(sinkBuilder.build()).name("doris-sink-minute-gmv");

        env.execute("P01-Minute-GMV");
    }

    // ========== 内部类 ==========

    public static class PaySuccessEvent {
        public String tenantId;
        public String appId;
        public String channelId;
        public String payMethod;
        public long eventTimeMs;
        public long amountMinor;
    }

    public static class CountAmountAccumulator {
        public long payCount;
        public long payAmount;
    }

    public static class PaySuccessParser extends ProcessFunction<String, PaySuccessEvent> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
        }

        @Override
        public void processElement(String rawJson, Context ctx, Collector<PaySuccessEvent> out) {
            try {
                JsonNode node = mapper.readTree(rawJson);
                String tenantId = getText(node, "tenant_id");
                String appId = getText(node, "app_id");
                Long eventTimeMs = getLong(node, "event_time");
                Long amountMinor = getLong(node, "amount_minor");

                if (isBlank(tenantId) || isBlank(appId) || eventTimeMs == null || amountMinor == null || amountMinor <= 0) {
                    return;
                }

                PaySuccessEvent event = new PaySuccessEvent();
                event.tenantId = tenantId;
                event.appId = appId;
                event.channelId = normalize(getText(node, "channel_id"), "unknown_channel");
                event.payMethod = normalize(getText(node, "pay_method"), "unknown_pay_method");
                event.eventTimeMs = eventTimeMs;
                event.amountMinor = amountMinor;
                out.collect(event);
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
        private static String normalize(String s, String fallback) { return isBlank(s) ? fallback : s.trim(); }
    }

    public static class CountAmountAggregate implements org.apache.flink.api.common.functions.AggregateFunction<PaySuccessEvent, CountAmountAccumulator, CountAmountAccumulator> {
        @Override public CountAmountAccumulator createAccumulator() { return new CountAmountAccumulator(); }
        @Override public CountAmountAccumulator add(PaySuccessEvent v, CountAmountAccumulator acc) { acc.payCount++; acc.payAmount += v.amountMinor; return acc; }
        @Override public CountAmountAccumulator getResult(CountAmountAccumulator acc) { return acc; }
        @Override public CountAmountAccumulator merge(CountAmountAccumulator a, CountAmountAccumulator b) { a.payCount += b.payCount; a.payAmount += b.payAmount; return a; }
    }

    public static class MinuteWindowResult extends ProcessWindowFunction<CountAmountAccumulator, String, Tuple4<String, String, String, String>, TimeWindow> {
        private transient ObjectMapper mapper;
        private transient SimpleDateFormat minuteFmt;
        private transient SimpleDateFormat dateFmt;
        private transient SimpleDateFormat dtFmt;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
            TimeZone tz = TimeZone.getTimeZone("Asia/Shanghai");
            minuteFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:00"); minuteFmt.setTimeZone(tz);
            dateFmt = new SimpleDateFormat("yyyy-MM-dd"); dateFmt.setTimeZone(tz);
            dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); dtFmt.setTimeZone(tz);
        }

        @Override
        public void process(Tuple4<String, String, String, String> key, Context ctx, Iterable<CountAmountAccumulator> elements, Collector<String> out) throws Exception {
            CountAmountAccumulator acc = elements.iterator().next();
            Date minuteDate = new Date(ctx.window().getStart());

            ObjectNode row = mapper.createObjectNode();
            row.put("stat_date", dateFmt.format(minuteDate));
            row.put("stat_minute", minuteFmt.format(minuteDate));
            row.put("tenant_id", key.f0);
            row.put("app_id", key.f1);
            row.put("channel_id", key.f2);
            row.put("pay_method", key.f3);
            row.put("pay_count", acc.payCount);
            row.put("pay_amount", acc.payAmount);
            row.put("net_amount", acc.payAmount);
            row.put("update_time", dtFmt.format(new Date()));
            out.collect(mapper.writeValueAsString(row));
        }
    }
}

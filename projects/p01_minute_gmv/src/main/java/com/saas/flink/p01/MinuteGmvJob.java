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
        // [教学] 先把连接参数写死在代码里，方便你把 Source -> Window -> Sink 的最小链路先跑起来。
        // [生产] 通常会改成配置文件、启动参数或配置中心，不直接把地址和口令写在源码里。
        String kafkaBootstrap = "kafka1-84239:9092,kafka2-84239:9092,kafka3-84239:9092";
        String kafkaTopic = "tp_pay_success";
        String kafkaGroupId = "flink-p01-minute-gmv";
        String dorisFe = "11.99.173.11:8030";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // [教学] 先只开 checkpoint，让你建立“状态恢复依赖 checkpoint”的基础心智模型。
        // [生产] 通常还会继续配置 checkpoint timeout、最小间隔、外部化 checkpoint、重启策略等。
        env.enableCheckpointing(60000);

        // [教学] Kafka Source 只读取 value，把 Source 的学习范围控制在最小闭环里。
        // [生产] 如果 key / header 有业务语义，也会纳入解析和排障信息。
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaBootstrap)
            .setTopics(kafkaTopic)
            .setGroupId(kafkaGroupId)
            // [教学] 从 earliest 开始，便于反复观察窗口如何累计历史数据。
            // [生产] 更常见是 latest、group committed offset，或按 timestamp 恢复，避免无限扫历史。
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // [教学] P01 的核心是“事件时间窗口”，所以 watermark 必须从业务字段 event_time 提取。
        // [教学] 这里允许 5 秒乱序：事件晚到 5 秒内，仍然能进入原本的分钟窗口。
        // [生产] 乱序容忍要结合真实链路延迟、补发情况和结果延迟目标来调。
        WatermarkStrategy<PaySuccessEvent> watermarkStrategy =
            WatermarkStrategy.<PaySuccessEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event.eventTimeMs);

        // [教学] 先做 JSON 解析和字段清洗，再把干净对象送进事件时间链路。
        // [生产] 这里通常还会增加脏数据旁路、监控指标、失败样本保留。
        SingleOutputStreamOperator<PaySuccessEvent> payStream = env
            .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-pay-success")
            .process(new PaySuccessParser())
            .name("parse-pay-success")
            .assignTimestampsAndWatermarks(watermarkStrategy)
            .name("assign-watermark");

        // [教学] keyBy 决定“一个窗口桶”的业务维度。
        // [教学] 同一个 tenant/app/channel/pay_method 会被聚到同一个 key 上。
        // [生产] key 的选择要同时考虑业务口径和热点风险，维度过粗会热点，过细会状态膨胀。
        DataStream<String> minuteAggStream = payStream
            .keyBy(new KeySelector<PaySuccessEvent, Tuple4<String, String, String, String>>() {
                @Override
                public Tuple4<String, String, String, String> getKey(PaySuccessEvent event) {
                    return Tuple4.of(event.tenantId, event.appId, event.channelId, event.payMethod);
                }
            })
            // [教学] 按事件时间做 1 分钟滚动窗口，是 P01 最核心的窗口类型。
            // [生产] 真正线上常常还会继续加 allowed lateness、迟到侧输出或补算链路。
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            // [教学] AggregateFunction 负责高效增量聚合，ProcessWindowFunction 负责补窗口时间和维度信息。
            // [生产] 这是窗口统计里很常见的写法：既省内存，又能拿到窗口上下文。
            .aggregate(new CountAmountAggregate(), new MinuteWindowResult())
            .name("minute-gmv-window");

        // [教学] 这里已经切到官方 Doris Connector，先让你把“目标配置 + 写入策略 + 序列化”三层分清。
        // [生产] 这比手写 HTTP Sink 更常规，因为它自带 FE 发现、批量 flush、重试和 checkpoint 配合。
        Properties sinkProps = new Properties();
        sinkProps.setProperty("format", "json");
        sinkProps.setProperty("read_json_by_line", "true");
        // [教学] 显式指定 columns，避免 Doris 隐藏列映射干扰学习过程。
        // [生产] 做列映射时要和目标表 schema 严格对齐，升级表结构时也要一起维护。
        sinkProps.setProperty("columns", "stat_date,stat_minute,tenant_id,app_id,channel_id,pay_method,pay_count,pay_amount,net_amount,update_time");

        DorisSink.Builder<String> sinkBuilder = DorisSink.builder();
        sinkBuilder.setDorisReadOptions(DorisReadOptions.builder().build())
            .setDorisExecutionOptions(DorisExecutionOptions.builder()
                // [教学] labelPrefix 用来标记每批 stream load，先知道它是批次标识即可。
                // [生产] label 还承担幂等和排障作用，命名要可追踪。
                .setLabelPrefix("p01_minute_gmv")
                // [教学] 这几个参数控制 sink 内部缓冲区大小、flush 检查频率和失败重试。
                // [生产] 这组参数需要和吞吐、延迟、内存、checkpoint 时长一起调优。
                .setBufferCount(200)
                .setBufferSize(256 * 1024)
                .setCheckInterval(10000)
                .setMaxRetries(3)
                .setStreamLoadProp(sinkProps)
                .build())
            .setDorisOptions(DorisOptions.builder()
                // [教学] DorisOptions 只管“写到哪”：FE 地址、表名、账号。
                // [生产] 账号口令也应外置，不在代码里硬编码。
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

    // [教学] 事件对象先压到最小，只保留窗口聚合必需字段，学习曲线更平滑。
    // [生产] 事件对象字段越多，序列化、网络和状态成本越高，所以也要克制。
    public static class PaySuccessEvent {
        public String tenantId;
        public String appId;
        public String channelId;
        public String payMethod;
        public long eventTimeMs;
        public long amountMinor;
    }

    // [教学] 累加器只放“聚合必需指标”，保持轻量，窗口状态占用才可控。
    // [生产] 不要把维度字段、原始明细塞进累加器，否则状态会很快膨胀。
    public static class CountAmountAccumulator {
        public long payCount;
        public long payAmount;
    }

    public static class PaySuccessParser extends ProcessFunction<String, PaySuccessEvent> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            // [教学] ObjectMapper 这类重对象放在 open 里初始化，避免每条数据反复 new。
            // [生产] 几乎所有可复用解析器、客户端、格式化器都应该遵守这个习惯。
            mapper = new ObjectMapper();
        }

        @Override
        public void processElement(String rawJson, Context ctx, Collector<PaySuccessEvent> out) {
            try {
                JsonNode node = mapper.readTree(rawJson);
                // [教学] P01 故意只抽窗口聚合必须的字段，先把“最小可用链路”吃透。
                // [生产] 真正落地时往往还会补充 trace_id、来源信息、脏数据原因等排障字段。
                String tenantId = getText(node, "tenant_id");
                String appId = getText(node, "app_id");
                Long eventTimeMs = getLong(node, "event_time");
                Long amountMinor = getLong(node, "amount_minor");

                // [教学] 这一步是准入校验：进不了窗口的记录，先直接丢弃，避免污染结果。
                // [生产] 更推荐打 side output 或脏表，而不是静默丢弃，方便后续回溯。
                if (isBlank(tenantId) || isBlank(appId) || eventTimeMs == null || amountMinor == null || amountMinor <= 0) {
                    return;
                }

                PaySuccessEvent event = new PaySuccessEvent();
                event.tenantId = tenantId;
                event.appId = appId;
                // [教学] 维度值缺失时用兜底值，而不是 null，避免 keyBy / Doris 落表出现歧义。
                // [生产] 兜底值要和下游口径约定清楚，避免 unknown_* 被误当成真实业务维度。
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

    // [教学] 这里是增量聚合：每来一条只更新累计值，不会把整个窗口所有明细都攒在内存里。
    // [生产] 只要窗口指标能用加法/累加器表达，都优先考虑这种写法。
    public static class CountAmountAggregate implements org.apache.flink.api.common.functions.AggregateFunction<PaySuccessEvent, CountAmountAccumulator, CountAmountAccumulator> {
        @Override public CountAmountAccumulator createAccumulator() { return new CountAmountAccumulator(); }
        @Override public CountAmountAccumulator add(PaySuccessEvent v, CountAmountAccumulator acc) { acc.payCount++; acc.payAmount += v.amountMinor; return acc; }
        @Override public CountAmountAccumulator getResult(CountAmountAccumulator acc) { return acc; }
        @Override public CountAmountAccumulator merge(CountAmountAccumulator a, CountAmountAccumulator b) { a.payCount += b.payCount; a.payAmount += b.payAmount; return a; }
    }

    // [教学] 这个算子负责“窗口收口”：补窗口起点、维度字段、更新时间，产出最终结果行。
    // [生产] 把“指标计算”和“结果整形”拆开，代码可读性和可维护性更好。
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
            // [教学] 前面已经做过增量聚合，所以这里通常只会拿到一个聚合结果对象。
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

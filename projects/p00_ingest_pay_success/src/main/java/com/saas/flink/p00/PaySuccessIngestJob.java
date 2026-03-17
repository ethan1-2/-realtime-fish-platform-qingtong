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
 * ===================================================================
 * P00: pay_success 明细入库 — 最小闭环练习
 * ===================================================================
 *
 * 【阅读顺序】
 *   1. main()           — 整体 Pipeline 骨架，从上往下读就是数据流方向
 *   2. PaySuccessValidator — 核心处理逻辑（解析+校验+转换）
 *   3. DorisStreamLoadSink — 写入 Doris 的自定义 Sink
 *
 * 【数据流】
 *   Kafka(tp_pay_success)
 *       → [1] Flink KafkaSource 消费原始 JSON 字符串
 *       → [2] ProcessFunction: JSON 解析 + 字段校验 + 维度补充
 *           ├── 合法数据 → 主输出 → [3] DorisStreamLoadSink → p00_payment_detail
 *           └── 脏数据   → 侧输出 → [3] DorisStreamLoadSink → p00_dirty_event
 *
 * 【幂等策略】
 *   不在 Flink 侧维护状态去重，完全依赖 Doris 表的 UNIQUE KEY(event_date, idempotency_key)。
 *   同一条数据写入多次 → Doris merge-on-write 执行 last-write-wins（最后写入覆盖）。
 *
 *   【适用场景限定】
 *   这种幂等策略只适用于：
 *   - 相同幂等键的重复写入是完全相同的 payload（或业务上允许覆盖）
 *   - 不需要按业务时间保留"最新"（Doris 没有 sequence column 时，是按写入顺序覆盖）
 *
 *   【生产边界】
 *   - P00 场景下，重复投递通常是相同 payload，last-write-wins 问题不大
 *   - 到了退款/规则回补场景（P02+），必须引入 sequence column 或 version 字段，
 *     否则乱序到达会导致"旧数据覆盖新数据"
 *   - 当前方案不是端到端 exactly-once（重复数据仍会发 HTTP 到 Doris，浪费网络 IO），
 *     P02 会加 Flink State 去重优化
 *
 *   优点：Flink 侧无状态，重启恢复快，不占 State 内存。
 *   缺点：重复数据仍然会发 HTTP 请求到 Doris（浪费网络 IO），P02 阶段会加 State 去重优化。
 *
 * 【为什么不用 Doris Flink Connector？】
 *   官方 flink-doris-connector-1.15:1.4.0 内部 RecordBuffer 会固定分配大块内存，
 *   在 TaskManager heap 只有 1.4GB 且需要 2 个 Sink 的情况下直接 OOM。
 *   改为自定义 HTTP Stream Load Sink，内存完全可控（只有一个 ArrayList 攒批）。
 *   详见 OPERATION.md 踩坑记录 #3。
 */
public class PaySuccessIngestJob {

    private static final Logger LOG = LoggerFactory.getLogger(PaySuccessIngestJob.class);

    /**
     * 合法的支付方式枚举白名单。
     * 数据生成器会以 0.1% 的概率生成 pay_method="unknown" 来测试校验逻辑。
     * 不在白名单中的 → 进脏数据表。
     */
    private static final Set<String> VALID_PAY_METHODS = new HashSet<>(
        Arrays.asList("wx", "alipay", "card", "h5", "quickpay")
    );

    /**
     * 侧输出标签：脏数据流。
     *
     * 【API 选择】为什么用 OutputTag + ProcessFunction，而不是 filter() 分流？
     *   - filter() 只能做"要/不要"的二分，无法同时输出到两个不同的下游
     *   - OutputTag 侧输出可以在同一个算子中把"合法数据→主输出"和"脏数据→侧输出"分开
     *   - 避免了两次遍历（一次 filter 合法 + 一次 filter 脏数据），性能更好
     *   - ProcessFunction 比 FlatMapFunction 更强大：能访问 Context（侧输出、定时器等）
     *
     * 注意：OutputTag 必须用匿名子类 `new OutputTag<String>("name"){}`，
     *       不能直接 `new OutputTag<>("name")`，否则泛型擦除后 Flink 无法推断类型。
     */
    private static final OutputTag<String> DIRTY_TAG =
        new OutputTag<String>("dirty-event", TypeInformation.of(String.class)){};

    // ===================================================================
    // 【1】main() — Pipeline 骨架，按数据流方向从上往下读
    // ===================================================================

    public static void main(String[] args) throws Exception {

        // ---------------------------------------------------------------
        // [1.1] 连接配置
        // ---------------------------------------------------------------
        String kafkaBootstrap = "kafka1-84239:9092,kafka2-84239:9092,kafka3-84239:9092";
        String kafkaTopic     = "tp_pay_success";
        String kafkaGroupId   = "flink-p00-pay-ingest";
        // Doris BE 节点（注意：Stream Load 直接发给 BE，不经过 FE）
        // 为什么是 BE 而不是 FE？因为 Stream Load 的数据流直接到 BE 写入，
        // FE 只负责元数据。走 FE 会多一跳重定向，且 FE 内存小（3G），不适合承载数据流量。
        String dorisBeNodes   = "11.26.164.150:8040,11.99.173.51:8040,11.26.164.162:8040";

        // ---------------------------------------------------------------
        // [1.2] Flink 执行环境
        // ---------------------------------------------------------------
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启 Checkpoint，间隔 60 秒。
        // 【为什么需要 Checkpoint？】
        //   P00 虽然没有用 Flink State，但 Checkpoint 会触发 Source 持久化消费进度。
        //   对于 KafkaSource，消费进度保存在 Flink 的 checkpoint state 中（不是 Kafka 的 __consumer_offsets）。
        //   Job 重启时，Flink 从最近一次成功的 checkpoint 恢复 Source state，继续从上次的 offset 消费。
        //
        //   【注意】当前项目的消费进度恢复依赖 Flink checkpoint，不依赖 Kafka consumer group offset。
        //   KafkaSource 是否回写 offset 到 Kafka 取决于 connector 版本和配置（Flink 1.15 默认不回写），
        //   所以用 kafka-consumer-groups.sh 可能查不到 group，这不代表消费进度没保存。
        //
        //   如果不开 Checkpoint：
        //   - Source 无法持久化消费进度
        //   - Job 重启后根据 OffsetsInitializer 策略决定从哪里开始（我们设了 earliest → 从头重跑）
        //   - 会导致数据重复消费（好在 Doris Unique Key 能兜底去重）
        env.enableCheckpointing(60000);

        // ---------------------------------------------------------------
        // [1.3] Kafka Source
        // ---------------------------------------------------------------
        // 【API 选择】为什么用 KafkaSource（新版 Source API）而不是 FlinkKafkaConsumer（旧版）？
        //   - KafkaSource 是 Flink 1.14+ 推荐的新 Source API (FLIP-27)
        //   - 支持 Source 并行度动态调整、更好的 Watermark 对齐
        //   - FlinkKafkaConsumer 在 Flink 1.17 已标记 @Deprecated
        //
        //   【消费进度可见性】
        //   本项目中，KafkaSource 的消费进度保存在 Flink checkpoint state，不回写到 Kafka __consumer_offsets。
        //   所以用 kafka-consumer-groups.sh 查不到 consumer group 是正常的（不代表消费进度没保存）。
        //   这个行为取决于 connector 版本和配置，不能把"Web 工具是否可见"当作唯一判断依据。
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaBootstrap)
            .setTopics(kafkaTopic)
            .setGroupId(kafkaGroupId)
            // earliest: 从最早的 offset 开始消费（首次启动时）。
            // Checkpoint 恢复时会忽略这个设置，从 Checkpoint 中保存的 offset 恢复。
            // 【为什么不用 latest？】因为数据生成器先灌数据再启动 Flink Job，
            //   用 latest 会丢掉之前灌入的数据。
            .setStartingOffsets(OffsetsInitializer.earliest())
            // SimpleStringSchema: 只反序列化 value，忽略 key。
            // 因为我们只需要 JSON payload，不需要 Kafka 消息的 key/headers/partition 等元数据。
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // 【API 选择】WatermarkStrategy.noWatermarks()
        //   P00 不做窗口聚合，不需要事件时间和 Watermark。
        //   P01 开始才需要换成 forBoundedOutOfOrderness()。
        DataStream<String> rawStream = env.fromSource(
            kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-pay-success");

        // ---------------------------------------------------------------
        // [1.4] 处理逻辑：JSON 解析 + 校验 + 维度补充
        // ---------------------------------------------------------------
        // 【API 选择】为什么用 process() 而不是 map() 或 flatMap()？
        //   - map(): 1 进 1 出，无法过滤（脏数据没法丢弃）
        //   - flatMap(): 1 进 N 出，可以过滤（不 collect 就是丢弃），但不支持侧输出
        //   - process(): 1 进 N 出 + 侧输出 + 可访问 Context（定时器、广播状态等）
        //   我们需要把脏数据路由到侧输出流 → 必须用 process()
        SingleOutputStreamOperator<String> validStream = rawStream
            .process(new PaySuccessValidator())
            .name("validate-and-enrich");  // 算子命名，在 Flink Web UI 中显示

        // 从主流中取出侧输出（脏数据流）
        DataStream<String> dirtyStream = validStream.getSideOutput(DIRTY_TAG);

        // ---------------------------------------------------------------
        // [1.5] Doris Sink：明细表
        // ---------------------------------------------------------------
        // 【API 选择】为什么用 addSink(RichSinkFunction) 而不是 sinkTo(Sink)？
        //   - sinkTo() 是新版 Sink API (FLIP-143)，需要实现 Sink 接口（较复杂）
        //   - addSink() 是旧版 API，接受 SinkFunction，实现简单
        //   - 我们的自定义 DorisStreamLoadSink 继承 RichSinkFunction，用 addSink() 即可
        //   - 注意：addSink 在未来版本可能被废弃，但 Flink 1.15 完全支持
        validStream.addSink(new DorisStreamLoadSink(
            dorisBeNodes,
            "saas_payment", "p00_payment_detail",
            "root", "12345678",
            5000,  // batchSize: 攒够 5000 条就 flush
            10     // flushIntervalSec: 或者超过 10 秒也 flush（防止低流量时数据卡在 buffer 里）
        )).name("doris-sink-detail");

        // ---------------------------------------------------------------
        // [1.6] Doris Sink：脏数据表
        // ---------------------------------------------------------------
        dirtyStream.addSink(new DorisStreamLoadSink(
            dorisBeNodes,
            "saas_payment", "p00_dirty_event",
            "root", "12345678",
            1000,  // 脏数据量少，攒批阈值可以小一些
            30     // 30 秒 flush，不着急
        )).name("doris-sink-dirty");

        // ---------------------------------------------------------------
        // [1.7] 启动执行
        // ---------------------------------------------------------------
        // Job 名称会显示在 Flink Web UI 和 REST API 中
        env.execute("P00-PaySuccess-Ingest");
    }

    // ===================================================================
    // 【2】PaySuccessValidator — JSON 解析 + 字段校验 + 维度补充
    // ===================================================================

    /**
     * 核心处理算子：接收 Kafka 原始 JSON 字符串，输出 Doris 需要的 JSON 字符串。
     *
     * 【API 选择】为什么继承 ProcessFunction 而不是 RichFlatMapFunction？
     *   ProcessFunction 能访问 Context 对象，从而使用侧输出（ctx.output(DIRTY_TAG, ...)）。
     *   RichFlatMapFunction 没有 Context，无法做侧输出。
     *
     * 【为什么输入输出都是 String？】
     *   - 输入 String：Kafka 消息反序列化为 JSON 字符串（SimpleStringSchema）
     *   - 输出 String：Doris Stream Load 接收 JSON 字符串
     *   - 中间用 Jackson 的 JsonNode 做解析和校验，最终序列化回 String
     *   - 没有定义 POJO 是因为 P00 是最小闭环，不需要类型安全；P01+ 可以考虑引入 POJO
     *
     * 【transient 关键字】
     *   ObjectMapper 和 SimpleDateFormat 不可序列化（Flink 会序列化算子并分发到 TaskManager），
     *   所以标记为 transient，在 open() 方法中初始化（open 在 TaskManager 上执行，不在 Client 上）。
     */
    public static class PaySuccessValidator extends ProcessFunction<String, String> {
        private transient ObjectMapper mapper;
        private transient SimpleDateFormat dateFormat;

        /**
         * [2.1] 初始化：在 TaskManager 上执行，不在提交 Job 的 Client 上。
         * 这里创建的对象不需要序列化传输，所以可以用不可序列化的类型。
         */
        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
            dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            // 时区必须显式设置！默认是 JVM 时区，不同 TaskManager 可能不一致。
            // 生成器用的是 Asia/Shanghai，这里必须对齐，否则 event_date 可能错一天。
            dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        }

        /**
         * [2.2] 核心处理方法：每条 Kafka 消息调用一次。
         *
         * @param rawJson  Kafka 消息的 value（JSON 字符串）
         * @param ctx      上下文，可以做侧输出（ctx.output）、访问时间戳等
         * @param out      主输出收集器，out.collect() 发送到下游（Doris 明细表 Sink）
         */
        @Override
        public void processElement(String rawJson, Context ctx, Collector<String> out) throws Exception {

            // ------ [2.2.1] JSON 解析 ------
            // 如果消息不是合法 JSON（网络损坏、编码错误等），直接进脏数据表
            JsonNode node;
            try {
                node = mapper.readTree(rawJson);
            } catch (Exception e) {
                emitDirty(ctx, "json_parse_error", null, null, rawJson);
                return;  // 不继续处理
            }

            // ------ [2.2.2] 提取字段 ------
            String tenantId       = getText(node, "tenant_id");
            String appId          = getText(node, "app_id");
            String orderId        = getText(node, "order_id");
            String paymentId      = getText(node, "payment_id");
            String idempotencyKey = getText(node, "idempotency_key");
            String userId         = getText(node, "user_id");
            Long   eventTimeMs    = getLong(node, "event_time");
            Long   amountMinor    = getLong(node, "amount_minor");
            String payMethod      = getText(node, "pay_method");

            // ------ [2.2.3] 字段校验 ------
            // 校验顺序：先校验最关键的字段（tenantId），再校验业务字段
            // 每个校验失败都记录具体原因，方便排查数据质量问题
            if (isBlank(tenantId))       { emitDirty(ctx, "missing_tenant_id",       null,          null,     rawJson); return; }
            if (isBlank(appId))          { emitDirty(ctx, "missing_app_id",          "pay_success", tenantId, rawJson); return; }
            if (isBlank(orderId))        { emitDirty(ctx, "missing_order_id",        "pay_success", tenantId, rawJson); return; }
            if (isBlank(paymentId))      { emitDirty(ctx, "missing_payment_id",      "pay_success", tenantId, rawJson); return; }
            if (isBlank(idempotencyKey)) { emitDirty(ctx, "missing_idempotency_key", "pay_success", tenantId, rawJson); return; }
            if (isBlank(userId))         { emitDirty(ctx, "missing_user_id",         "pay_success", tenantId, rawJson); return; }
            if (eventTimeMs == null)     { emitDirty(ctx, "missing_event_time",      "pay_success", tenantId, rawJson); return; }
            if (amountMinor == null)     { emitDirty(ctx, "missing_amount_minor",    "pay_success", tenantId, rawJson); return; }

            // 金额必须 > 0（数据生成器会以 0.05% 概率产生 0 金额，0.01% 产生负金额）
            if (amountMinor <= 0) {
                emitDirty(ctx, "amount_le_zero:" + amountMinor, "pay_success", tenantId, rawJson);
                return;
            }

            // 支付方式枚举校验（数据生成器会以 0.1% 概率产生 "unknown"）
            // 注意：pay_method 允许为空（某些渠道可能没有），但如果有值必须合法
            if (payMethod != null && !payMethod.isEmpty() && !VALID_PAY_METHODS.contains(payMethod)) {
                emitDirty(ctx, "invalid_pay_method:" + payMethod, "pay_success", tenantId, rawJson);
                return;
            }

            // ------ [2.2.4] 维度补充 ------
            // event_date: 从 event_time 毫秒时间戳派生出日期字符串，作为 Doris 分区键
            String eventDate = dateFormat.format(new Date(eventTimeMs));

            // process_time: Flink 处理时间，用于跟踪数据延迟（ingest_time → process_time 的差值）
            SimpleDateFormat dtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            dtFmt.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));

            // ------ [2.2.5] 构建输出 JSON ------
            // 字段顺序和命名必须与 Doris 表 p00_payment_detail 的列名完全匹配，
            // 因为 Stream Load 用 json 格式导入时按字段名映射。
            ObjectNode output = mapper.createObjectNode();
            output.put("event_date",      eventDate);           // Doris 分区键
            output.put("idempotency_key", idempotencyKey);      // Doris Unique Key 的一部分
            output.put("tenant_id",       tenantId);
            output.put("app_id",          appId);
            output.put("order_id",        orderId);
            output.put("payment_id",      paymentId);
            output.put("user_id",         userId);
            output.put("event_type",      "pay_success");       // P00 只处理 pay_success
            output.put("amount_minor",    amountMinor);

            // 可选字段：有就带上，没有就不写（Doris 会填 NULL）
            copyField(output, node, "currency");
            copyField(output, node, "channel_id");
            output.put("pay_method", payMethod != null ? payMethod : "");
            copyField(output, node, "psp");
            copyField(output, node, "region");

            // 抽成相关字段：数据生成器已经计算好了，P00 直接透传
            // （P01/P02 阶段不需要这些，完整场景1才需要）
            copyLong(output, node, "take_rate_rule_version");
            copyDouble(output, node, "take_rate_pct");
            copyLong(output, node, "take_rate_amount");
            copyLong(output, node, "settlement_amount");

            copyField(output, node, "trace_id");

            // 时间字段：毫秒时间戳 → "yyyy-MM-dd HH:mm:ss.SSS" 字符串
            // Doris 的 DATETIME(3) 类型接受这种格式
            output.put("event_time", dtFmt.format(new Date(eventTimeMs)));
            Long ingestTimeMs = getLong(node, "ingest_time");
            if (ingestTimeMs != null) {
                output.put("ingest_time", dtFmt.format(new Date(ingestTimeMs)));
            }
            output.put("process_time", dtFmt.format(new Date()));

            // 数据质量标记（生成器会以 0.2% 概率设为 "suspect" 模拟时区偏移问题）
            String flag = getText(node, "data_quality_flag");
            output.put("data_quality_flag", flag != null ? flag : "normal");

            // 输出到主流 → 下游 DorisStreamLoadSink → p00_payment_detail
            out.collect(mapper.writeValueAsString(output));
        }

        /**
         * [2.3] 发送脏数据到侧输出流。
         *
         * 构建一个匹配 p00_dirty_event 表结构的 JSON 字符串，通过 ctx.output() 发到侧输出。
         * raw_payload 截断到 2000 字符，防止超大异常消息撑爆 Doris 的 TEXT 列。
         */
        private void emitDirty(Context ctx, String reason, String eventType, String tenantId, String raw) {
            try {
                ObjectNode dirty = mapper.createObjectNode();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                SimpleDateFormat dtf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                dirty.put("event_date",    df.format(new Date()));
                dirty.put("receive_time",  dtf.format(new Date()));
                dirty.put("dirty_reason",  reason);
                dirty.put("event_type",    eventType != null ? eventType : "");
                dirty.put("tenant_id",     tenantId != null ? tenantId : "");
                dirty.put("raw_payload",   raw.length() > 2000 ? raw.substring(0, 2000) : raw);
                ctx.output(DIRTY_TAG, mapper.writeValueAsString(dirty));
            } catch (Exception e) {
                // 脏数据处理本身不能再抛异常，否则会导致整个 Job 失败
                LOG.error("Failed to emit dirty: {}", e.getMessage());
            }
        }

        // [2.4] 工具方法：安全地从 JsonNode 中提取字段
        private boolean isBlank(String s)    { return s == null || s.isEmpty(); }
        private String getText(JsonNode n, String f)  { JsonNode v = n.get(f); return v != null && !v.isNull() ? v.asText() : null; }
        private Long getLong(JsonNode n, String f)    { JsonNode v = n.get(f); return v != null && !v.isNull() && v.isNumber() ? v.asLong() : null; }
        private void copyField(ObjectNode o, JsonNode s, String f)  { JsonNode v = s.get(f); if (v != null && !v.isNull()) o.put(f, v.asText()); }
        private void copyLong(ObjectNode o, JsonNode s, String f)   { JsonNode v = s.get(f); if (v != null && !v.isNull() && v.isNumber()) o.put(f, v.asLong()); }
        private void copyDouble(ObjectNode o, JsonNode s, String f) { JsonNode v = s.get(f); if (v != null && !v.isNull() && v.isNumber()) o.put(f, v.asDouble()); }
    }

    // ===================================================================
    // 【3】DorisStreamLoadSink — 自定义轻量级 Doris 写入
    // ===================================================================

    /**
     * 通过 HTTP PUT 调 Doris BE 的 Stream Load API 写入数据。
     *
     * 【API 选择】为什么继承 RichSinkFunction 而不是实现 Sink 接口？
     *   - RichSinkFunction 是旧版 Sink API，实现简单（只需 open/invoke/close）
     *   - Sink 接口（FLIP-143）需要实现 SinkWriter + Committer，适合需要两阶段提交的场景
     *   - 我们的幂等靠 Doris Unique Key，不需要两阶段提交，RichSinkFunction 足够
     *
     * 【为什么用 Stream Load 而不是 JDBC INSERT？】
     *   - JDBC INSERT INTO ... VALUES 是逐行写入，性能差（每条一次 RPC）
     *   - Stream Load 是批量导入，Doris 内部直接生成 Segment 文件，吞吐量高 10-100 倍
     *   - 即使 JDBC 攒批（多行 VALUES），也不如 Stream Load 高效
     *   - Stream Load 直接发给 BE 节点，不经过 FE，减少一跳
     *
     * 【为什么发给 BE 而不是 FE？】
     *   - 如果发给 FE (8030)，FE 会 302 重定向到某个 BE，多一次网络往返
     *   - 直接发给 BE (8040) 跳过重定向，延迟更低
     *   - 我们在客户端做 BE 节点轮询，也起到了负载均衡的作用
     *
     * 【攒批策略】
     *   - 按条数（batchSize）或时间（flushIntervalSec）触发 flush，取先到者
     *   - 按条数：防止 buffer 占用太多内存
     *   - 按时间：防止低流量时数据长期卡在 buffer 不可见（Doris 查不到）
     *
     * 【transient 字段】
     *   buffer、beList 等运行时状态标记为 transient，因为：
     *   - Flink 序列化算子时不应该序列化运行时状态
     *   - 这些状态在 open() 中初始化，在每个 TaskManager 上独立管理
     */
    public static class DorisStreamLoadSink extends RichSinkFunction<String> {
        private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoadSink.class);

        // 构造参数（可序列化，会被 Flink 序列化传输到 TaskManager）
        private final String beNodes;            // BE 节点列表，逗号分隔
        private final String database;           // Doris 数据库名
        private final String table;              // Doris 表名
        private final String user;               // Doris 用户名
        private final String password;           // Doris 密码
        private final int batchSize;             // 攒批条数阈值
        private final int flushIntervalSec;      // 攒批时间阈值（秒）

        // 运行时状态（transient，不序列化，在 open() 中初始化）
        private transient List<String> buffer;   // 攒批缓冲区（JSON 字符串列表）
        private transient String[] beList;        // BE 节点数组
        private transient int beIndex;            // 当前 BE 轮询索引
        private transient long lastFlushTime;     // 上次 flush 的时间戳（毫秒）
        private transient long labelCounter;      // label 计数器，保证唯一性

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

        /**
         * [3.1] 初始化：在 TaskManager 上执行。
         * 每个 Sink 的 subtask 会独立调用一次 open()。
         */
        @Override
        public void open(Configuration parameters) {
            buffer = new ArrayList<>(batchSize);
            beList = beNodes.split(",");
            beIndex = 0;
            lastFlushTime = System.currentTimeMillis();
            labelCounter = 0;
        }

        /**
         * [3.2] 每条数据调用一次。
         * 把 JSON 字符串加入 buffer，达到阈值后触发 flush。
         *
         * 注意：invoke() 和 flush() 在同一个线程中执行（Flink 算子是单线程模型），
         * 所以 buffer 不需要加锁。
         */
        @Override
        public void invoke(String value, Context context) throws Exception {
            buffer.add(value);

            boolean sizeReached = buffer.size() >= batchSize;
            boolean timeReached = (System.currentTimeMillis() - lastFlushTime) > flushIntervalSec * 1000L;

            if (sizeReached || timeReached) {
                flush();
            }
        }

        /**
         * [3.3] 算子生命周期结束时调用（Job 正常结束、取消、失败时）。
         * 把 buffer 中剩余的数据 flush 出去，防止数据丢失。
         *
         * 【重要】close() 不是 checkpoint 回调！
         *   - close() 只在算子销毁时调用（Job 停止/取消/失败），不在 checkpoint 边界调用
         *   - 这意味着：当前 sink buffer 不受 checkpoint 保护
         *   - 如果 Job 在两次 checkpoint 之间崩溃，buffer 中未 flush 的数据会丢失
         *
         * 【生产边界】
         *   P00 为了简化，没有实现 checkpoint 时的 buffer flush（需要实现 CheckpointListener 接口）。
         *   生产环境如果要求严格的 at-least-once，有两种方案：
         *   1. 实现 CheckpointListener，在 notifyCheckpointComplete() 中 flush
         *   2. 用官方 Doris Flink Connector（它实现了两阶段提交）
         *   3. 或者接受"最多丢失 flushIntervalSec 秒的数据"（对于秒级延迟要求的场景可接受）
         *
         *   当前方案的容错语义：
         *   - Kafka offset 由 checkpoint 保护（at-least-once 消费）
         *   - Sink buffer 不受保护（可能丢失最后一批）
         *   - 整体是"at-least-once 消费 + 可能丢失尾部"
         */
        @Override
        public void close() throws Exception {
            if (buffer != null && !buffer.isEmpty()) {
                flush();
            }
        }

        /**
         * [3.4] 核心：把 buffer 中的数据通过 HTTP Stream Load 写入 Doris。
         *
         * 流程：
         *   1. 把 buffer 中的 JSON 字符串拼接成 "JSON lines" 格式（每行一个 JSON 对象）
         *   2. 生成唯一 label（Doris 用 label 做导入去重，同一 label 不会重复导入）
         *   3. HTTP PUT 发给 BE 节点
         *   4. 检查 HTTP 状态码和 Doris 返回的 Status 字段
         *   5. 失败则重试（切换 BE 节点），最多 3 次
         */
        private void flush() throws Exception {
            if (buffer.isEmpty()) return;

            // 取走 buffer 内容，立即清空（为下一批数据腾出空间）
            List<String> toSend = new ArrayList<>(buffer);
            buffer.clear();
            lastFlushTime = System.currentTimeMillis();

            // 拼接成 JSON lines 格式：每行一个 JSON 对象，用换行符分隔
            // 这是 Doris Stream Load 的 "read_json_by_line" 模式要求的格式
            StringBuilder sb = new StringBuilder();
            for (String line : toSend) {
                sb.append(line).append("\n");
            }
            byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);

            // 轮询选择 BE 节点（简单的 round-robin 负载均衡）
            String be = beList[beIndex % beList.length];
            beIndex++;

            // 生成唯一 label
            // 格式：p00_<表名>_<subtask序号>_<时间戳>_<计数器>
            // Doris 要求每次 Stream Load 的 label 全局唯一，重复 label 会被拒绝（这也是一种幂等机制）
            String label = "p00_" + table + "_" + getRuntimeContext().getIndexOfThisSubtask()
                + "_" + System.currentTimeMillis() + "_" + (labelCounter++);

            // Stream Load API URL 格式：http://<be_host>:<be_http_port>/api/<db>/<table>/_stream_load
            String urlStr = "http://" + be + "/api/" + database + "/" + table + "/_stream_load";

            // 重试逻辑：最多 3 次，每次失败切换到下一个 BE 节点
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
                        throw e;  // 最终失败，抛异常让 Flink 重启 Task
                    }
                    LOG.warn("[{}] Stream load retry {}: {}", table, retries, e.getMessage());
                    // 切换到下一个 BE 节点重试
                    be = beList[beIndex % beList.length];
                    beIndex++;
                    urlStr = "http://" + be + "/api/" + database + "/" + table + "/_stream_load";
                    Thread.sleep(1000);  // 等 1 秒再重试
                }
            }
        }

        /**
         * [3.5] 执行一次 HTTP Stream Load 请求。
         *
         * Doris Stream Load 协议要点：
         *   - HTTP 方法：PUT（不是 POST）
         *   - 认证：HTTP Basic Auth
         *   - Header "label"：本次导入的唯一标识，Doris 用它做去重
         *   - Header "format"："json" 表示 JSON 格式
         *   - Header "read_json_by_line"："true" 表示每行一个 JSON 对象
         *   - Header "strip_outer_array"："false" 因为我们不是 JSON 数组格式
         *   - Header "Expect"："100-continue" 让服务端先返回 100 再发数据（HTTP 1.1 标准）
         *   - Body：JSON lines 格式的数据
         *
         * 返回值：HTTP 200 + JSON body 包含 "Status":"Success" 表示成功
         *         即使 HTTP 200，如果 Status 是 "Fail" 也算失败（部分数据格式错误等）
         */
        private void doStreamLoad(String urlStr, String label, byte[] data) throws Exception {
            URL url = new URL(urlStr);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("PUT");
            conn.setDoOutput(true);  // 允许写请求体
            // HTTP Basic Auth：Base64(user:password)
            conn.setRequestProperty("Authorization", "Basic " +
                Base64.getEncoder().encodeToString((user + ":" + password).getBytes()));
            conn.setRequestProperty("Expect", "100-continue");
            conn.setRequestProperty("label", label);
            conn.setRequestProperty("format", "json");
            conn.setRequestProperty("strip_outer_array", "false");
            conn.setRequestProperty("read_json_by_line", "true");
            conn.setConnectTimeout(10000);   // 连接超时 10 秒
            conn.setReadTimeout(60000);      // 读超时 60 秒（大批量数据导入可能较慢）

            // 发送数据
            try (OutputStream os = conn.getOutputStream()) {
                os.write(data);
                os.flush();
            }

            // 读取响应
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

            // HTTP 状态码检查
            if (responseCode != 200) {
                throw new RuntimeException("Stream load HTTP " + responseCode + ": " + response);
            }

            // Doris 业务状态检查：即使 HTTP 200，Doris 可能返回 Status=Fail
            // 常见原因：JSON 字段与表列不匹配、数据类型转换失败等
            if (response.contains("\"Status\":\"Fail\"") || response.contains("\"Status\": \"Fail\"")) {
                throw new RuntimeException("Stream load failed: " + response);
            }
        }
    }
}

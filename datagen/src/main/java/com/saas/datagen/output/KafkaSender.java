package com.saas.datagen.output;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.saas.datagen.config.GenConfig;
import com.saas.datagen.model.PaymentEvent;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka 事件发送器 - 多线程并发发送,支持流速控制
 *
 * 架构:
 * - 按 topic 分组,每个 topic 一个独立 producer 线程
 * - 每个线程内用令牌桶限速到 targetTps
 * - 主线程负责分发事件到各 topic 队列
 */
public class KafkaSender {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSender.class);
    private final GenConfig config;
    private final AtomicLong sentCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final List<KafkaProducer<String, String>> producers = new ArrayList<>();
    private final int numProducers;

    public KafkaSender(GenConfig config) {
        this(config, 4);
    }

    public KafkaSender(GenConfig config, int numProducers) {
        this.config = config;
        this.numProducers = numProducers;

        for (int i = 0; i < numProducers; i++) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "1");
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);       // 64KB batch
            props.put(ProducerConfig.LINGER_MS_CONFIG, 5);            // 5ms linger
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 134217728); // 128MB buffer
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
            producers.add(new KafkaProducer<>(props));
        }
    }

    /**
     * 发送事件列表 - 多线程并发,每个 topic 独立限速
     * @param events 事件列表
     * @param targetTpsPerTopic 每个 topic 的目标 TPS (0 = 不限速)
     */
    public void send(List<PaymentEvent> events, int targetTpsPerTopic) throws Exception {
        // 按 topic 分组
        Map<String, List<IndexedEvent>> topicEvents = new LinkedHashMap<>();
        ObjectMapper mapper = new ObjectMapper();

        for (int i = 0; i < events.size(); i++) {
            PaymentEvent event = events.get(i);
            String topic = routeToTopic(event);
            topicEvents.computeIfAbsent(topic, k -> new ArrayList<>())
                       .add(new IndexedEvent(i, event));
        }

        LOG.info("Sending {} total events across {} topics, target TPS per topic: {}",
            events.size(), topicEvents.size(), targetTpsPerTopic > 0 ? targetTpsPerTopic : "unlimited");

        for (Map.Entry<String, List<IndexedEvent>> entry : topicEvents.entrySet()) {
            LOG.info("  {} : {} events", entry.getKey(), entry.getValue().size());
        }

        // 每个 topic 启动一个发送线程
        ExecutorService executor = Executors.newFixedThreadPool(
            Math.min(topicEvents.size(), numProducers));
        List<Future<?>> futures = new ArrayList<>();
        long startTime = System.currentTimeMillis();

        int producerIdx = 0;
        for (Map.Entry<String, List<IndexedEvent>> entry : topicEvents.entrySet()) {
            String topic = entry.getKey();
            List<IndexedEvent> topicEventList = entry.getValue();
            KafkaProducer<String, String> producer = producers.get(producerIdx % producers.size());
            producerIdx++;

            futures.add(executor.submit(() -> {
                try {
                    sendTopicEvents(producer, topic, topicEventList, targetTpsPerTopic, mapper);
                } catch (Exception e) {
                    LOG.error("Error sending to topic {}: {}", topic, e.getMessage(), e);
                }
            }));
        }

        // 等所有 topic 发送完毕
        for (Future<?> f : futures) {
            f.get();
        }

        // flush 所有 producer
        for (KafkaProducer<String, String> p : producers) {
            p.flush();
        }

        executor.shutdown();

        long totalTime = System.currentTimeMillis() - startTime;
        LOG.info("All topics done! Sent: {}, Errors: {}, Time: {}ms, Avg TPS: {}",
            sentCount.get(), errorCount.get(), totalTime,
            (int)(sentCount.get() * 1000.0 / Math.max(totalTime, 1)));
    }

    /**
     * 单 topic 发送,带令牌桶限速
     */
    private void sendTopicEvents(KafkaProducer<String, String> producer,
                                  String topic,
                                  List<IndexedEvent> events,
                                  int targetTps,
                                  ObjectMapper mapper) throws Exception {
        long startTime = System.currentTimeMillis();
        long windowStart = startTime;
        int windowCount = 0;
        String threadName = Thread.currentThread().getName();

        for (int i = 0; i < events.size(); i++) {
            PaymentEvent event = events.get(i).event;
            String key = buildPartitionKey(event);
            String value = mapper.writeValueAsString(event.toMap());

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    errorCount.incrementAndGet();
                    if (errorCount.get() % 1000 == 1) {
                        LOG.error("[{}] Send error (total: {}): {}", topic, errorCount.get(), exception.getMessage());
                    }
                } else {
                    sentCount.incrementAndGet();
                }
            });

            windowCount++;

            // 令牌桶限速: 每秒不超过 targetTps
            if (targetTps > 0 && windowCount >= targetTps) {
                long elapsed = System.currentTimeMillis() - windowStart;
                if (elapsed < 1000) {
                    Thread.sleep(1000 - elapsed);
                }
                windowStart = System.currentTimeMillis();
                windowCount = 0;
            }

            // 进度日志 (每 10 万条)
            if ((i + 1) % 100000 == 0) {
                long elapsed = System.currentTimeMillis() - startTime;
                int tps = (int)((i + 1) * 1000.0 / Math.max(elapsed, 1));
                LOG.info("[{}] Progress: {}/{} ({}%), TPS: {}",
                    topic, i + 1, events.size(),
                    (int)((i + 1) * 100.0 / events.size()), tps);
            }
        }

        long totalTime = System.currentTimeMillis() - startTime;
        LOG.info("[{}] Done: {} events in {}ms, TPS: {}",
            topic, events.size(), totalTime,
            (int)(events.size() * 1000.0 / Math.max(totalTime, 1)));
    }

    private String routeToTopic(PaymentEvent event) {
        switch (event.eventType) {
            case "pay_success": return config.topicPaySuccess;
            case "refund_success": return config.topicRefundSuccess;
            case "chargeback": return config.topicChargeback;
            case "settlement_adjust": return config.topicSettlementAdjust;
            case "take_rate_rule_change": return config.topicTakeRateRuleChange;
            default: return config.topicPaySuccess;
        }
    }

    private String buildPartitionKey(PaymentEvent event) {
        if (event.tenantId != null && event.orderId != null) {
            return event.tenantId + "|" + event.orderId;
        } else if (event.tenantId != null) {
            return event.tenantId;
        }
        return UUID.randomUUID().toString();
    }

    public void close() {
        for (KafkaProducer<String, String> p : producers) {
            p.close();
        }
    }

    public long getSentCount() { return sentCount.get(); }
    public long getErrorCount() { return errorCount.get(); }

    private static class IndexedEvent {
        final int index;
        final PaymentEvent event;
        IndexedEvent(int index, PaymentEvent event) {
            this.index = index;
            this.event = event;
        }
    }
}

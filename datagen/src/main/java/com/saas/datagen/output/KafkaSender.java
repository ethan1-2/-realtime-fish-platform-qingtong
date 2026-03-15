package com.saas.datagen.output;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.saas.datagen.config.GenConfig;
import com.saas.datagen.model.PaymentEvent;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka 事件发送器 - 按 ingestTime 顺序发送,支持流速控制
 */
public class KafkaSender {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSender.class);
    private final GenConfig config;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicLong sentCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);

    public KafkaSender(GenConfig config) {
        this.config = config;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * 发送事件列表(已按 ingestTime 排序)
     * @param events 事件列表
     * @param targetTps 目标 TPS (0 = 不限速)
     */
    public void send(List<PaymentEvent> events, int targetTps) throws Exception {
        LOG.info("Sending {} events to Kafka, target TPS: {}", events.size(),
            targetTps > 0 ? targetTps : "unlimited");

        long startTime = System.currentTimeMillis();
        long windowStart = startTime;
        int windowCount = 0;

        for (int i = 0; i < events.size(); i++) {
            PaymentEvent event = events.get(i);
            String topic = routeToTopic(event);
            String key = buildPartitionKey(event);
            String value = mapper.writeValueAsString(event.toMap());

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    errorCount.incrementAndGet();
                    if (errorCount.get() % 100 == 1) {
                        LOG.error("Send error (total: {}): {}", errorCount.get(), exception.getMessage());
                    }
                } else {
                    sentCount.incrementAndGet();
                }
            });

            windowCount++;

            // 流速控制
            if (targetTps > 0 && windowCount >= targetTps) {
                long elapsed = System.currentTimeMillis() - windowStart;
                if (elapsed < 1000) {
                    Thread.sleep(1000 - elapsed);
                }
                windowStart = System.currentTimeMillis();
                windowCount = 0;
            }

            // 进度日志
            if ((i + 1) % 50000 == 0) {
                long elapsed = System.currentTimeMillis() - startTime;
                double tps = (i + 1) * 1000.0 / elapsed;
                LOG.info("Progress: {}/{} ({}%), TPS: {}, errors: {}",
                    i + 1, events.size(), (int)((i + 1) * 100.0 / events.size()), (int)tps, errorCount.get());
            }
        }

        producer.flush();

        long totalTime = System.currentTimeMillis() - startTime;
        LOG.info("Done! Sent: {}, Errors: {}, Time: {}ms, Avg TPS: {}",
            sentCount.get(), errorCount.get(), totalTime,
            (int)(sentCount.get() * 1000.0 / Math.max(totalTime, 1)));
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
        // 分区键: tenant_id + order_id 保证同一订单的事件在同一分区
        if (event.tenantId != null && event.orderId != null) {
            return event.tenantId + "|" + event.orderId;
        } else if (event.tenantId != null) {
            return event.tenantId;
        }
        return UUID.randomUUID().toString();
    }

    public void close() {
        producer.close();
    }

    public long getSentCount() { return sentCount.get(); }
    public long getErrorCount() { return errorCount.get(); }
}

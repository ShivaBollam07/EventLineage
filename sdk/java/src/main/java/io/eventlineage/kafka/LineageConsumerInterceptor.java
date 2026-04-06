package io.eventlineage.kafka;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

/**
 * Kafka ConsumerInterceptor that automatically extracts lineage headers
 * and sets lineage context for downstream produces.
 *
 * <p>Zero-code instrumentation: add this interceptor via configuration:
 * <pre>
 * interceptor.classes=io.eventlineage.kafka.LineageConsumerInterceptor
 * lineage.service.name=my-service
 * </pre>
 */
public class LineageConsumerInterceptor implements ConsumerInterceptor<Object, Object> {

    private static final Logger log = LoggerFactory.getLogger(LineageConsumerInterceptor.class);

    private String serviceName;
    private String lineageEventsTopic;
    private String tenantId;
    private boolean emitConsumeEvents;
    private Producer<String, String> lineageProducer;

    @Override
    public void configure(Map<String, ?> configs) {
        this.serviceName = getRequiredConfig(configs, LineageHeaders.CONFIG_SERVICE_NAME);
        this.lineageEventsTopic = getConfig(configs, LineageHeaders.CONFIG_EVENTS_TOPIC,
                LineageHeaders.LINEAGE_EVENTS_TOPIC);
        this.tenantId = getConfig(configs, LineageHeaders.CONFIG_TENANT_ID, null);
        this.emitConsumeEvents = Boolean.parseBoolean(
                getConfig(configs, "lineage.emit.consume.events", "true"));

        if (emitConsumeEvents) {
            String lineageBootstrap = getConfig(configs, LineageHeaders.CONFIG_BOOTSTRAP_SERVERS,
                    (String) configs.get("bootstrap.servers"));

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, lineageBootstrap);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "1");

            this.lineageProducer = new KafkaProducer<>(props);
        }

        log.info("LineageConsumerInterceptor configured for service: {}", serviceName);
    }

    @Override
    public ConsumerRecords<Object, Object> onConsume(ConsumerRecords<Object, Object> records) {
        for (ConsumerRecord<Object, Object> record : records) {
            processRecord(record);
        }
        return records;
    }

    private void processRecord(ConsumerRecord<Object, Object> record) {
        Headers headers = record.headers();

        // Extract lineage headers
        String messageId = getHeaderValue(headers, LineageHeaders.MESSAGE_ID);
        String correlationId = getHeaderValue(headers, LineageHeaders.CORRELATION_ID);
        String rootMessageId = getHeaderValue(headers, LineageHeaders.ROOT_MESSAGE_ID);
        String parentId = getHeaderValue(headers, LineageHeaders.PARENT_ID);
        String causationReason = getHeaderValue(headers, LineageHeaders.CAUSATION_REASON);

        int retryCount = 0;
        String retryCountStr = getHeaderValue(headers, LineageHeaders.RETRY_COUNT);
        if (retryCountStr != null) {
            try {
                retryCount = Integer.parseInt(retryCountStr);
            } catch (NumberFormatException e) {
                // Ignore
            }
        }

        // Use defaults if headers are missing
        if (messageId == null || messageId.isEmpty()) {
            messageId = UUID.randomUUID().toString();
        }
        if (correlationId == null || correlationId.isEmpty()) {
            correlationId = messageId;
        }
        if (rootMessageId == null || rootMessageId.isEmpty()) {
            rootMessageId = messageId;
        }

        // Set lineage context for downstream produces
        LineageContext context = new LineageContext(messageId, correlationId, rootMessageId, retryCount);
        LineageContext.set(context);

        log.debug("Set lineage context: messageId={}, correlationId={}", messageId, correlationId);

        // Emit consume lineage event
        if (emitConsumeEvents && lineageProducer != null) {
            emitConsumeEvent(record, messageId, parentId, correlationId, rootMessageId,
                    causationReason, retryCount);
        }
    }

    private void emitConsumeEvent(
            ConsumerRecord<Object, Object> record,
            String messageId,
            String parentId,
            String correlationId,
            String rootMessageId,
            String causationReason,
            int retryCount) {

        LineageEvent event = LineageEvent.builder()
                .eventType(LineageEvent.EventType.CONSUMED)
                .messageId(messageId)
                .parentId(parentId)
                .correlationId(correlationId)
                .rootMessageId(rootMessageId)
                .causationReason(causationReason)
                .serviceOrigin(serviceName)
                .topic(record.topic())
                .retryCount(retryCount)
                .timestamp(Instant.now())
                .status(LineageEvent.MessageStatus.OK)
                .tenantId(tenantId)
                .partition(record.partition())
                .offset(record.offset())
                .build();

        try {
            lineageProducer.send(new ProducerRecord<>(
                    lineageEventsTopic,
                    correlationId,
                    event.toJson()
            ));
        } catch (Exception e) {
            log.warn("Failed to emit consume lineage event: {}", e.getMessage());
        }
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // Clear context after commit to prevent leakage
        // Note: This may not align perfectly with processing in all cases
    }

    @Override
    public void close() {
        LineageContext.clear();
        if (lineageProducer != null) {
            lineageProducer.close();
        }
    }

    private String getHeaderValue(Headers headers, String key) {
        Header header = headers.lastHeader(key);
        if (header != null && header.value() != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return null;
    }

    private String getRequiredConfig(Map<String, ?> configs, String key) {
        Object value = configs.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Required configuration missing: " + key);
        }
        return value.toString();
    }

    private String getConfig(Map<String, ?> configs, String key, String defaultValue) {
        Object value = configs.get(key);
        return value != null ? value.toString() : defaultValue;
    }
}

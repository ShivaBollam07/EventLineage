package io.eventlineage.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

/**
 * Kafka ProducerInterceptor that automatically injects lineage headers
 * and emits lineage events for every produced message.
 *
 * <p>Zero-code instrumentation: add this interceptor via configuration:
 * <pre>
 * interceptor.classes=io.eventlineage.kafka.LineageProducerInterceptor
 * lineage.service.name=my-service
 * </pre>
 *
 * <p>Configuration options:
 * <ul>
 *   <li>lineage.service.name (required): Name of this service</li>
 *   <li>lineage.events.topic: Topic for lineage events (default: _lineage.events)</li>
 *   <li>lineage.bootstrap.servers: Separate Kafka cluster for lineage (default: same as bootstrap.servers)</li>
 *   <li>lineage.tenant.id: Tenant ID for multi-tenancy</li>
 *   <li>lineage.payload.preview.enabled: Include payload preview (default: true)</li>
 *   <li>lineage.payload.preview.max.length: Max preview length (default: 500)</li>
 * </ul>
 */
public class LineageProducerInterceptor implements ProducerInterceptor<Object, Object> {

    private static final Logger log = LoggerFactory.getLogger(LineageProducerInterceptor.class);

    private String serviceName;
    private String lineageEventsTopic;
    private String tenantId;
    private boolean payloadPreviewEnabled;
    private int payloadPreviewMaxLength;
    private Producer<String, String> lineageProducer;

    @Override
    public void configure(Map<String, ?> configs) {
        this.serviceName = getRequiredConfig(configs, LineageHeaders.CONFIG_SERVICE_NAME);
        this.lineageEventsTopic = getConfig(configs, LineageHeaders.CONFIG_EVENTS_TOPIC,
                LineageHeaders.LINEAGE_EVENTS_TOPIC);
        this.tenantId = getConfig(configs, LineageHeaders.CONFIG_TENANT_ID, null);
        this.payloadPreviewEnabled = Boolean.parseBoolean(
                getConfig(configs, LineageHeaders.CONFIG_PAYLOAD_PREVIEW_ENABLED, "true"));
        this.payloadPreviewMaxLength = Integer.parseInt(
                getConfig(configs, LineageHeaders.CONFIG_PAYLOAD_PREVIEW_MAX_LENGTH,
                        String.valueOf(LineageHeaders.DEFAULT_PAYLOAD_PREVIEW_MAX_LENGTH)));

        // Create producer for lineage events
        String lineageBootstrap = getConfig(configs, LineageHeaders.CONFIG_BOOTSTRAP_SERVERS,
                (String) configs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, lineageBootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        this.lineageProducer = new KafkaProducer<>(props);

        log.info("LineageProducerInterceptor configured for service: {}", serviceName);
    }

    @Override
    public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> record) {
        // Generate message ID
        String messageId = UUID.randomUUID().toString();
        Instant timestamp = Instant.now();

        // Get lineage context (set by consumer interceptor)
        LineageContext context = LineageContext.current();

        // Determine lineage fields
        String parentId = null;
        String correlationId;
        String rootMessageId;
        String causationReason = null;

        if (context != null) {
            parentId = context.getMessageId();
            correlationId = context.getCorrelationId();
            rootMessageId = context.getRootMessageId();
            causationReason = context.consumeCausationReason();
        } else {
            // This is a root message
            correlationId = UUID.randomUUID().toString();
            rootMessageId = messageId;
        }

        // Check for existing causation reason header
        if (causationReason == null) {
            causationReason = getHeaderValue(record.headers(), LineageHeaders.CAUSATION_REASON);
        }

        // Get retry count from headers
        int retryCount = 0;
        String retryCountStr = getHeaderValue(record.headers(), LineageHeaders.RETRY_COUNT);
        if (retryCountStr != null) {
            try {
                retryCount = Integer.parseInt(retryCountStr);
            } catch (NumberFormatException e) {
                // Ignore
            }
        }

        // Add lineage headers
        Headers headers = record.headers();
        addHeader(headers, LineageHeaders.MESSAGE_ID, messageId);
        addHeader(headers, LineageHeaders.CORRELATION_ID, correlationId);
        addHeader(headers, LineageHeaders.ROOT_MESSAGE_ID, rootMessageId);
        addHeader(headers, LineageHeaders.SERVICE_ORIGIN, serviceName);
        addHeader(headers, LineageHeaders.TIMESTAMP, timestamp.toString());

        if (parentId != null) {
            addHeader(headers, LineageHeaders.PARENT_ID, parentId);
            addHeader(headers, LineageHeaders.CAUSATION_ID, parentId);
        }
        if (causationReason != null) {
            addHeader(headers, LineageHeaders.CAUSATION_REASON, causationReason);
        }
        if (tenantId != null) {
            addHeader(headers, LineageHeaders.TENANT_ID, tenantId);
        }

        // Emit lineage event
        emitLineageEvent(
                LineageEvent.EventType.PRODUCED,
                messageId,
                parentId,
                correlationId,
                rootMessageId,
                causationReason,
                record.topic(),
                retryCount,
                timestamp,
                record.value()
        );

        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // Could emit additional event on delivery confirmation
    }

    @Override
    public void close() {
        if (lineageProducer != null) {
            lineageProducer.close();
        }
    }

    private void emitLineageEvent(
            LineageEvent.EventType eventType,
            String messageId,
            String parentId,
            String correlationId,
            String rootMessageId,
            String causationReason,
            String topic,
            int retryCount,
            Instant timestamp,
            Object payload) {

        LineageEvent event = LineageEvent.builder()
                .eventType(eventType)
                .messageId(messageId)
                .parentId(parentId)
                .correlationId(correlationId)
                .rootMessageId(rootMessageId)
                .causationReason(causationReason)
                .serviceOrigin(serviceName)
                .topic(topic)
                .retryCount(retryCount)
                .timestamp(timestamp)
                .status(LineageEvent.MessageStatus.OK)
                .tenantId(tenantId)
                .build();

        if (payloadPreviewEnabled && payload != null) {
            event.setPayloadPreview(truncatePayload(payload.toString()));
        }

        try {
            lineageProducer.send(new ProducerRecord<>(
                    lineageEventsTopic,
                    correlationId,
                    event.toJson()
            ));
        } catch (Exception e) {
            log.warn("Failed to emit lineage event: {}", e.getMessage());
        }
    }

    private String truncatePayload(String payload) {
        if (payload.length() > payloadPreviewMaxLength) {
            return payload.substring(0, payloadPreviewMaxLength) + "...";
        }
        return payload;
    }

    private void addHeader(Headers headers, String key, String value) {
        if (value != null) {
            // Remove existing header if present
            headers.remove(key);
            headers.add(key, value.getBytes(StandardCharsets.UTF_8));
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

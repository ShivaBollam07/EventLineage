package io.eventlineage.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Instant;

/**
 * Represents a lineage event to be published to the lineage events topic.
 * This class mirrors the Go backend's envelope.LineageEvent struct.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LineageEvent {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public enum EventType {
        @JsonProperty("produced")
        PRODUCED,
        @JsonProperty("consumed")
        CONSUMED
    }

    public enum MessageStatus {
        @JsonProperty("ok")
        OK,
        @JsonProperty("failed")
        FAILED,
        @JsonProperty("pending")
        PENDING
    }

    @JsonProperty("eventType")
    private EventType eventType;

    @JsonProperty("messageId")
    private String messageId;

    @JsonProperty("parentId")
    private String parentId;

    @JsonProperty("correlationId")
    private String correlationId;

    @JsonProperty("rootMessageId")
    private String rootMessageId;

    @JsonProperty("causationReason")
    private String causationReason;

    @JsonProperty("serviceOrigin")
    private String serviceOrigin;

    @JsonProperty("topic")
    private String topic;

    @JsonProperty("retryCount")
    private int retryCount;

    @JsonProperty("timestamp")
    private Instant timestamp;

    @JsonProperty("status")
    private MessageStatus status;

    @JsonProperty("payloadPreview")
    private String payloadPreview;

    @JsonProperty("tenantId")
    private String tenantId;

    @JsonProperty("partition")
    private Integer partition;

    @JsonProperty("offset")
    private Long offset;

    public LineageEvent() {
        this.timestamp = Instant.now();
        this.status = MessageStatus.OK;
    }

    public static Builder builder() {
        return new Builder();
    }

    // Getters and setters
    public EventType getEventType() { return eventType; }
    public void setEventType(EventType eventType) { this.eventType = eventType; }

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }

    public String getParentId() { return parentId; }
    public void setParentId(String parentId) { this.parentId = parentId; }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    public String getRootMessageId() { return rootMessageId; }
    public void setRootMessageId(String rootMessageId) { this.rootMessageId = rootMessageId; }

    public String getCausationReason() { return causationReason; }
    public void setCausationReason(String causationReason) { this.causationReason = causationReason; }

    public String getServiceOrigin() { return serviceOrigin; }
    public void setServiceOrigin(String serviceOrigin) { this.serviceOrigin = serviceOrigin; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    public MessageStatus getStatus() { return status; }
    public void setStatus(MessageStatus status) { this.status = status; }

    public String getPayloadPreview() { return payloadPreview; }
    public void setPayloadPreview(String payloadPreview) {
        if (payloadPreview != null && payloadPreview.length() > LineageHeaders.DEFAULT_PAYLOAD_PREVIEW_MAX_LENGTH) {
            this.payloadPreview = payloadPreview.substring(0, LineageHeaders.DEFAULT_PAYLOAD_PREVIEW_MAX_LENGTH) + "...";
        } else {
            this.payloadPreview = payloadPreview;
        }
    }

    public String getTenantId() { return tenantId; }
    public void setTenantId(String tenantId) { this.tenantId = tenantId; }

    public Integer getPartition() { return partition; }
    public void setPartition(Integer partition) { this.partition = partition; }

    public Long getOffset() { return offset; }
    public void setOffset(Long offset) { this.offset = offset; }

    public boolean isRootMessage() {
        return parentId == null || parentId.isEmpty() || messageId.equals(rootMessageId);
    }

    public boolean isRetry() {
        return retryCount > 0;
    }

    public boolean isFailed() {
        return status == MessageStatus.FAILED;
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize LineageEvent", e);
        }
    }

    public static LineageEvent fromJson(String json) {
        try {
            return MAPPER.readValue(json, LineageEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize LineageEvent", e);
        }
    }

    public static class Builder {
        private final LineageEvent event = new LineageEvent();

        public Builder eventType(EventType eventType) {
            event.setEventType(eventType);
            return this;
        }

        public Builder messageId(String messageId) {
            event.setMessageId(messageId);
            return this;
        }

        public Builder parentId(String parentId) {
            event.setParentId(parentId);
            return this;
        }

        public Builder correlationId(String correlationId) {
            event.setCorrelationId(correlationId);
            return this;
        }

        public Builder rootMessageId(String rootMessageId) {
            event.setRootMessageId(rootMessageId);
            return this;
        }

        public Builder causationReason(String causationReason) {
            event.setCausationReason(causationReason);
            return this;
        }

        public Builder serviceOrigin(String serviceOrigin) {
            event.setServiceOrigin(serviceOrigin);
            return this;
        }

        public Builder topic(String topic) {
            event.setTopic(topic);
            return this;
        }

        public Builder retryCount(int retryCount) {
            event.setRetryCount(retryCount);
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            event.setTimestamp(timestamp);
            return this;
        }

        public Builder status(MessageStatus status) {
            event.setStatus(status);
            return this;
        }

        public Builder payloadPreview(String payloadPreview) {
            event.setPayloadPreview(payloadPreview);
            return this;
        }

        public Builder tenantId(String tenantId) {
            event.setTenantId(tenantId);
            return this;
        }

        public Builder partition(Integer partition) {
            event.setPartition(partition);
            return this;
        }

        public Builder offset(Long offset) {
            event.setOffset(offset);
            return this;
        }

        public LineageEvent build() {
            return event;
        }
    }
}

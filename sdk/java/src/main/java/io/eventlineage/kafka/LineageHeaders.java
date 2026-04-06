package io.eventlineage.kafka;

/**
 * Header key constants for lineage metadata.
 * These must match the Go backend's envelope/headers.go definitions.
 */
public final class LineageHeaders {

    private LineageHeaders() {
        // Utility class
    }

    public static final String MESSAGE_ID = "x-lineage-message-id";
    public static final String PARENT_ID = "x-lineage-parent-id";
    public static final String CORRELATION_ID = "x-lineage-correlation-id";
    public static final String CAUSATION_ID = "x-lineage-causation-id";
    public static final String CAUSATION_REASON = "x-lineage-causation-reason";
    public static final String ROOT_MESSAGE_ID = "x-lineage-root-id";
    public static final String SERVICE_ORIGIN = "x-lineage-service";
    public static final String RETRY_COUNT = "x-lineage-retry-count";
    public static final String TIMESTAMP = "x-lineage-timestamp";
    public static final String TENANT_ID = "x-lineage-tenant-id";

    /** Topic for lineage events */
    public static final String LINEAGE_EVENTS_TOPIC = "_lineage.events";

    /** Configuration keys */
    public static final String CONFIG_SERVICE_NAME = "lineage.service.name";
    public static final String CONFIG_EVENTS_TOPIC = "lineage.events.topic";
    public static final String CONFIG_BOOTSTRAP_SERVERS = "lineage.bootstrap.servers";
    public static final String CONFIG_TENANT_ID = "lineage.tenant.id";
    public static final String CONFIG_PAYLOAD_PREVIEW_ENABLED = "lineage.payload.preview.enabled";
    public static final String CONFIG_PAYLOAD_PREVIEW_MAX_LENGTH = "lineage.payload.preview.max.length";

    /** Default values */
    public static final int DEFAULT_PAYLOAD_PREVIEW_MAX_LENGTH = 500;
    public static final int DEFAULT_CHAIN_DEPTH_LIMIT = 20;
}

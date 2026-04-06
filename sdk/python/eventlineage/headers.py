"""
Header key constants for lineage metadata.
These must match the Go backend's envelope/headers.go definitions.
"""


class LineageHeaders:
    """Kafka header keys for lineage metadata."""

    MESSAGE_ID = "x-lineage-message-id"
    PARENT_ID = "x-lineage-parent-id"
    CORRELATION_ID = "x-lineage-correlation-id"
    CAUSATION_ID = "x-lineage-causation-id"
    CAUSATION_REASON = "x-lineage-causation-reason"
    ROOT_MESSAGE_ID = "x-lineage-root-id"
    SERVICE_ORIGIN = "x-lineage-service"
    RETRY_COUNT = "x-lineage-retry-count"
    TIMESTAMP = "x-lineage-timestamp"
    TENANT_ID = "x-lineage-tenant-id"


# Topic for lineage events
LINEAGE_EVENTS_TOPIC = "_lineage.events"

# Default configuration
DEFAULT_PAYLOAD_PREVIEW_MAX_LENGTH = 500
DEFAULT_CHAIN_DEPTH_LIMIT = 20

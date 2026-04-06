// Package envelope defines the message envelope contract shared between
// the EventLineage server and all SDK implementations (Go, Python, Java).
package envelope

// Header key constants for lineage metadata.
// These headers are injected into Kafka message headers by the SDK
// and read by the ingestion service.
const (
	HeaderMessageID       = "x-lineage-message-id"
	HeaderParentID        = "x-lineage-parent-id"
	HeaderCorrelationID   = "x-lineage-correlation-id"
	HeaderCausationID     = "x-lineage-causation-id"
	HeaderCausationReason = "x-lineage-causation-reason"
	HeaderRootMessageID   = "x-lineage-root-id"
	HeaderServiceOrigin   = "x-lineage-service"
	HeaderRetryCount      = "x-lineage-retry-count"
	HeaderTimestamp       = "x-lineage-timestamp"
	HeaderTenantID        = "x-lineage-tenant-id" // Stub for future multi-tenancy
)

// LineageEventsTopic is the internal Kafka topic where SDK clients
// publish lineage events for ingestion.
const LineageEventsTopic = "_lineage.events"

// Default configuration values
const (
	DefaultPayloadPreviewMaxLength = 500
	DefaultChainDepthLimit         = 20
	DefaultQueryLimit              = 20
	DefaultQueryMaxLimit           = 100
	DefaultSinceHours              = 24
)

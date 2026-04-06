package envelope

import "time"

// EventType represents the type of lineage event.
type EventType string

const (
	// EventTypeProduced is emitted when a message is produced to a topic.
	EventTypeProduced EventType = "produced"
	// EventTypeConsumed is emitted when a message is consumed from a topic.
	EventTypeConsumed EventType = "consumed"
)

// MessageStatus represents the processing status of a message.
type MessageStatus string

const (
	StatusOK      MessageStatus = "ok"
	StatusFailed  MessageStatus = "failed"
	StatusPending MessageStatus = "pending"
)

// LineageEvent represents a single lineage event emitted by an SDK.
// This struct is serialized to JSON and published to the _lineage.events topic.
// Both the SDK and server depend on this definition.
type LineageEvent struct {
	// EventType indicates whether this is a produce or consume event.
	EventType EventType `json:"eventType"`

	// MessageID is the unique identifier for this message.
	MessageID string `json:"messageId"`

	// ParentID is the messageId of the message that caused this one.
	// Empty for root messages (chain originators).
	ParentID string `json:"parentId,omitempty"`

	// CorrelationID groups all messages in the same business transaction chain.
	CorrelationID string `json:"correlationId"`

	// RootMessageID is the messageId of the first message in the chain.
	RootMessageID string `json:"rootMessageId"`

	// CausationReason is a human-readable explanation of why this message was produced.
	// Examples: "OrderCreated", "PaymentFailed", "RetryAttempt"
	CausationReason string `json:"causationReason,omitempty"`

	// ServiceOrigin is the name of the service that produced/consumed this message.
	ServiceOrigin string `json:"serviceOrigin"`

	// Topic is the Kafka topic where the message was produced/consumed.
	Topic string `json:"topic"`

	// RetryCount is the number of retry attempts for this message.
	// 0 for first attempt, increments on each retry.
	RetryCount int `json:"retryCount"`

	// Timestamp is when the event occurred.
	Timestamp time.Time `json:"timestamp"`

	// Status indicates the processing outcome.
	Status MessageStatus `json:"status,omitempty"`

	// PayloadPreview is a truncated preview of the message payload (max 500 chars).
	// Useful for debugging without storing full payloads.
	PayloadPreview string `json:"payloadPreview,omitempty"`

	// TenantID is a stub field for future multi-tenancy support.
	TenantID string `json:"tenantId,omitempty"`

	// Partition is the Kafka partition number (for debugging).
	Partition int `json:"partition,omitempty"`

	// Offset is the Kafka offset (for debugging).
	Offset int64 `json:"offset,omitempty"`
}

// NewLineageEvent creates a new LineageEvent with required fields.
func NewLineageEvent(
	eventType EventType,
	messageID, correlationID, rootMessageID, serviceOrigin, topic string,
) *LineageEvent {
	return &LineageEvent{
		EventType:     eventType,
		MessageID:     messageID,
		CorrelationID: correlationID,
		RootMessageID: rootMessageID,
		ServiceOrigin: serviceOrigin,
		Topic:         topic,
		Timestamp:     time.Now().UTC(),
		Status:        StatusOK,
	}
}

// WithParent sets the parent message ID and causation reason.
func (e *LineageEvent) WithParent(parentID, reason string) *LineageEvent {
	e.ParentID = parentID
	e.CausationReason = reason
	return e
}

// WithRetry sets the retry count.
func (e *LineageEvent) WithRetry(count int) *LineageEvent {
	e.RetryCount = count
	return e
}

// WithStatus sets the message status.
func (e *LineageEvent) WithStatus(status MessageStatus) *LineageEvent {
	e.Status = status
	return e
}

// WithPayloadPreview sets a truncated payload preview.
func (e *LineageEvent) WithPayloadPreview(payload string) *LineageEvent {
	if len(payload) > DefaultPayloadPreviewMaxLength {
		e.PayloadPreview = payload[:DefaultPayloadPreviewMaxLength] + "..."
	} else {
		e.PayloadPreview = payload
	}
	return e
}

// IsRootMessage returns true if this is the first message in a chain.
func (e *LineageEvent) IsRootMessage() bool {
	return e.ParentID == "" || e.MessageID == e.RootMessageID
}

// IsRetry returns true if this message is a retry attempt.
func (e *LineageEvent) IsRetry() bool {
	return e.RetryCount > 0
}

// IsFailed returns true if this message has failed status.
func (e *LineageEvent) IsFailed() bool {
	return e.Status == StatusFailed
}

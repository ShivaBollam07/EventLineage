// Package models defines domain types used across the application.
package models

import (
	"time"

	"github.com/eventlineage/backend/pkg/envelope"
)

// Message represents a message node in the lineage graph.
type Message struct {
	// Core identifiers
	MessageID     string `json:"messageId"`
	ParentID      string `json:"parentId,omitempty"`
	CorrelationID string `json:"correlationId"`
	RootMessageID string `json:"rootMessageId"`

	// Context
	CausationReason string `json:"causationReason,omitempty"`
	ServiceOrigin   string `json:"serviceOrigin"`
	Topic           string `json:"topic"`

	// State
	RetryCount int                   `json:"retryCount"`
	Status     envelope.MessageStatus `json:"status"`
	Timestamp  time.Time             `json:"timestamp"`

	// Optional
	PayloadPreview string `json:"payloadPreview,omitempty"`
	TenantID       string `json:"tenantId,omitempty"`

	// Graph relationships (populated by queries)
	Children []string `json:"children,omitempty"`
	Parent   *Message `json:"parent,omitempty"`
}

// MessageWithRelations includes the message and its immediate relations.
type MessageWithRelations struct {
	Message  *Message   `json:"message"`
	Parent   *Message   `json:"parent,omitempty"`
	Children []*Message `json:"children,omitempty"`
}

// NewMessageFromEvent creates a Message from a LineageEvent.
func NewMessageFromEvent(event *envelope.LineageEvent) *Message {
	return &Message{
		MessageID:       event.MessageID,
		ParentID:        event.ParentID,
		CorrelationID:   event.CorrelationID,
		RootMessageID:   event.RootMessageID,
		CausationReason: event.CausationReason,
		ServiceOrigin:   event.ServiceOrigin,
		Topic:           event.Topic,
		RetryCount:      event.RetryCount,
		Status:          event.Status,
		Timestamp:       event.Timestamp,
		PayloadPreview:  event.PayloadPreview,
		TenantID:        event.TenantID,
	}
}

// IsRoot returns true if this is a root message.
func (m *Message) IsRoot() bool {
	return m.ParentID == "" || m.MessageID == m.RootMessageID
}

// IsRetry returns true if this is a retry attempt.
func (m *Message) IsRetry() bool {
	return m.RetryCount > 0
}

// IsFailed returns true if the message has failed status.
func (m *Message) IsFailed() bool {
	return m.Status == envelope.StatusFailed
}

// ServiceInfo represents aggregated information about a service.
type ServiceInfo struct {
	Name         string    `json:"name"`
	MessageCount int64     `json:"messageCount"`
	ChainCount   int64     `json:"chainCount"`
	LastSeen     time.Time `json:"lastSeen"`
	FailureRate  float64   `json:"failureRate"`
}

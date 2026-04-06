package models

import (
	"testing"
	"time"

	"github.com/eventlineage/backend/pkg/envelope"
)

func TestNewMessageFromEvent(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	event := &envelope.LineageEvent{
		EventType:       envelope.EventTypeProduced,
		MessageID:       "msg-1",
		ParentID:        "msg-0",
		CorrelationID:   "corr-1",
		RootMessageID:   "root-1",
		CausationReason: "OrderCreated",
		ServiceOrigin:   "order-service",
		Topic:           "orders",
		RetryCount:      2,
		Status:          envelope.StatusOK,
		Timestamp:       ts,
		PayloadPreview:  "preview",
		TenantID:        "tenant-1",
		Partition:       3,
		Offset:          42,
	}

	msg := NewMessageFromEvent(event)

	if msg.MessageID != "msg-1" {
		t.Errorf("MessageID = %q, want %q", msg.MessageID, "msg-1")
	}
	if msg.ParentID != "msg-0" {
		t.Errorf("ParentID = %q, want %q", msg.ParentID, "msg-0")
	}
	if msg.CorrelationID != "corr-1" {
		t.Errorf("CorrelationID = %q, want %q", msg.CorrelationID, "corr-1")
	}
	if msg.RootMessageID != "root-1" {
		t.Errorf("RootMessageID = %q, want %q", msg.RootMessageID, "root-1")
	}
	if msg.CausationReason != "OrderCreated" {
		t.Errorf("CausationReason = %q, want %q", msg.CausationReason, "OrderCreated")
	}
	if msg.ServiceOrigin != "order-service" {
		t.Errorf("ServiceOrigin = %q, want %q", msg.ServiceOrigin, "order-service")
	}
	if msg.Topic != "orders" {
		t.Errorf("Topic = %q, want %q", msg.Topic, "orders")
	}
	if msg.RetryCount != 2 {
		t.Errorf("RetryCount = %d, want 2", msg.RetryCount)
	}
	if msg.Status != envelope.StatusOK {
		t.Errorf("Status = %q, want %q", msg.Status, envelope.StatusOK)
	}
	if !msg.Timestamp.Equal(ts) {
		t.Errorf("Timestamp = %v, want %v", msg.Timestamp, ts)
	}
	if msg.PayloadPreview != "preview" {
		t.Errorf("PayloadPreview = %q, want %q", msg.PayloadPreview, "preview")
	}
	if msg.TenantID != "tenant-1" {
		t.Errorf("TenantID = %q, want %q", msg.TenantID, "tenant-1")
	}
}

func TestNewMessageFromEventMinimalFields(t *testing.T) {
	event := &envelope.LineageEvent{
		MessageID:     "msg-1",
		CorrelationID: "corr-1",
		ServiceOrigin: "svc",
		Topic:         "topic",
	}

	msg := NewMessageFromEvent(event)

	if msg.MessageID != "msg-1" {
		t.Errorf("MessageID = %q, want %q", msg.MessageID, "msg-1")
	}
	if msg.ParentID != "" {
		t.Errorf("ParentID = %q, want empty", msg.ParentID)
	}
	if msg.RootMessageID != "" {
		t.Errorf("RootMessageID = %q, want empty", msg.RootMessageID)
	}
	if msg.CausationReason != "" {
		t.Errorf("CausationReason = %q, want empty", msg.CausationReason)
	}
	if msg.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0", msg.RetryCount)
	}
	if msg.PayloadPreview != "" {
		t.Errorf("PayloadPreview = %q, want empty", msg.PayloadPreview)
	}
	if msg.TenantID != "" {
		t.Errorf("TenantID = %q, want empty", msg.TenantID)
	}
}

func TestMessageIsRoot(t *testing.T) {
	tests := []struct {
		name     string
		msg      *Message
		expected bool
	}{
		{
			name:     "no parent and empty root",
			msg:      &Message{MessageID: "msg-1"},
			expected: true,
		},
		{
			name:     "no parent with different root",
			msg:      &Message{MessageID: "msg-1", RootMessageID: "root-1"},
			expected: true,
		},
		{
			name:     "no parent with same root",
			msg:      &Message{MessageID: "msg-1", RootMessageID: "msg-1"},
			expected: true,
		},
		{
			name:     "has parent but messageId equals rootMessageId",
			msg:      &Message{MessageID: "msg-1", ParentID: "msg-0", RootMessageID: "msg-1"},
			expected: true,
		},
		{
			name:     "has parent and different root - not root",
			msg:      &Message{MessageID: "msg-2", ParentID: "msg-1", RootMessageID: "msg-1"},
			expected: false,
		},
		{
			name:     "has parent and empty root - not root",
			msg:      &Message{MessageID: "msg-2", ParentID: "msg-1"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.msg.IsRoot(); got != tt.expected {
				t.Errorf("IsRoot() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestMessageIsRetry(t *testing.T) {
	tests := []struct {
		name       string
		retryCount int
		expected   bool
	}{
		{"zero retries", 0, false},
		{"one retry", 1, true},
		{"multiple retries", 3, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Message{RetryCount: tt.retryCount}
			if got := msg.IsRetry(); got != tt.expected {
				t.Errorf("IsRetry() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestMessageIsFailed(t *testing.T) {
	tests := []struct {
		name     string
		status   envelope.MessageStatus
		expected bool
	}{
		{"ok status", envelope.StatusOK, false},
		{"failed status", envelope.StatusFailed, true},
		{"pending status", envelope.StatusPending, false},
		{"empty status", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &Message{Status: tt.status}
			if got := msg.IsFailed(); got != tt.expected {
				t.Errorf("IsFailed() = %v, want %v", got, tt.expected)
			}
		})
	}
}

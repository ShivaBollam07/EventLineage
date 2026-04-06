package envelope

import (
	"strings"
	"testing"
	"time"
)

func TestNewLineageEvent(t *testing.T) {
	before := time.Now().UTC()
	e := NewLineageEvent(EventTypeProduced, "msg-1", "corr-1", "root-1", "order-service", "orders")
	after := time.Now().UTC()

	if e.EventType != EventTypeProduced {
		t.Errorf("EventType = %q, want %q", e.EventType, EventTypeProduced)
	}
	if e.MessageID != "msg-1" {
		t.Errorf("MessageID = %q, want %q", e.MessageID, "msg-1")
	}
	if e.CorrelationID != "corr-1" {
		t.Errorf("CorrelationID = %q, want %q", e.CorrelationID, "corr-1")
	}
	if e.RootMessageID != "root-1" {
		t.Errorf("RootMessageID = %q, want %q", e.RootMessageID, "root-1")
	}
	if e.ServiceOrigin != "order-service" {
		t.Errorf("ServiceOrigin = %q, want %q", e.ServiceOrigin, "order-service")
	}
	if e.Topic != "orders" {
		t.Errorf("Topic = %q, want %q", e.Topic, "orders")
	}
	if e.Status != StatusOK {
		t.Errorf("Status = %q, want %q", e.Status, StatusOK)
	}
	if e.Timestamp.Before(before) || e.Timestamp.After(after) {
		t.Errorf("Timestamp %v not between %v and %v", e.Timestamp, before, after)
	}
	// Verify zero-value defaults
	if e.ParentID != "" {
		t.Errorf("ParentID = %q, want empty", e.ParentID)
	}
	if e.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0", e.RetryCount)
	}
	if e.PayloadPreview != "" {
		t.Errorf("PayloadPreview = %q, want empty", e.PayloadPreview)
	}
	if e.TenantID != "" {
		t.Errorf("TenantID = %q, want empty", e.TenantID)
	}
}

func TestNewLineageEventConsumed(t *testing.T) {
	e := NewLineageEvent(EventTypeConsumed, "msg-2", "corr-2", "root-2", "payment-service", "payments")

	if e.EventType != EventTypeConsumed {
		t.Errorf("EventType = %q, want %q", e.EventType, EventTypeConsumed)
	}
}

func TestWithParent(t *testing.T) {
	e := NewLineageEvent(EventTypeProduced, "msg-2", "corr-1", "root-1", "svc", "topic")
	result := e.WithParent("msg-1", "OrderCreated")

	if result != e {
		t.Error("WithParent should return the same pointer for chaining")
	}
	if e.ParentID != "msg-1" {
		t.Errorf("ParentID = %q, want %q", e.ParentID, "msg-1")
	}
	if e.CausationReason != "OrderCreated" {
		t.Errorf("CausationReason = %q, want %q", e.CausationReason, "OrderCreated")
	}
}

func TestWithRetry(t *testing.T) {
	e := NewLineageEvent(EventTypeProduced, "msg-1", "corr-1", "root-1", "svc", "topic")
	result := e.WithRetry(3)

	if result != e {
		t.Error("WithRetry should return the same pointer for chaining")
	}
	if e.RetryCount != 3 {
		t.Errorf("RetryCount = %d, want 3", e.RetryCount)
	}
}

func TestWithRetryZero(t *testing.T) {
	e := NewLineageEvent(EventTypeProduced, "msg-1", "corr-1", "root-1", "svc", "topic")
	e.WithRetry(0)

	if e.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0", e.RetryCount)
	}
}

func TestWithStatus(t *testing.T) {
	e := NewLineageEvent(EventTypeProduced, "msg-1", "corr-1", "root-1", "svc", "topic")

	e.WithStatus(StatusFailed)
	if e.Status != StatusFailed {
		t.Errorf("Status = %q, want %q", e.Status, StatusFailed)
	}

	e.WithStatus(StatusPending)
	if e.Status != StatusPending {
		t.Errorf("Status = %q, want %q", e.Status, StatusPending)
	}

	e.WithStatus(StatusOK)
	if e.Status != StatusOK {
		t.Errorf("Status = %q, want %q", e.Status, StatusOK)
	}
}

func TestWithPayloadPreviewShort(t *testing.T) {
	e := NewLineageEvent(EventTypeProduced, "msg-1", "corr-1", "root-1", "svc", "topic")
	e.WithPayloadPreview("short payload")

	if e.PayloadPreview != "short payload" {
		t.Errorf("PayloadPreview = %q, want %q", e.PayloadPreview, "short payload")
	}
}

func TestWithPayloadPreviewEmpty(t *testing.T) {
	e := NewLineageEvent(EventTypeProduced, "msg-1", "corr-1", "root-1", "svc", "topic")
	e.WithPayloadPreview("")

	if e.PayloadPreview != "" {
		t.Errorf("PayloadPreview = %q, want empty", e.PayloadPreview)
	}
}

func TestWithPayloadPreviewExactly500(t *testing.T) {
	e := NewLineageEvent(EventTypeProduced, "msg-1", "corr-1", "root-1", "svc", "topic")
	exact500 := strings.Repeat("a", 500)
	e.WithPayloadPreview(exact500)

	if e.PayloadPreview != exact500 {
		t.Errorf("PayloadPreview length = %d, want 500", len(e.PayloadPreview))
	}
	if strings.HasSuffix(e.PayloadPreview, "...") {
		t.Error("PayloadPreview at exactly 500 chars should not be truncated")
	}
}

func TestWithPayloadPreviewTruncation501(t *testing.T) {
	e := NewLineageEvent(EventTypeProduced, "msg-1", "corr-1", "root-1", "svc", "topic")
	long501 := strings.Repeat("b", 501)
	e.WithPayloadPreview(long501)

	// 500 chars + "..." = 503
	if len(e.PayloadPreview) != 503 {
		t.Errorf("PayloadPreview length = %d, want 503", len(e.PayloadPreview))
	}
	if !strings.HasSuffix(e.PayloadPreview, "...") {
		t.Error("PayloadPreview > 500 chars should end with '...'")
	}
	// Verify it kept the first 500 characters
	if e.PayloadPreview[:500] != strings.Repeat("b", 500) {
		t.Error("Truncated preview should keep first 500 characters")
	}
}

func TestWithPayloadPreviewTruncationLong(t *testing.T) {
	e := NewLineageEvent(EventTypeProduced, "msg-1", "corr-1", "root-1", "svc", "topic")
	longPayload := strings.Repeat("c", 1000)
	e.WithPayloadPreview(longPayload)

	if len(e.PayloadPreview) != 503 {
		t.Errorf("PayloadPreview length = %d, want 503", len(e.PayloadPreview))
	}
	if !strings.HasSuffix(e.PayloadPreview, "...") {
		t.Error("Long payload should be truncated with '...'")
	}
}

func TestIsRootMessage(t *testing.T) {
	tests := []struct {
		name     string
		event    *LineageEvent
		expected bool
	}{
		{
			name:     "no parent and empty root",
			event:    &LineageEvent{MessageID: "msg-1"},
			expected: true,
		},
		{
			name:     "no parent with different root",
			event:    &LineageEvent{MessageID: "msg-1", RootMessageID: "root-1"},
			expected: true,
		},
		{
			name:     "no parent with same root",
			event:    &LineageEvent{MessageID: "msg-1", RootMessageID: "msg-1"},
			expected: true,
		},
		{
			name:     "has parent but messageId equals rootMessageId",
			event:    &LineageEvent{MessageID: "msg-1", ParentID: "msg-0", RootMessageID: "msg-1"},
			expected: true,
		},
		{
			name:     "has parent and different root",
			event:    &LineageEvent{MessageID: "msg-2", ParentID: "msg-1", RootMessageID: "msg-1"},
			expected: false,
		},
		{
			name:     "has parent and empty root",
			event:    &LineageEvent{MessageID: "msg-2", ParentID: "msg-1", RootMessageID: ""},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.event.IsRootMessage(); got != tt.expected {
				t.Errorf("IsRootMessage() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestEventIsRetry(t *testing.T) {
	tests := []struct {
		name       string
		retryCount int
		expected   bool
	}{
		{"zero retries", 0, false},
		{"one retry", 1, true},
		{"multiple retries", 5, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &LineageEvent{RetryCount: tt.retryCount}
			if got := e.IsRetry(); got != tt.expected {
				t.Errorf("IsRetry() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestEventIsFailed(t *testing.T) {
	tests := []struct {
		name     string
		status   MessageStatus
		expected bool
	}{
		{"ok status", StatusOK, false},
		{"failed status", StatusFailed, true},
		{"pending status", StatusPending, false},
		{"empty status", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &LineageEvent{Status: tt.status}
			if got := e.IsFailed(); got != tt.expected {
				t.Errorf("IsFailed() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestBuilderChaining(t *testing.T) {
	e := NewLineageEvent(EventTypeConsumed, "msg-1", "corr-1", "root-1", "svc", "topic").
		WithParent("parent-1", "SomeReason").
		WithRetry(2).
		WithStatus(StatusFailed).
		WithPayloadPreview("preview data")

	if e.EventType != EventTypeConsumed {
		t.Errorf("EventType = %q, want %q", e.EventType, EventTypeConsumed)
	}
	if e.ParentID != "parent-1" {
		t.Errorf("ParentID = %q, want %q", e.ParentID, "parent-1")
	}
	if e.CausationReason != "SomeReason" {
		t.Errorf("CausationReason = %q, want %q", e.CausationReason, "SomeReason")
	}
	if e.RetryCount != 2 {
		t.Errorf("RetryCount = %d, want 2", e.RetryCount)
	}
	if e.Status != StatusFailed {
		t.Errorf("Status = %q, want %q", e.Status, StatusFailed)
	}
	if e.PayloadPreview != "preview data" {
		t.Errorf("PayloadPreview = %q, want %q", e.PayloadPreview, "preview data")
	}
}

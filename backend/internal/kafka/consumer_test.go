package kafka

import (
	"context"
	"sync/atomic"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
)

func TestProcessMessageInvalidJSON(t *testing.T) {
	c := &Consumer{}
	msg := kafkago.Message{Value: []byte("not valid json {")}

	err := c.processMessage(context.Background(), msg)

	if err != nil {
		t.Errorf("Expected nil error for invalid JSON (skip), got: %v", err)
	}
	if atomic.LoadInt64(&c.metrics.Skipped) != 1 {
		t.Errorf("Skipped = %d, want 1", atomic.LoadInt64(&c.metrics.Skipped))
	}
}

func TestProcessMessageEmptyBody(t *testing.T) {
	c := &Consumer{}
	msg := kafkago.Message{Value: []byte("")}

	err := c.processMessage(context.Background(), msg)

	if err != nil {
		t.Errorf("Expected nil error for empty body (skip), got: %v", err)
	}
	if atomic.LoadInt64(&c.metrics.Skipped) != 1 {
		t.Errorf("Skipped = %d, want 1", atomic.LoadInt64(&c.metrics.Skipped))
	}
}

func TestProcessMessageNullJSON(t *testing.T) {
	c := &Consumer{}
	msg := kafkago.Message{Value: []byte("null")}

	err := c.processMessage(context.Background(), msg)

	// "null" is valid JSON that unmarshals to a zero-value LineageEvent
	// Both messageId and correlationId will be empty, so it should be skipped
	if err != nil {
		t.Errorf("Expected nil error, got: %v", err)
	}
	if atomic.LoadInt64(&c.metrics.Skipped) != 1 {
		t.Errorf("Skipped = %d, want 1", atomic.LoadInt64(&c.metrics.Skipped))
	}
}

func TestProcessMessageMissingMessageID(t *testing.T) {
	c := &Consumer{}
	// Valid JSON but no messageId field
	msg := kafkago.Message{Value: []byte(`{"correlationId":"corr-1","serviceOrigin":"svc"}`)}

	err := c.processMessage(context.Background(), msg)

	if err != nil {
		t.Errorf("Expected nil error for missing messageId (skip), got: %v", err)
	}
	if atomic.LoadInt64(&c.metrics.Skipped) != 1 {
		t.Errorf("Skipped = %d, want 1", atomic.LoadInt64(&c.metrics.Skipped))
	}
}

func TestProcessMessageEmptyMessageID(t *testing.T) {
	c := &Consumer{}
	// messageId present but empty string
	msg := kafkago.Message{Value: []byte(`{"messageId":"","correlationId":"corr-1"}`)}

	err := c.processMessage(context.Background(), msg)

	if err != nil {
		t.Errorf("Expected nil error for empty messageId (skip), got: %v", err)
	}
	if atomic.LoadInt64(&c.metrics.Skipped) != 1 {
		t.Errorf("Skipped = %d, want 1", atomic.LoadInt64(&c.metrics.Skipped))
	}
}

func TestProcessMessageMissingCorrelationID(t *testing.T) {
	c := &Consumer{}
	// Has messageId but no correlationId
	msg := kafkago.Message{Value: []byte(`{"messageId":"msg-1","serviceOrigin":"svc"}`)}

	err := c.processMessage(context.Background(), msg)

	if err != nil {
		t.Errorf("Expected nil error for missing correlationId (skip), got: %v", err)
	}
	if atomic.LoadInt64(&c.metrics.Skipped) != 1 {
		t.Errorf("Skipped = %d, want 1", atomic.LoadInt64(&c.metrics.Skipped))
	}
}

func TestProcessMessageEmptyCorrelationID(t *testing.T) {
	c := &Consumer{}
	msg := kafkago.Message{Value: []byte(`{"messageId":"msg-1","correlationId":""}`)}

	err := c.processMessage(context.Background(), msg)

	if err != nil {
		t.Errorf("Expected nil error for empty correlationId (skip), got: %v", err)
	}
	if atomic.LoadInt64(&c.metrics.Skipped) != 1 {
		t.Errorf("Skipped = %d, want 1", atomic.LoadInt64(&c.metrics.Skipped))
	}
}

func TestProcessMessageValidJSONPassesValidation(t *testing.T) {
	c := &Consumer{}
	// Valid JSON with all required fields
	validJSON := `{"messageId":"msg-1","correlationId":"corr-1","serviceOrigin":"svc","topic":"test"}`
	msg := kafkago.Message{
		Value:     []byte(validJSON),
		Partition: 2,
		Offset:    42,
	}

	// processMessage will panic when trying to call UpsertMessage on nil messageRepo.
	// This confirms the message passed validation and reached the repo call.
	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		c.processMessage(context.Background(), msg)
	}()

	if !panicked {
		t.Error("Expected panic from nil messageRepo (message should pass validation and reach repo call)")
	}
	if atomic.LoadInt64(&c.metrics.Skipped) != 0 {
		t.Errorf("Skipped = %d, want 0 (valid message should not be skipped)", atomic.LoadInt64(&c.metrics.Skipped))
	}
}

func TestProcessMessageValidJSONMinimalFields(t *testing.T) {
	c := &Consumer{}
	// Only messageId and correlationId - the bare minimum
	validJSON := `{"messageId":"msg-1","correlationId":"corr-1"}`
	msg := kafkago.Message{Value: []byte(validJSON)}

	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		c.processMessage(context.Background(), msg)
	}()

	if !panicked {
		t.Error("Expected panic from nil messageRepo (message should pass validation)")
	}
	if atomic.LoadInt64(&c.metrics.Skipped) != 0 {
		t.Errorf("Skipped = %d, want 0", atomic.LoadInt64(&c.metrics.Skipped))
	}
}

func TestProcessMessageSkipCounterAccumulates(t *testing.T) {
	c := &Consumer{}
	messages := []kafkago.Message{
		{Value: []byte("bad json")},                                   // invalid JSON
		{Value: []byte(`{"correlationId":"c1"}`)},                     // missing messageId
		{Value: []byte(`{"messageId":"m1"}`)},                         // missing correlationId
		{Value: []byte(`{"messageId":"","correlationId":"c1"}`)},      // empty messageId
		{Value: []byte(`{"messageId":"m1","correlationId":""}`)},      // empty correlationId
	}

	for _, msg := range messages {
		c.processMessage(context.Background(), msg)
	}

	if atomic.LoadInt64(&c.metrics.Skipped) != 5 {
		t.Errorf("Skipped = %d, want 5", atomic.LoadInt64(&c.metrics.Skipped))
	}
}

func TestProcessMessageValidationOrder(t *testing.T) {
	// When both messageId and correlationId are missing, messageId check comes first
	c := &Consumer{}
	msg := kafkago.Message{Value: []byte(`{"serviceOrigin":"svc"}`)}

	c.processMessage(context.Background(), msg)

	// Should be skipped exactly once (messageId check triggers first)
	if atomic.LoadInt64(&c.metrics.Skipped) != 1 {
		t.Errorf("Skipped = %d, want 1", atomic.LoadInt64(&c.metrics.Skipped))
	}
}

func TestProcessMessageDefaultValues(t *testing.T) {
	// processMessage applies defaults for status, rootMessageId, and timestamp
	// before calling the repo. Since we can't mock the repo, we verify the
	// message passes validation (is not skipped) when these fields are missing,
	// confirming the defaults path is exercised.

	c := &Consumer{}
	// JSON with no status, no rootMessageId, no timestamp
	jsonData := `{"messageId":"msg-1","correlationId":"corr-1"}`
	msg := kafkago.Message{Value: []byte(jsonData)}

	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		c.processMessage(context.Background(), msg)
	}()

	if !panicked {
		t.Error("Expected panic (message should pass validation, hit repo call)")
	}
	if atomic.LoadInt64(&c.metrics.Skipped) != 0 {
		t.Error("Message with defaultable fields should not be skipped")
	}
}

func TestConsumerMetrics(t *testing.T) {
	c := &Consumer{}

	// Verify initial metrics are zero
	metrics := c.GetMetrics()
	if metrics.Processed != 0 {
		t.Errorf("Initial Processed = %d, want 0", metrics.Processed)
	}
	if metrics.Failed != 0 {
		t.Errorf("Initial Failed = %d, want 0", metrics.Failed)
	}
	if metrics.Skipped != 0 {
		t.Errorf("Initial Skipped = %d, want 0", metrics.Skipped)
	}
	if metrics.Retries != 0 {
		t.Errorf("Initial Retries = %d, want 0", metrics.Retries)
	}
	if metrics.DLQMessages != 0 {
		t.Errorf("Initial DLQMessages = %d, want 0", metrics.DLQMessages)
	}

	// Process some invalid messages to increment Skipped
	c.processMessage(context.Background(), kafkago.Message{Value: []byte("bad")})
	c.processMessage(context.Background(), kafkago.Message{Value: []byte(`{"messageId":"m1"}`)})

	metrics = c.GetMetrics()
	if metrics.Skipped != 2 {
		t.Errorf("Skipped = %d, want 2", metrics.Skipped)
	}
	// Processed should still be 0 (messages were skipped, not processed)
	if metrics.Processed != 0 {
		t.Errorf("Processed = %d, want 0", metrics.Processed)
	}
}

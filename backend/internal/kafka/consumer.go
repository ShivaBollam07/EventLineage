// Package kafka handles Kafka ingestion for lineage events.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/eventlineage/backend/internal/store"
	"github.com/eventlineage/backend/pkg/envelope"
	"github.com/segmentio/kafka-go"
)

// ConsumerMetrics tracks consumer processing statistics.
type ConsumerMetrics struct {
	Processed   int64 `json:"processed"`
	Failed      int64 `json:"failed"`
	Skipped     int64 `json:"skipped"`
	Retries     int64 `json:"retries"`
	DLQMessages int64 `json:"dlqMessages"`
}

// Consumer reads lineage events from Kafka and writes to Neo4j.
// Uses Kafka as the retry mechanism: offsets are only committed after
// successful Neo4j writes. This ensures no data loss on restart.
type Consumer struct {
	reader      *kafka.Reader
	dlqWriter   *kafka.Writer
	messageRepo *store.MessageRepository
	maxRetries  int
	retryDelay  time.Duration
	metrics     ConsumerMetrics
}

// NewConsumer creates a new Kafka consumer for lineage events.
// brokers is a comma-separated list of broker addresses.
func NewConsumer(brokers, groupID string, messageRepo *store.MessageRepository) (*Consumer, error) {
	brokerList := strings.Split(brokers, ",")
	for i, b := range brokerList {
		brokerList[i] = strings.TrimSpace(b)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokerList,
		GroupID:     groupID,
		Topic:       envelope.LineageEventsTopic,
		MinBytes:    1e3,  // 1KB
		MaxBytes:    10e6, // 10MB
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
		// IMPORTANT: Manual commit - don't commit until Neo4j write succeeds
		CommitInterval: 0, // Disable auto-commit
	})

	// DLQ writer for messages that fail all retries
	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokerList...),
		Topic:        envelope.LineageEventsTopic + ".dlq",
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
	}

	return &Consumer{
		reader:      reader,
		dlqWriter:   dlqWriter,
		messageRepo: messageRepo,
		maxRetries:  3,
		retryDelay:  time.Second,
	}, nil
}

// GetMetrics returns current consumer metrics.
func (c *Consumer) GetMetrics() ConsumerMetrics {
	return ConsumerMetrics{
		Processed:   atomic.LoadInt64(&c.metrics.Processed),
		Failed:      atomic.LoadInt64(&c.metrics.Failed),
		Skipped:     atomic.LoadInt64(&c.metrics.Skipped),
		Retries:     atomic.LoadInt64(&c.metrics.Retries),
		DLQMessages: atomic.LoadInt64(&c.metrics.DLQMessages),
	}
}

// Start begins consuming lineage events.
// Offset is only committed after successful Neo4j write.
// On Neo4j failure, retries with backoff before moving to next message.
// If all retries fail, message is sent to DLQ and offset is committed.
func (c *Consumer) Start(ctx context.Context) error {
	log.Printf("Starting Kafka consumer for topic: %s", envelope.LineageEventsTopic)
	log.Printf("Offset commit strategy: manual (commit only after successful Neo4j write)")
	log.Printf("Dead letter queue: %s.dlq", envelope.LineageEventsTopic)

	for {
		select {
		case <-ctx.Done():
			log.Println("Kafka consumer shutting down...")
			c.dlqWriter.Close()
			return c.reader.Close()
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				log.Printf("Error fetching message: %v", err)
				continue
			}

			// Process with retries
			success := c.processWithRetries(ctx, msg)

			if success {
				atomic.AddInt64(&c.metrics.Processed, 1)
				// Only commit offset after successful Neo4j write
				if err := c.reader.CommitMessages(ctx, msg); err != nil {
					log.Printf("Error committing offset: %v", err)
				}
			} else {
				// Send to dead letter queue
				atomic.AddInt64(&c.metrics.Failed, 1)
				c.sendToDLQ(ctx, msg)

				// Commit to avoid infinite loop
				if err := c.reader.CommitMessages(ctx, msg); err != nil {
					log.Printf("Error committing offset: %v", err)
				}
			}
		}
	}
}

// sendToDLQ sends a failed message to the dead letter queue.
func (c *Consumer) sendToDLQ(ctx context.Context, msg kafka.Message) {
	dlqMsg := kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: append(msg.Headers,
			kafka.Header{Key: "x-dlq-reason", Value: []byte("max retries exhausted")},
			kafka.Header{Key: "x-dlq-timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
			kafka.Header{Key: "x-original-topic", Value: []byte(envelope.LineageEventsTopic)},
		),
	}

	if err := c.dlqWriter.WriteMessages(ctx, dlqMsg); err != nil {
		log.Printf("CRITICAL: Failed to write to DLQ: %v", err)
	} else {
		atomic.AddInt64(&c.metrics.DLQMessages, 1)
		log.Printf("Message sent to DLQ after %d retries", c.maxRetries)
	}
}

// processWithRetries attempts to process a message with exponential backoff.
func (c *Consumer) processWithRetries(ctx context.Context, msg kafka.Message) bool {
	var lastErr error
	delay := c.retryDelay

	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		err := c.processMessage(ctx, msg)
		if err == nil {
			return true
		}

		lastErr = err
		atomic.AddInt64(&c.metrics.Retries, 1)
		log.Printf("Attempt %d/%d failed: %v", attempt, c.maxRetries, err)

		if attempt < c.maxRetries {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(delay):
				delay *= 2 // Exponential backoff
				if delay > 16*time.Second {
					delay = 16 * time.Second
				}
			}
		}
	}

	log.Printf("All %d retries exhausted. Last error: %v", c.maxRetries, lastErr)
	return false
}

// processMessage parses and stores a lineage event.
func (c *Consumer) processMessage(ctx context.Context, msg kafka.Message) error {
	var event envelope.LineageEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		// Invalid JSON - no point retrying, just log and skip
		atomic.AddInt64(&c.metrics.Skipped, 1)
		log.Printf("Invalid JSON (skipping): %v", err)
		return nil // Return nil to avoid retries for malformed data
	}

	// Validate required fields
	if event.MessageID == "" {
		atomic.AddInt64(&c.metrics.Skipped, 1)
		log.Printf("Missing messageId (skipping)")
		return nil // Skip invalid events
	}
	if event.CorrelationID == "" {
		atomic.AddInt64(&c.metrics.Skipped, 1)
		log.Printf("Missing correlationId (skipping)")
		return nil
	}

	// Set defaults
	if event.Status == "" {
		event.Status = envelope.StatusOK
	}
	if event.RootMessageID == "" {
		event.RootMessageID = event.MessageID
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	// Store Kafka metadata
	event.Partition = int(msg.Partition)
	event.Offset = msg.Offset

	// Write to Neo4j - this is the operation that can fail and needs retry
	if err := c.messageRepo.UpsertMessage(ctx, &event); err != nil {
		return fmt.Errorf("neo4j write failed: %w", err)
	}

	log.Printf("Processed lineage event: %s (correlation: %s)", event.MessageID, event.CorrelationID)
	return nil
}

// Close closes the Kafka consumer and DLQ writer.
func (c *Consumer) Close() error {
	c.dlqWriter.Close()
	return c.reader.Close()
}

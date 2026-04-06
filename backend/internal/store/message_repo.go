package store

import (
	"context"
	"fmt"
	"time"

	"github.com/eventlineage/backend/internal/models"
	"github.com/eventlineage/backend/pkg/envelope"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// MessageRepository handles message node operations in Neo4j.
type MessageRepository struct {
	store *Neo4jStore
}

// NewMessageRepository creates a new message repository.
func NewMessageRepository(store *Neo4jStore) *MessageRepository {
	return &MessageRepository{store: store}
}

// UpsertMessage creates or updates a message node and its causal edge.
// Uses MERGE to handle out-of-order event arrival (child before parent).
func (r *MessageRepository) UpsertMessage(ctx context.Context, event *envelope.LineageEvent) error {
	session := r.store.Session(ctx)
	defer session.Close(ctx)

	// Use MERGE for idempotent, order-independent writes
	query := `
		MERGE (m:Message {messageId: $messageId})
		ON CREATE SET
			m.correlationId = $correlationId,
			m.rootMessageId = $rootMessageId,
			m.parentId = $parentId,
			m.causationReason = $causationReason,
			m.serviceOrigin = $serviceOrigin,
			m.topic = $topic,
			m.retryCount = $retryCount,
			m.status = $status,
			m.timestamp = datetime($timestamp),
			m.payloadPreview = $payloadPreview,
			m.tenantId = $tenantId
		ON MATCH SET
			m.correlationId = COALESCE(m.correlationId, $correlationId),
			m.rootMessageId = COALESCE(m.rootMessageId, $rootMessageId),
			m.parentId = COALESCE(m.parentId, $parentId),
			m.causationReason = COALESCE(m.causationReason, $causationReason),
			m.serviceOrigin = COALESCE(m.serviceOrigin, $serviceOrigin),
			m.topic = COALESCE(m.topic, $topic),
			m.retryCount = COALESCE(m.retryCount, $retryCount),
			m.status = COALESCE(m.status, $status),
			m.timestamp = COALESCE(m.timestamp, datetime($timestamp)),
			m.payloadPreview = COALESCE(m.payloadPreview, $payloadPreview),
			m.tenantId = COALESCE(m.tenantId, $tenantId)
	`

	params := map[string]any{
		"messageId":       event.MessageID,
		"correlationId":   event.CorrelationID,
		"rootMessageId":   event.RootMessageID,
		"parentId":        event.ParentID,
		"causationReason": event.CausationReason,
		"serviceOrigin":   event.ServiceOrigin,
		"topic":           event.Topic,
		"retryCount":      event.RetryCount,
		"status":          string(event.Status),
		"timestamp":       event.Timestamp.Format(time.RFC3339),
		"payloadPreview":  event.PayloadPreview,
		"tenantId":        event.TenantID,
	}

	_, err := session.Run(ctx, query, params)
	if err != nil {
		return fmt.Errorf("failed to upsert message: %w", err)
	}

	// Create causal edge if parent exists
	if event.ParentID != "" {
		edgeQuery := `
			MERGE (parent:Message {messageId: $parentId})
			MERGE (child:Message {messageId: $childId})
			MERGE (parent)-[r:CAUSED]->(child)
			ON CREATE SET r.reason = $reason, r.createdAt = datetime()
		`
		edgeParams := map[string]any{
			"parentId": event.ParentID,
			"childId":  event.MessageID,
			"reason":   event.CausationReason,
		}

		_, err := session.Run(ctx, edgeQuery, edgeParams)
		if err != nil {
			return fmt.Errorf("failed to create causal edge: %w", err)
		}
	}

	return nil
}

// GetMessage retrieves a single message by ID.
func (r *MessageRepository) GetMessage(ctx context.Context, messageID string) (*models.Message, error) {
	session := r.store.ReadSession(ctx)
	defer session.Close(ctx)

	query := `
		MATCH (m:Message {messageId: $messageId})
		RETURN m
	`

	result, err := session.Run(ctx, query, map[string]any{"messageId": messageID})
	if err != nil {
		return nil, fmt.Errorf("failed to get message: %w", err)
	}

	record, err := result.Single(ctx)
	if err != nil {
		return nil, fmt.Errorf("message not found: %w", err)
	}

	return r.nodeToMessage(record.Values[0].(neo4j.Node)), nil
}

// GetMessageWithRelations retrieves a message with its parent and children.
func (r *MessageRepository) GetMessageWithRelations(ctx context.Context, messageID string) (*models.MessageWithRelations, error) {
	session := r.store.ReadSession(ctx)
	defer session.Close(ctx)

	query := `
		MATCH (m:Message {messageId: $messageId})
		OPTIONAL MATCH (parent:Message)-[:CAUSED]->(m)
		OPTIONAL MATCH (m)-[:CAUSED]->(child:Message)
		RETURN m, parent, collect(child) AS children
	`

	result, err := session.Run(ctx, query, map[string]any{"messageId": messageID})
	if err != nil {
		return nil, fmt.Errorf("failed to get message with relations: %w", err)
	}

	record, err := result.Single(ctx)
	if err != nil {
		return nil, fmt.Errorf("message not found: %w", err)
	}

	msg := r.nodeToMessage(record.Values[0].(neo4j.Node))

	var parent *models.Message
	if record.Values[1] != nil {
		parent = r.nodeToMessage(record.Values[1].(neo4j.Node))
	}

	var children []*models.Message
	if childNodes, ok := record.Values[2].([]any); ok {
		for _, cn := range childNodes {
			if node, ok := cn.(neo4j.Node); ok {
				children = append(children, r.nodeToMessage(node))
			}
		}
	}

	return &models.MessageWithRelations{
		Message:  msg,
		Parent:   parent,
		Children: children,
	}, nil
}

// GetMessagesByCorrelationID retrieves all messages in a chain.
func (r *MessageRepository) GetMessagesByCorrelationID(ctx context.Context, correlationID string) ([]*models.Message, error) {
	session := r.store.ReadSession(ctx)
	defer session.Close(ctx)

	query := `
		MATCH (m:Message {correlationId: $correlationId})
		RETURN m
		ORDER BY m.timestamp ASC
	`

	result, err := session.Run(ctx, query, map[string]any{"correlationId": correlationID})
	if err != nil {
		return nil, fmt.Errorf("failed to get messages: %w", err)
	}

	records, err := result.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to collect results: %w", err)
	}

	messages := make([]*models.Message, 0, len(records))
	for _, record := range records {
		messages = append(messages, r.nodeToMessage(record.Values[0].(neo4j.Node)))
	}

	return messages, nil
}

// nodeToMessage converts a Neo4j node to a Message model.
func (r *MessageRepository) nodeToMessage(node neo4j.Node) *models.Message {
	props := node.Props

	msg := &models.Message{
		MessageID:       getString(props, "messageId"),
		ParentID:        getString(props, "parentId"),
		CorrelationID:   getString(props, "correlationId"),
		RootMessageID:   getString(props, "rootMessageId"),
		CausationReason: getString(props, "causationReason"),
		ServiceOrigin:   getString(props, "serviceOrigin"),
		Topic:           getString(props, "topic"),
		RetryCount:      getInt(props, "retryCount"),
		Status:          envelope.MessageStatus(getString(props, "status")),
		PayloadPreview:  getString(props, "payloadPreview"),
		TenantID:        getString(props, "tenantId"),
	}

	if ts, ok := props["timestamp"].(time.Time); ok {
		msg.Timestamp = ts
	} else if ts, ok := props["timestamp"].(neo4j.LocalDateTime); ok {
		msg.Timestamp = ts.Time()
	}

	return msg
}

// Helper functions for safe type assertion
func getString(props map[string]any, key string) string {
	if v, ok := props[key].(string); ok {
		return v
	}
	return ""
}

func getInt(props map[string]any, key string) int {
	if v, ok := props[key].(int64); ok {
		return int(v)
	}
	if v, ok := props[key].(int); ok {
		return v
	}
	return 0
}

package store

import (
	"context"
	"fmt"
	"time"

	"github.com/eventlineage/backend/internal/models"
	"github.com/eventlineage/backend/pkg/envelope"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// ChainRepository handles chain-level operations in Neo4j.
type ChainRepository struct {
	store *Neo4jStore
}

// NewChainRepository creates a new chain repository.
func NewChainRepository(store *Neo4jStore) *ChainRepository {
	return &ChainRepository{store: store}
}

// GetChain retrieves a complete chain by correlation ID.
func (r *ChainRepository) GetChain(ctx context.Context, correlationID string) (*models.Chain, error) {
	session := r.store.ReadSession(ctx)
	defer session.Close(ctx)

	// Query 1: Get all nodes in the chain
	// Find root (no incoming CAUSED edge) and traverse down
	// IMPORTANT: Depth limit of 20 to prevent runaway queries
	nodesQuery := `
		MATCH (root:Message {correlationId: $correlationId})
		WHERE NOT ()-[:CAUSED]->(root)
		MATCH path = (root)-[:CAUSED*0..20]->(node)
		WITH collect(DISTINCT root) + collect(DISTINCT node) AS allNodes
		UNWIND allNodes AS n
		RETURN DISTINCT n
		ORDER BY n.timestamp ASC
	`

	nodesResult, err := session.Run(ctx, nodesQuery, map[string]any{"correlationId": correlationID})
	if err != nil {
		return nil, fmt.Errorf("failed to get chain nodes: %w", err)
	}

	nodesRecords, err := nodesResult.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to collect chain nodes: %w", err)
	}

	if len(nodesRecords) == 0 {
		return nil, fmt.Errorf("chain not found: %s", correlationID)
	}

	// Build messages from nodes
	messages := make([]*models.Message, 0, len(nodesRecords))
	for _, record := range nodesRecords {
		node := record.Values[0].(neo4j.Node)
		messages = append(messages, r.nodeToMessage(node))
	}

	// Query 2: Get all edges separately for React Flow renderer
	edgesQuery := `
		MATCH (a:Message {correlationId: $correlationId})-[r:CAUSED]->(b:Message)
		RETURN a.messageId AS from, b.messageId AS to, r.reason AS reason
	`

	edgesResult, err := session.Run(ctx, edgesQuery, map[string]any{"correlationId": correlationID})
	if err != nil {
		return nil, fmt.Errorf("failed to get chain edges: %w", err)
	}

	edgesRecords, err := edgesResult.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to collect chain edges: %w", err)
	}

	// Build edges
	edges := make([]models.ChainEdge, 0, len(edgesRecords))
	for _, record := range edgesRecords {
		edges = append(edges, models.ChainEdge{
			From:   getString(record.AsMap(), "from"),
			To:     getString(record.AsMap(), "to"),
			Reason: getString(record.AsMap(), "reason"),
		})
	}

	// Build chain from messages
	chain := models.NewChainFromMessages(correlationID, messages)
	chain.Edges = edges

	return chain, nil
}

// ListChains retrieves chain summaries with optional filters.
// Fetches limit+1 results to determine if there are more pages.
func (r *ChainRepository) ListChains(ctx context.Context, params *models.ChainQueryParams) ([]models.ChainSummary, error) {
	session := r.store.ReadSession(ctx)
	defer session.Close(ctx)

	params.SetDefaults()

	// Step 1: Filter messages by time and service
	query := `
		MATCH (m:Message)
		WHERE m.timestamp >= datetime($since) AND m.timestamp <= datetime($until)
	`

	queryParams := map[string]any{
		"since":  params.Since.Format(time.RFC3339),
		"until":  params.Until.Format(time.RFC3339),
		"limit":  params.Limit + 1,
		"offset": params.Offset,
		"status": params.Status, // Added here for later use
	}

	if params.Service != "" {
		query += ` AND m.serviceOrigin = $service`
		queryParams["service"] = params.Service
	}

	// Step 2: Group by correlationId and calculate chain-level stats
	query += `
		WITH m.correlationId AS correlationId,
			 m.rootMessageId AS rootMessageId,
			 collect(m) AS messages
		WITH correlationId, rootMessageId, messages,
			 size(messages) AS messageCount,
			 [msg IN messages WHERE msg.retryCount > 0 | msg] AS retries,
			 [msg IN messages WHERE msg.status = 'failed' | msg] AS failures,
			 min([msg IN messages | msg.timestamp]) AS startTime,
			 max([msg IN messages | msg.timestamp]) AS endTime
		MATCH (root:Message {messageId: rootMessageId})
		WITH correlationId, rootMessageId, root.serviceOrigin AS rootService,
			   messageCount, size(retries) AS retryCount, size(failures) AS failureCount,
			   startTime, endTime,
			   CASE WHEN size(failures) > 0 THEN 'failed'
					ELSE 'completed' END AS status
		WHERE $status = '' OR status = $status
		RETURN correlationId, rootMessageId, rootService,
			   messageCount, retryCount, failureCount,
			   startTime, endTime, status
		ORDER BY startTime DESC
		SKIP $offset
		LIMIT $limit
	`


	result, err := session.Run(ctx, query, queryParams)
	if err != nil {
		return nil, fmt.Errorf("failed to list chains: %w", err)
	}

	records, err := result.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to collect chain list: %w", err)
	}

	summaries := make([]models.ChainSummary, 0, len(records))
	for _, record := range records {
		summary := models.ChainSummary{
			CorrelationID: getString(record.AsMap(), "correlationId"),
			RootMessageID: getString(record.AsMap(), "rootMessageId"),
			RootService:   getString(record.AsMap(), "rootService"),
			Status:        models.ChainStatus(getString(record.AsMap(), "status")),
			MessageCount:  getInt(record.AsMap(), "messageCount"),
			RetryCount:    getInt(record.AsMap(), "retryCount"),
			FailureCount:  getInt(record.AsMap(), "failureCount"),
		}

		if st, ok := record.AsMap()["startTime"].(time.Time); ok {
			summary.StartTime = st
		}
		if et, ok := record.AsMap()["endTime"].(time.Time); ok {
			summary.EndTime = et
			summary.Duration = float64(et.Sub(summary.StartTime).Milliseconds())
		}

		summaries = append(summaries, summary)
	}

	return summaries, nil
}

// nodeToMessage converts a Neo4j node to a Message model.
func (r *ChainRepository) nodeToMessage(node neo4j.Node) *models.Message {
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

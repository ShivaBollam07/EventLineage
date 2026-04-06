package models

import (
	"time"

	"github.com/eventlineage/backend/pkg/envelope"
)

// ChainStatus represents the overall status of a message chain.
type ChainStatus string

const (
	ChainStatusCompleted ChainStatus = "completed"
	ChainStatusInFlight  ChainStatus = "in_flight"
	ChainStatusFailed    ChainStatus = "failed"
)

// Chain represents a complete message lineage chain.
type Chain struct {
	// Identifiers
	CorrelationID string `json:"correlationId"`
	RootMessageID string `json:"rootMessageId"`

	// Status
	Status       ChainStatus `json:"status"`
	MessageCount int         `json:"messageCount"`

	// Time range
	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime,omitempty"`

	// Graph data for visualization
	Nodes []ChainNode `json:"nodes"`
	Edges []ChainEdge `json:"edges"`

	// Metrics
	RetryCount   int     `json:"retryCount"`
	FailureCount int     `json:"failureCount"`
	DepthMax     int     `json:"depthMax"`
	Duration     float64 `json:"durationMs,omitempty"`
}

// ChainNode represents a node in the chain visualization.
type ChainNode struct {
	MessageID       string                `json:"messageId"`
	Service         string                `json:"service"`
	Topic           string                `json:"topic"`
	Status          envelope.MessageStatus `json:"status"`
	RetryCount      int                   `json:"retryCount"`
	Timestamp       time.Time             `json:"timestamp"`
	CausationReason string                `json:"causationReason,omitempty"`
	PayloadPreview  string                `json:"payloadPreview,omitempty"`
	Depth           int                   `json:"depth"`
}

// ChainEdge represents a causal relationship between two messages.
type ChainEdge struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Reason string `json:"reason,omitempty"`
}

// ChainSummary is a lightweight representation for list views.
type ChainSummary struct {
	CorrelationID string      `json:"correlationId"`
	RootMessageID string      `json:"rootMessageId"`
	RootService   string      `json:"rootService"`
	Status        ChainStatus `json:"status"`
	MessageCount  int         `json:"messageCount"`
	RetryCount    int         `json:"retryCount"`
	FailureCount  int         `json:"failureCount"`
	StartTime     time.Time   `json:"startTime"`
	EndTime       time.Time   `json:"endTime,omitempty"`
	Duration      float64     `json:"durationMs,omitempty"`
}

// NewChainFromMessages builds a Chain from a list of messages.
func NewChainFromMessages(correlationID string, messages []*Message) *Chain {
	if len(messages) == 0 {
		return &Chain{
			CorrelationID: correlationID,
			Status:        ChainStatusInFlight,
			Nodes:         []ChainNode{},
			Edges:         []ChainEdge{},
		}
	}

	chain := &Chain{
		CorrelationID: correlationID,
		Nodes:         make([]ChainNode, 0, len(messages)),
		Edges:         make([]ChainEdge, 0, len(messages)),
	}

	// Build message map for depth calculation
	msgMap := make(map[string]*Message)
	for _, msg := range messages {
		msgMap[msg.MessageID] = msg
	}

	// Calculate depths and build nodes/edges
	depths := make(map[string]int)
	var maxDepth int
	var startTime, endTime time.Time
	var retryCount, failureCount int
	var rootMsgID string

	for _, msg := range messages {
		depth := calculateDepth(msg.MessageID, msgMap, depths)
		if depth > maxDepth {
			maxDepth = depth
		}

		// Track times
		if startTime.IsZero() || msg.Timestamp.Before(startTime) {
			startTime = msg.Timestamp
		}
		if msg.Timestamp.After(endTime) {
			endTime = msg.Timestamp
		}

		// Track root
		if msg.IsRoot() {
			rootMsgID = msg.MessageID
		}

		// Track metrics
		if msg.IsRetry() {
			retryCount++
		}
		if msg.IsFailed() {
			failureCount++
		}

		// Add node
		chain.Nodes = append(chain.Nodes, ChainNode{
			MessageID:       msg.MessageID,
			Service:         msg.ServiceOrigin,
			Topic:           msg.Topic,
			Status:          msg.Status,
			RetryCount:      msg.RetryCount,
			Timestamp:       msg.Timestamp,
			CausationReason: msg.CausationReason,
			PayloadPreview:  msg.PayloadPreview,
			Depth:           depth,
		})

		// Add edge if has parent
		if msg.ParentID != "" {
			chain.Edges = append(chain.Edges, ChainEdge{
				From:   msg.ParentID,
				To:     msg.MessageID,
				Reason: msg.CausationReason,
			})
		}
	}

	chain.RootMessageID = rootMsgID
	chain.MessageCount = len(messages)
	chain.StartTime = startTime
	chain.EndTime = endTime
	chain.RetryCount = retryCount
	chain.FailureCount = failureCount
	chain.DepthMax = maxDepth

	if !endTime.IsZero() && !startTime.IsZero() {
		chain.Duration = float64(endTime.Sub(startTime).Milliseconds())
	}

	// Determine chain status
	if failureCount > 0 {
		chain.Status = ChainStatusFailed
	} else {
		chain.Status = ChainStatusCompleted
	}

	return chain
}

// calculateDepth recursively calculates the depth of a message in the chain.
func calculateDepth(messageID string, msgMap map[string]*Message, depths map[string]int) int {
	if d, ok := depths[messageID]; ok {
		return d
	}

	msg, ok := msgMap[messageID]
	if !ok || msg.ParentID == "" {
		depths[messageID] = 0
		return 0
	}

	depth := calculateDepth(msg.ParentID, msgMap, depths) + 1
	depths[messageID] = depth
	return depth
}

// ChainQueryParams holds query parameters for listing chains.
type ChainQueryParams struct {
	Service string    `form:"service"`
	Since   time.Time `form:"since"`
	Until   time.Time `form:"until"`
	Status  string    `form:"status"`
	Limit   int       `form:"limit"`
	Offset  int       `form:"offset"`
}

// SetDefaults applies default values to query params.
func (p *ChainQueryParams) SetDefaults() {
	if p.Limit <= 0 {
		p.Limit = envelope.DefaultQueryLimit
	}
	if p.Limit > envelope.DefaultQueryMaxLimit {
		p.Limit = envelope.DefaultQueryMaxLimit
	}
	if p.Since.IsZero() {
		p.Since = time.Now().Add(-time.Duration(envelope.DefaultSinceHours) * time.Hour)
	}
	if p.Until.IsZero() {
		p.Until = time.Now()
	}
}

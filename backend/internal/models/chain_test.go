package models

import (
	"testing"
	"time"

	"github.com/eventlineage/backend/pkg/envelope"
)

func TestNewChainFromMessagesEmpty(t *testing.T) {
	chain := NewChainFromMessages("corr-1", []*Message{})

	if chain.CorrelationID != "corr-1" {
		t.Errorf("CorrelationID = %q, want %q", chain.CorrelationID, "corr-1")
	}
	if chain.Status != ChainStatusInFlight {
		t.Errorf("Status = %q, want %q", chain.Status, ChainStatusInFlight)
	}
	if len(chain.Nodes) != 0 {
		t.Errorf("Nodes length = %d, want 0", len(chain.Nodes))
	}
	if len(chain.Edges) != 0 {
		t.Errorf("Edges length = %d, want 0", len(chain.Edges))
	}
	if chain.MessageCount != 0 {
		t.Errorf("MessageCount = %d, want 0", chain.MessageCount)
	}
}

func TestNewChainFromMessagesNil(t *testing.T) {
	chain := NewChainFromMessages("corr-1", nil)

	if chain.Status != ChainStatusInFlight {
		t.Errorf("Status = %q, want %q", chain.Status, ChainStatusInFlight)
	}
	if chain.Nodes == nil {
		t.Error("Nodes should be empty slice, not nil")
	}
	if chain.Edges == nil {
		t.Error("Edges should be empty slice, not nil")
	}
}

func TestNewChainFromMessagesSingle(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	msgs := []*Message{
		{
			MessageID:     "msg-1",
			CorrelationID: "corr-1",
			RootMessageID: "msg-1",
			ServiceOrigin: "order-service",
			Topic:         "orders",
			Status:        envelope.StatusOK,
			Timestamp:     ts,
		},
	}

	chain := NewChainFromMessages("corr-1", msgs)

	if chain.MessageCount != 1 {
		t.Errorf("MessageCount = %d, want 1", chain.MessageCount)
	}
	if chain.RootMessageID != "msg-1" {
		t.Errorf("RootMessageID = %q, want %q", chain.RootMessageID, "msg-1")
	}
	if chain.Status != ChainStatusCompleted {
		t.Errorf("Status = %q, want %q", chain.Status, ChainStatusCompleted)
	}
	if chain.DepthMax != 0 {
		t.Errorf("DepthMax = %d, want 0", chain.DepthMax)
	}
	if len(chain.Nodes) != 1 {
		t.Errorf("Nodes length = %d, want 1", len(chain.Nodes))
	}
	if len(chain.Edges) != 0 {
		t.Errorf("Edges length = %d, want 0 (root has no parent edge)", len(chain.Edges))
	}
	if chain.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0", chain.RetryCount)
	}
	if chain.FailureCount != 0 {
		t.Errorf("FailureCount = %d, want 0", chain.FailureCount)
	}
	if !chain.StartTime.Equal(ts) {
		t.Errorf("StartTime = %v, want %v", chain.StartTime, ts)
	}
}

func TestNewChainFromMessagesLinear(t *testing.T) {
	// Linear chain: msg-1 -> msg-2 -> msg-3
	t0 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(1 * time.Second)
	t2 := t0.Add(2 * time.Second)

	msgs := []*Message{
		{
			MessageID:     "msg-1",
			CorrelationID: "corr-1",
			RootMessageID: "msg-1",
			ServiceOrigin: "svc-a",
			Topic:         "topic-1",
			Status:        envelope.StatusOK,
			Timestamp:     t0,
		},
		{
			MessageID:       "msg-2",
			ParentID:        "msg-1",
			CorrelationID:   "corr-1",
			RootMessageID:   "msg-1",
			CausationReason: "OrderCreated",
			ServiceOrigin:   "svc-b",
			Topic:           "topic-2",
			Status:          envelope.StatusOK,
			Timestamp:       t1,
		},
		{
			MessageID:       "msg-3",
			ParentID:        "msg-2",
			CorrelationID:   "corr-1",
			RootMessageID:   "msg-1",
			CausationReason: "PaymentProcessed",
			ServiceOrigin:   "svc-c",
			Topic:           "topic-3",
			Status:          envelope.StatusOK,
			Timestamp:       t2,
		},
	}

	chain := NewChainFromMessages("corr-1", msgs)

	if chain.MessageCount != 3 {
		t.Errorf("MessageCount = %d, want 3", chain.MessageCount)
	}
	if chain.DepthMax != 2 {
		t.Errorf("DepthMax = %d, want 2", chain.DepthMax)
	}
	if chain.Status != ChainStatusCompleted {
		t.Errorf("Status = %q, want %q", chain.Status, ChainStatusCompleted)
	}
	if len(chain.Edges) != 2 {
		t.Errorf("Edges length = %d, want 2", len(chain.Edges))
	}
	if !chain.StartTime.Equal(t0) {
		t.Errorf("StartTime = %v, want %v", chain.StartTime, t0)
	}
	if !chain.EndTime.Equal(t2) {
		t.Errorf("EndTime = %v, want %v", chain.EndTime, t2)
	}

	expectedDuration := float64(t2.Sub(t0).Milliseconds())
	if chain.Duration != expectedDuration {
		t.Errorf("Duration = %f, want %f", chain.Duration, expectedDuration)
	}
}

func TestNewChainFromMessagesBranching(t *testing.T) {
	// Branching chain: msg-1 -> msg-2, msg-1 -> msg-3
	t0 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(1 * time.Second)
	t2 := t0.Add(2 * time.Second)

	msgs := []*Message{
		{
			MessageID:     "msg-1",
			CorrelationID: "corr-1",
			RootMessageID: "msg-1",
			ServiceOrigin: "svc-a",
			Topic:         "topic-1",
			Status:        envelope.StatusOK,
			Timestamp:     t0,
		},
		{
			MessageID:       "msg-2",
			ParentID:        "msg-1",
			CorrelationID:   "corr-1",
			RootMessageID:   "msg-1",
			CausationReason: "BranchA",
			ServiceOrigin:   "svc-b",
			Topic:           "topic-2",
			Status:          envelope.StatusOK,
			Timestamp:       t1,
		},
		{
			MessageID:       "msg-3",
			ParentID:        "msg-1",
			CorrelationID:   "corr-1",
			RootMessageID:   "msg-1",
			CausationReason: "BranchB",
			ServiceOrigin:   "svc-c",
			Topic:           "topic-3",
			Status:          envelope.StatusOK,
			Timestamp:       t2,
		},
	}

	chain := NewChainFromMessages("corr-1", msgs)

	if chain.MessageCount != 3 {
		t.Errorf("MessageCount = %d, want 3", chain.MessageCount)
	}
	// Both branches have depth 1 (one step from root)
	if chain.DepthMax != 1 {
		t.Errorf("DepthMax = %d, want 1", chain.DepthMax)
	}
	if len(chain.Edges) != 2 {
		t.Errorf("Edges length = %d, want 2", len(chain.Edges))
	}
}

func TestNewChainFromMessagesDiamond(t *testing.T) {
	// Diamond shape: msg-1 -> msg-2, msg-1 -> msg-3, msg-2 -> msg-4, msg-3 -> msg-4
	// Note: msg-4 can only have one parent in the message model (single ParentID field)
	// So this tests: msg-1 -> msg-2, msg-1 -> msg-3, msg-2 -> msg-4
	t0 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	msgs := []*Message{
		{MessageID: "msg-1", RootMessageID: "msg-1", Status: envelope.StatusOK, Timestamp: t0},
		{MessageID: "msg-2", ParentID: "msg-1", RootMessageID: "msg-1", Status: envelope.StatusOK, Timestamp: t0.Add(time.Second)},
		{MessageID: "msg-3", ParentID: "msg-1", RootMessageID: "msg-1", Status: envelope.StatusOK, Timestamp: t0.Add(time.Second)},
		{MessageID: "msg-4", ParentID: "msg-2", RootMessageID: "msg-1", Status: envelope.StatusOK, Timestamp: t0.Add(2 * time.Second)},
	}

	chain := NewChainFromMessages("corr-1", msgs)

	if chain.MessageCount != 4 {
		t.Errorf("MessageCount = %d, want 4", chain.MessageCount)
	}
	if chain.DepthMax != 2 {
		t.Errorf("DepthMax = %d, want 2", chain.DepthMax)
	}
	// 3 edges: msg-1->msg-2, msg-1->msg-3, msg-2->msg-4
	if len(chain.Edges) != 3 {
		t.Errorf("Edges length = %d, want 3", len(chain.Edges))
	}
}

func TestNewChainFromMessagesWithFailures(t *testing.T) {
	t0 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	msgs := []*Message{
		{
			MessageID:     "msg-1",
			CorrelationID: "corr-1",
			RootMessageID: "msg-1",
			ServiceOrigin: "svc-a",
			Status:        envelope.StatusOK,
			Timestamp:     t0,
		},
		{
			MessageID:     "msg-2",
			ParentID:      "msg-1",
			CorrelationID: "corr-1",
			RootMessageID: "msg-1",
			ServiceOrigin: "svc-b",
			Status:        envelope.StatusFailed,
			Timestamp:     t0.Add(1 * time.Second),
		},
	}

	chain := NewChainFromMessages("corr-1", msgs)

	if chain.Status != ChainStatusFailed {
		t.Errorf("Status = %q, want %q", chain.Status, ChainStatusFailed)
	}
	if chain.FailureCount != 1 {
		t.Errorf("FailureCount = %d, want 1", chain.FailureCount)
	}
}

func TestNewChainFromMessagesMultipleFailures(t *testing.T) {
	t0 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	msgs := []*Message{
		{MessageID: "msg-1", RootMessageID: "msg-1", Status: envelope.StatusFailed, Timestamp: t0},
		{MessageID: "msg-2", ParentID: "msg-1", RootMessageID: "msg-1", Status: envelope.StatusFailed, Timestamp: t0.Add(time.Second)},
	}

	chain := NewChainFromMessages("corr-1", msgs)

	if chain.Status != ChainStatusFailed {
		t.Errorf("Status = %q, want %q", chain.Status, ChainStatusFailed)
	}
	if chain.FailureCount != 2 {
		t.Errorf("FailureCount = %d, want 2", chain.FailureCount)
	}
}

func TestNewChainFromMessagesWithRetries(t *testing.T) {
	t0 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	msgs := []*Message{
		{
			MessageID:     "msg-1",
			CorrelationID: "corr-1",
			RootMessageID: "msg-1",
			ServiceOrigin: "svc-a",
			Status:        envelope.StatusOK,
			Timestamp:     t0,
		},
		{
			MessageID:     "msg-2",
			ParentID:      "msg-1",
			CorrelationID: "corr-1",
			RootMessageID: "msg-1",
			ServiceOrigin: "svc-b",
			Status:        envelope.StatusOK,
			RetryCount:    3,
			Timestamp:     t0.Add(1 * time.Second),
		},
	}

	chain := NewChainFromMessages("corr-1", msgs)

	// RetryCount on chain counts messages that have retries, not total retry attempts
	if chain.RetryCount != 1 {
		t.Errorf("RetryCount = %d, want 1", chain.RetryCount)
	}
	if chain.Status != ChainStatusCompleted {
		t.Errorf("Status = %q, want %q", chain.Status, ChainStatusCompleted)
	}
}

func TestNewChainFromMessagesNodeFields(t *testing.T) {
	t0 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	msgs := []*Message{
		{
			MessageID:       "msg-1",
			CorrelationID:   "corr-1",
			RootMessageID:   "msg-1",
			CausationReason: "Initial",
			ServiceOrigin:   "svc-a",
			Topic:           "topic-1",
			Status:          envelope.StatusOK,
			RetryCount:      0,
			PayloadPreview:  "preview-data",
			Timestamp:       t0,
		},
	}

	chain := NewChainFromMessages("corr-1", msgs)

	if len(chain.Nodes) != 1 {
		t.Fatalf("Nodes length = %d, want 1", len(chain.Nodes))
	}

	node := chain.Nodes[0]
	if node.MessageID != "msg-1" {
		t.Errorf("Node.MessageID = %q, want %q", node.MessageID, "msg-1")
	}
	if node.Service != "svc-a" {
		t.Errorf("Node.Service = %q, want %q", node.Service, "svc-a")
	}
	if node.Topic != "topic-1" {
		t.Errorf("Node.Topic = %q, want %q", node.Topic, "topic-1")
	}
	if node.Status != envelope.StatusOK {
		t.Errorf("Node.Status = %q, want %q", node.Status, envelope.StatusOK)
	}
	if node.CausationReason != "Initial" {
		t.Errorf("Node.CausationReason = %q, want %q", node.CausationReason, "Initial")
	}
	if node.PayloadPreview != "preview-data" {
		t.Errorf("Node.PayloadPreview = %q, want %q", node.PayloadPreview, "preview-data")
	}
	if node.Depth != 0 {
		t.Errorf("Node.Depth = %d, want 0", node.Depth)
	}
	if !node.Timestamp.Equal(t0) {
		t.Errorf("Node.Timestamp = %v, want %v", node.Timestamp, t0)
	}
}

func TestNewChainFromMessagesEdgeFields(t *testing.T) {
	t0 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	msgs := []*Message{
		{MessageID: "msg-1", RootMessageID: "msg-1", Status: envelope.StatusOK, Timestamp: t0},
		{
			MessageID:       "msg-2",
			ParentID:        "msg-1",
			RootMessageID:   "msg-1",
			CausationReason: "OrderCreated",
			Status:          envelope.StatusOK,
			Timestamp:       t0.Add(time.Second),
		},
	}

	chain := NewChainFromMessages("corr-1", msgs)

	if len(chain.Edges) != 1 {
		t.Fatalf("Edges length = %d, want 1", len(chain.Edges))
	}

	edge := chain.Edges[0]
	if edge.From != "msg-1" {
		t.Errorf("Edge.From = %q, want %q", edge.From, "msg-1")
	}
	if edge.To != "msg-2" {
		t.Errorf("Edge.To = %q, want %q", edge.To, "msg-2")
	}
	if edge.Reason != "OrderCreated" {
		t.Errorf("Edge.Reason = %q, want %q", edge.Reason, "OrderCreated")
	}
}

func TestCalculateDepthLinear(t *testing.T) {
	// msg-1 -> msg-2 -> msg-3 -> msg-4
	msgMap := map[string]*Message{
		"msg-1": {MessageID: "msg-1"},
		"msg-2": {MessageID: "msg-2", ParentID: "msg-1"},
		"msg-3": {MessageID: "msg-3", ParentID: "msg-2"},
		"msg-4": {MessageID: "msg-4", ParentID: "msg-3"},
	}

	depths := make(map[string]int)

	if d := calculateDepth("msg-1", msgMap, depths); d != 0 {
		t.Errorf("depth(msg-1) = %d, want 0", d)
	}
	if d := calculateDepth("msg-2", msgMap, depths); d != 1 {
		t.Errorf("depth(msg-2) = %d, want 1", d)
	}
	if d := calculateDepth("msg-3", msgMap, depths); d != 2 {
		t.Errorf("depth(msg-3) = %d, want 2", d)
	}
	if d := calculateDepth("msg-4", msgMap, depths); d != 3 {
		t.Errorf("depth(msg-4) = %d, want 3", d)
	}
}

func TestCalculateDepthBranching(t *testing.T) {
	// msg-1 -> msg-2 -> msg-4
	// msg-1 -> msg-3
	msgMap := map[string]*Message{
		"msg-1": {MessageID: "msg-1"},
		"msg-2": {MessageID: "msg-2", ParentID: "msg-1"},
		"msg-3": {MessageID: "msg-3", ParentID: "msg-1"},
		"msg-4": {MessageID: "msg-4", ParentID: "msg-2"},
	}

	depths := make(map[string]int)

	if d := calculateDepth("msg-1", msgMap, depths); d != 0 {
		t.Errorf("depth(msg-1) = %d, want 0", d)
	}
	if d := calculateDepth("msg-2", msgMap, depths); d != 1 {
		t.Errorf("depth(msg-2) = %d, want 1", d)
	}
	if d := calculateDepth("msg-3", msgMap, depths); d != 1 {
		t.Errorf("depth(msg-3) = %d, want 1", d)
	}
	if d := calculateDepth("msg-4", msgMap, depths); d != 2 {
		t.Errorf("depth(msg-4) = %d, want 2", d)
	}
}

func TestCalculateDepthMemoization(t *testing.T) {
	// Verify that repeated calls use the cached result
	msgMap := map[string]*Message{
		"msg-1": {MessageID: "msg-1"},
		"msg-2": {MessageID: "msg-2", ParentID: "msg-1"},
		"msg-3": {MessageID: "msg-3", ParentID: "msg-2"},
	}

	depths := make(map[string]int)

	// Calculate msg-2 first to populate cache
	calculateDepth("msg-2", msgMap, depths)
	if _, ok := depths["msg-2"]; !ok {
		t.Error("Expected msg-2 depth to be cached")
	}
	if _, ok := depths["msg-1"]; !ok {
		t.Error("Expected msg-1 depth to be cached (calculated as part of msg-2)")
	}

	// Calculate msg-3 - should reuse cached values
	d := calculateDepth("msg-3", msgMap, depths)
	if d != 2 {
		t.Errorf("depth(msg-3) = %d, want 2", d)
	}
}

func TestCalculateDepthMissingParent(t *testing.T) {
	// Parent referenced but not in the message map
	msgMap := map[string]*Message{
		"msg-2": {MessageID: "msg-2", ParentID: "msg-1"},
	}

	depths := make(map[string]int)

	// msg-1 not in map, so calculateDepth("msg-1") returns 0
	// msg-2 depth = 0 + 1 = 1
	d := calculateDepth("msg-2", msgMap, depths)
	if d != 1 {
		t.Errorf("depth(msg-2) = %d, want 1", d)
	}
}

func TestCalculateDepthUnknownMessage(t *testing.T) {
	msgMap := map[string]*Message{}
	depths := make(map[string]int)

	d := calculateDepth("nonexistent", msgMap, depths)
	if d != 0 {
		t.Errorf("depth(nonexistent) = %d, want 0", d)
	}
}

func TestChainQueryParamsSetDefaultsEmpty(t *testing.T) {
	p := &ChainQueryParams{}
	before := time.Now()
	p.SetDefaults()

	if p.Limit != envelope.DefaultQueryLimit {
		t.Errorf("Limit = %d, want %d", p.Limit, envelope.DefaultQueryLimit)
	}
	if p.Since.IsZero() {
		t.Error("Since should not be zero after SetDefaults")
	}
	if p.Until.IsZero() {
		t.Error("Until should not be zero after SetDefaults")
	}

	// Since should be approximately 24 hours before now
	expectedSince := before.Add(-time.Duration(envelope.DefaultSinceHours) * time.Hour)
	if p.Since.Before(expectedSince.Add(-2 * time.Second)) {
		t.Errorf("Since is too far in the past: %v", p.Since)
	}
}

func TestChainQueryParamsSetDefaultsNegativeLimit(t *testing.T) {
	p := &ChainQueryParams{Limit: -1}
	p.SetDefaults()

	if p.Limit != envelope.DefaultQueryLimit {
		t.Errorf("Limit = %d, want %d", p.Limit, envelope.DefaultQueryLimit)
	}
}

func TestChainQueryParamsSetDefaultsZeroLimit(t *testing.T) {
	p := &ChainQueryParams{Limit: 0}
	p.SetDefaults()

	if p.Limit != envelope.DefaultQueryLimit {
		t.Errorf("Limit = %d, want %d", p.Limit, envelope.DefaultQueryLimit)
	}
}

func TestChainQueryParamsSetDefaultsExcessiveLimit(t *testing.T) {
	p := &ChainQueryParams{Limit: 500}
	p.SetDefaults()

	if p.Limit != envelope.DefaultQueryMaxLimit {
		t.Errorf("Limit = %d, want %d (capped to max)", p.Limit, envelope.DefaultQueryMaxLimit)
	}
}

func TestChainQueryParamsSetDefaultsValidLimit(t *testing.T) {
	p := &ChainQueryParams{Limit: 50}
	p.SetDefaults()

	if p.Limit != 50 {
		t.Errorf("Limit = %d, want 50 (should preserve valid limit)", p.Limit)
	}
}

func TestChainQueryParamsSetDefaultsPreservesProvidedTimes(t *testing.T) {
	since := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	p := &ChainQueryParams{
		Limit: 10,
		Since: since,
		Until: until,
	}
	p.SetDefaults()

	if !p.Since.Equal(since) {
		t.Errorf("Since = %v, want %v (should preserve provided time)", p.Since, since)
	}
	if !p.Until.Equal(until) {
		t.Errorf("Until = %v, want %v (should preserve provided time)", p.Until, until)
	}
}

func TestChainQueryParamsSetDefaultsPreservesService(t *testing.T) {
	p := &ChainQueryParams{
		Service: "order-service",
		Limit:   10,
	}
	p.SetDefaults()

	if p.Service != "order-service" {
		t.Errorf("Service = %q, want %q", p.Service, "order-service")
	}
}

func TestChainQueryParamsSetDefaultsPreservesOffset(t *testing.T) {
	p := &ChainQueryParams{
		Limit:  10,
		Offset: 20,
	}
	p.SetDefaults()

	if p.Offset != 20 {
		t.Errorf("Offset = %d, want 20", p.Offset)
	}
}

func TestChainQueryParamsSetDefaultsBoundaryLimit(t *testing.T) {
	// Exactly at max limit - should be preserved
	p := &ChainQueryParams{Limit: envelope.DefaultQueryMaxLimit}
	p.SetDefaults()

	if p.Limit != envelope.DefaultQueryMaxLimit {
		t.Errorf("Limit = %d, want %d", p.Limit, envelope.DefaultQueryMaxLimit)
	}

	// One over max limit - should be capped
	p2 := &ChainQueryParams{Limit: envelope.DefaultQueryMaxLimit + 1}
	p2.SetDefaults()

	if p2.Limit != envelope.DefaultQueryMaxLimit {
		t.Errorf("Limit = %d, want %d (should be capped)", p2.Limit, envelope.DefaultQueryMaxLimit)
	}
}

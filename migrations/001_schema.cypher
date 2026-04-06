-- EventLineage Neo4j Schema Migration
-- Version: 001
-- Description: Initial schema setup with indexes and constraints

-- ============================================================================
-- CONSTRAINTS
-- ============================================================================

-- Ensure message IDs are unique
CREATE CONSTRAINT message_unique IF NOT EXISTS
FOR (m:Message) REQUIRE m.messageId IS UNIQUE;

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Primary lookup index - most queries start with messageId
CREATE INDEX message_id_idx IF NOT EXISTS
FOR (m:Message) ON (m.messageId);

-- Correlation ID index - for chain reconstruction queries
-- This is the most common query pattern: "show me all messages in chain X"
CREATE INDEX correlation_id_idx IF NOT EXISTS
FOR (m:Message) ON (m.correlationId);

-- Root message ID index - for "list recent chains" queries
-- Without this, GET /api/v1/chains would require a full scan
CREATE INDEX root_message_id_idx IF NOT EXISTS
FOR (m:Message) ON (m.rootMessageId);

-- Service origin index - for service discovery and filtering
CREATE INDEX service_idx IF NOT EXISTS
FOR (m:Message) ON (m.serviceOrigin);

-- Timestamp index - for time-range queries
CREATE INDEX timestamp_idx IF NOT EXISTS
FOR (m:Message) ON (m.timestamp);

-- Composite index for common query pattern: service + time range
CREATE INDEX service_timestamp_idx IF NOT EXISTS
FOR (m:Message) ON (m.serviceOrigin, m.timestamp);

-- Status index - for filtering by message status
CREATE INDEX status_idx IF NOT EXISTS
FOR (m:Message) ON (m.status);

-- ============================================================================
-- NOTES FOR QUERY AUTHORS
-- ============================================================================

-- IMPORTANT: Chain reconstruction queries MUST use a depth limit of 20
-- to prevent runaway queries on pathological chains.
--
-- Example correct query:
--   MATCH path = (root:Message {correlationId: $correlationId})-[:CAUSED*0..20]->(leaf)
--   RETURN path
--
-- Example INCORRECT query (no depth limit):
--   MATCH path = (root:Message {correlationId: $correlationId})-[:CAUSED*]->(leaf)
--   RETURN path
--
-- The depth limit of 20 is defined in pkg/envelope/headers.go as DefaultChainDepthLimit

-- ============================================================================
-- SAMPLE QUERIES FOR REFERENCE
-- ============================================================================

-- Reconstruct a chain by correlationId:
-- MATCH (m:Message {correlationId: $correlationId})
-- OPTIONAL MATCH path = (m)-[:CAUSED*0..20]->(descendant)
-- WITH m, collect(DISTINCT descendant) AS descendants
-- RETURN m, descendants

-- Find root messages for a service in time range:
-- MATCH (m:Message)
-- WHERE m.serviceOrigin = $service
--   AND m.rootMessageId = m.messageId
--   AND m.timestamp >= $since
--   AND m.timestamp <= $until
-- RETURN m
-- ORDER BY m.timestamp DESC
-- LIMIT $limit

-- Get service statistics:
-- MATCH (m:Message)
-- WHERE m.timestamp >= $since
-- WITH m.serviceOrigin AS service,
--      count(m) AS messageCount,
--      count(DISTINCT m.correlationId) AS chainCount,
--      max(m.timestamp) AS lastSeen,
--      sum(CASE WHEN m.status = 'failed' THEN 1 ELSE 0 END) AS failures
-- RETURN service, messageCount, chainCount, lastSeen,
--        toFloat(failures) / messageCount AS failureRate
-- ORDER BY messageCount DESC

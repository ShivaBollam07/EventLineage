// Package store provides data access layer implementations.
package store

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Neo4jStore manages the Neo4j database connection.
type Neo4jStore struct {
	driver neo4j.DriverWithContext
	dbName string
}

// NewNeo4jStore creates a new Neo4j store connection.
func NewNeo4jStore(uri, username, password string) (*Neo4jStore, error) {
	driver, err := neo4j.NewDriverWithContext(
		uri,
		neo4j.BasicAuth(username, password, ""),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create neo4j driver: %w", err)
	}

	// Verify connectivity
	ctx := context.Background()
	if err := driver.VerifyConnectivity(ctx); err != nil {
		return nil, fmt.Errorf("failed to verify neo4j connectivity: %w", err)
	}

	return &Neo4jStore{
		driver: driver,
		dbName: "neo4j",
	}, nil
}

// Close closes the Neo4j driver connection.
func (s *Neo4jStore) Close() error {
	return s.driver.Close(context.Background())
}

// Session creates a new session for executing queries.
func (s *Neo4jStore) Session(ctx context.Context) neo4j.SessionWithContext {
	return s.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: s.dbName,
		AccessMode:   neo4j.AccessModeWrite,
	})
}

// ReadSession creates a new read-only session.
func (s *Neo4jStore) ReadSession(ctx context.Context) neo4j.SessionWithContext {
	return s.driver.NewSession(ctx, neo4j.SessionConfig{
		DatabaseName: s.dbName,
		AccessMode:   neo4j.AccessModeRead,
	})
}

// RunMigrations executes the schema migration.
func (s *Neo4jStore) RunMigrations(ctx context.Context) error {
	session := s.Session(ctx)
	defer session.Close(ctx)

	migrations := []string{
		// Constraint
		`CREATE CONSTRAINT message_unique IF NOT EXISTS FOR (m:Message) REQUIRE m.messageId IS UNIQUE`,

		// Indexes
		`CREATE INDEX message_id_idx IF NOT EXISTS FOR (m:Message) ON (m.messageId)`,
		`CREATE INDEX correlation_id_idx IF NOT EXISTS FOR (m:Message) ON (m.correlationId)`,
		`CREATE INDEX root_message_id_idx IF NOT EXISTS FOR (m:Message) ON (m.rootMessageId)`,
		`CREATE INDEX service_idx IF NOT EXISTS FOR (m:Message) ON (m.serviceOrigin)`,
		`CREATE INDEX timestamp_idx IF NOT EXISTS FOR (m:Message) ON (m.timestamp)`,
		`CREATE INDEX status_idx IF NOT EXISTS FOR (m:Message) ON (m.status)`,
	}

	for _, migration := range migrations {
		_, err := session.Run(ctx, migration, nil)
		if err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
	}

	return nil
}

// HealthCheck verifies the database connection is healthy.
func (s *Neo4jStore) HealthCheck(ctx context.Context) error {
	session := s.ReadSession(ctx)
	defer session.Close(ctx)

	result, err := session.Run(ctx, "RETURN 1", nil)
	if err != nil {
		return err
	}

	_, err = result.Single(ctx)
	return err
}

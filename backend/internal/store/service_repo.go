package store

import (
	"context"
	"fmt"
	"time"

	"github.com/eventlineage/backend/internal/models"
)

// ServiceRepository handles service-level aggregation queries.
type ServiceRepository struct {
	store *Neo4jStore
}

// NewServiceRepository creates a new service repository.
func NewServiceRepository(store *Neo4jStore) *ServiceRepository {
	return &ServiceRepository{store: store}
}

// ListServices returns aggregated statistics for all services.
func (r *ServiceRepository) ListServices(ctx context.Context, since time.Time) ([]models.ServiceInfo, error) {
	session := r.store.ReadSession(ctx)
	defer session.Close(ctx)

	query := `
		MATCH (m:Message)
		WHERE m.timestamp >= datetime($since)
		WITH m.serviceOrigin AS service,
			 count(m) AS messageCount,
			 count(DISTINCT m.correlationId) AS chainCount,
			 max(m.timestamp) AS lastSeen,
			 sum(CASE WHEN m.status = 'failed' THEN 1 ELSE 0 END) AS failures
		RETURN service, messageCount, chainCount, lastSeen,
			   CASE WHEN messageCount > 0
					THEN toFloat(failures) / messageCount
					ELSE 0.0 END AS failureRate
		ORDER BY messageCount DESC
	`

	result, err := session.Run(ctx, query, map[string]any{
		"since": since.Format(time.RFC3339),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list services: %w", err)
	}

	records, err := result.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to collect service list: %w", err)
	}

	services := make([]models.ServiceInfo, 0, len(records))
	for _, record := range records {
		props := record.AsMap()

		service := models.ServiceInfo{
			Name:         getString(props, "service"),
			MessageCount: int64(getInt(props, "messageCount")),
			ChainCount:   int64(getInt(props, "chainCount")),
			FailureRate:  getFloat(props, "failureRate"),
		}

		if ts, ok := props["lastSeen"].(time.Time); ok {
			service.LastSeen = ts
		}

		services = append(services, service)
	}

	return services, nil
}

// GetServiceStats returns statistics for a specific service.
func (r *ServiceRepository) GetServiceStats(ctx context.Context, serviceName string, since time.Time) (*models.ServiceInfo, error) {
	session := r.store.ReadSession(ctx)
	defer session.Close(ctx)

	query := `
		MATCH (m:Message {serviceOrigin: $service})
		WHERE m.timestamp >= datetime($since)
		WITH count(m) AS messageCount,
			 count(DISTINCT m.correlationId) AS chainCount,
			 max(m.timestamp) AS lastSeen,
			 sum(CASE WHEN m.status = 'failed' THEN 1 ELSE 0 END) AS failures
		RETURN messageCount, chainCount, lastSeen,
			   CASE WHEN messageCount > 0
					THEN toFloat(failures) / messageCount
					ELSE 0.0 END AS failureRate
	`

	result, err := session.Run(ctx, query, map[string]any{
		"service": serviceName,
		"since":   since.Format(time.RFC3339),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get service stats: %w", err)
	}

	record, err := result.Single(ctx)
	if err != nil {
		return nil, fmt.Errorf("service not found: %w", err)
	}

	props := record.AsMap()
	service := &models.ServiceInfo{
		Name:         serviceName,
		MessageCount: int64(getInt(props, "messageCount")),
		ChainCount:   int64(getInt(props, "chainCount")),
		FailureRate:  getFloat(props, "failureRate"),
	}

	if ts, ok := props["lastSeen"].(time.Time); ok {
		service.LastSeen = ts
	}

	return service, nil
}

func getFloat(props map[string]any, key string) float64 {
	if v, ok := props[key].(float64); ok {
		return v
	}
	return 0.0
}

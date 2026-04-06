package api

import (
	"context"
	"net/http"
	"time"

	"github.com/eventlineage/backend/internal/store"
	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

// Version is the application version, set at build time or defaulting here.
var Version = "1.1.0"

// HealthHandler handles health check endpoints.
type HealthHandler struct {
	neo4jStore   *store.Neo4jStore
	kafkaBrokers string
}

// NewHealthHandler creates a new health handler.
func NewHealthHandler(neo4jStore *store.Neo4jStore, kafkaBrokers string) *HealthHandler {
	return &HealthHandler{
		neo4jStore:   neo4jStore,
		kafkaBrokers: kafkaBrokers,
	}
}

// HealthResponse is the response format for health checks.
type HealthResponse struct {
	Status  string `json:"status"`
	Version string `json:"version"`
}

// Health returns the service liveness status.
// @Summary Liveness check
// @Description Returns OK if the service is alive
// @Tags health
// @Produce json
// @Success 200 {object} HealthResponse
// @Router /health [get]
func (h *HealthHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, HealthResponse{
		Status:  "ok",
		Version: Version,
	})
}

// ReadyResponse is the response format for readiness checks.
type ReadyResponse struct {
	Status  string            `json:"status"`
	Checks  map[string]string `json:"checks"`
	Version string            `json:"version"`
}

// Ready returns the service readiness status.
// @Summary Readiness check
// @Description Returns OK if the service is ready to handle requests
// @Tags health
// @Produce json
// @Success 200 {object} ReadyResponse
// @Failure 503 {object} ReadyResponse
// @Router /ready [get]
func (h *HealthHandler) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	checks := map[string]string{}
	allOk := true

	// Check Neo4j connectivity
	if err := h.neo4jStore.HealthCheck(ctx); err != nil {
		checks["neo4j"] = "failed: " + err.Error()
		allOk = false
	} else {
		checks["neo4j"] = "ok"
	}

	// Check Kafka connectivity
	if err := h.checkKafka(ctx); err != nil {
		checks["kafka"] = "failed: " + err.Error()
		allOk = false
	} else {
		checks["kafka"] = "ok"
	}

	status := "ok"
	httpStatus := http.StatusOK
	if !allOk {
		status = "degraded"
		httpStatus = http.StatusServiceUnavailable
	}

	c.JSON(httpStatus, ReadyResponse{
		Status:  status,
		Checks:  checks,
		Version: Version,
	})
}

// checkKafka verifies Kafka broker connectivity.
func (h *HealthHandler) checkKafka(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", h.kafkaBrokers)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Brokers()
	return err
}

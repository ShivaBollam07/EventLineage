package api

import (
	"time"

	"github.com/eventlineage/backend/internal/store"
	"github.com/eventlineage/backend/pkg/envelope"
	"github.com/gin-gonic/gin"
)

// StatsHandler handles aggregate dashboard statistics.
type StatsHandler struct {
	serviceRepo *store.ServiceRepository
	chainRepo   *store.ChainRepository
}

// NewStatsHandler creates a new stats handler.
func NewStatsHandler(serviceRepo *store.ServiceRepository, chainRepo *store.ChainRepository) *StatsHandler {
	return &StatsHandler{serviceRepo: serviceRepo, chainRepo: chainRepo}
}

// StatsResponse is the aggregate stats payload for the UI dashboard.
type StatsResponse struct {
	ActiveServices    int     `json:"activeServices"`
	TotalChains       int64   `json:"totalChains"`
	TotalMessages     int64   `json:"totalMessages"`
	AvgFailureRate    float64 `json:"avgFailureRate"`
	FailedChains      int     `json:"failedChains"`
	RetriedChains     int     `json:"retriedChains"`
	HealthyChains     int     `json:"healthyChains"`
}

// GetStats returns aggregate dashboard statistics.
// @Summary Get aggregate stats
// @Description Returns overview statistics for the dashboard
// @Tags stats
// @Produce json
// @Success 200 {object} StatsResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/stats [get]
func (h *StatsHandler) GetStats(c *gin.Context) {
	since := time.Now().Add(-time.Duration(envelope.DefaultSinceHours) * time.Hour)

	services, err := h.serviceRepo.ListServices(c.Request.Context(), since)
	if err != nil {
		respondError(c, 500, err.Error())
		return
	}

	var totalMessages int64
	var totalChains int64
	var totalFailRate float64
	for _, svc := range services {
		totalMessages += svc.MessageCount
		totalChains += svc.ChainCount
		totalFailRate += svc.FailureRate
	}

	avgFail := 0.0
	if len(services) > 0 {
		avgFail = totalFailRate / float64(len(services))
	}

	respondSuccess(c, StatsResponse{
		ActiveServices: len(services),
		TotalChains:    totalChains,
		TotalMessages:  totalMessages,
		AvgFailureRate: avgFail,
	})
}

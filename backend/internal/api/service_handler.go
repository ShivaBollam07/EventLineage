package api

import (
	"net/http"
	"time"

	"github.com/eventlineage/backend/internal/models"
	"github.com/eventlineage/backend/internal/store"
	"github.com/eventlineage/backend/pkg/envelope"
	"github.com/gin-gonic/gin"
)

// ServiceHandler handles service-related API endpoints.
type ServiceHandler struct {
	serviceRepo *store.ServiceRepository
}

// NewServiceHandler creates a new service handler.
func NewServiceHandler(serviceRepo *store.ServiceRepository) *ServiceHandler {
	return &ServiceHandler{serviceRepo: serviceRepo}
}

// ListServicesResponse is the response format for listing services.
type ListServicesResponse struct {
	Services []models.ServiceInfo `json:"services"`
}

// ListServices returns statistics for all services.
// @Summary List services
// @Description Get list of services with message counts and failure rates
// @Tags services
// @Accept json
// @Produce json
// @Param since query string false "Start time (ISO8601), default: 24h ago"
// @Success 200 {object} ListServicesResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/services [get]
func (h *ServiceHandler) ListServices(c *gin.Context) {
	since := time.Now().Add(-time.Duration(envelope.DefaultSinceHours) * time.Hour)

	if sinceStr := c.Query("since"); sinceStr != "" {
		t, err := time.Parse(time.RFC3339, sinceStr)
		if err != nil {
			respondError(c, http.StatusBadRequest, "Invalid 'since' format. Use ISO8601.")
			return
		}
		since = t
	}

	services, err := h.serviceRepo.ListServices(c.Request.Context(), since)
	if err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}

	respondSuccess(c, ListServicesResponse{Services: services})
}

// GetService returns statistics for a specific service.
// @Summary Get service
// @Description Get statistics for a specific service
// @Tags services
// @Accept json
// @Produce json
// @Param name path string true "Service name"
// @Param since query string false "Start time (ISO8601), default: 24h ago"
// @Success 200 {object} models.ServiceInfo
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/services/{name} [get]
func (h *ServiceHandler) GetService(c *gin.Context) {
	name := c.Param("name")
	if name == "" {
		respondError(c, http.StatusBadRequest, "service name is required")
		return
	}

	since := time.Now().Add(-time.Duration(envelope.DefaultSinceHours) * time.Hour)

	if sinceStr := c.Query("since"); sinceStr != "" {
		t, err := time.Parse(time.RFC3339, sinceStr)
		if err != nil {
			respondError(c, http.StatusBadRequest, "Invalid 'since' format. Use ISO8601.")
			return
		}
		since = t
	}

	service, err := h.serviceRepo.GetServiceStats(c.Request.Context(), name, since)
	if err != nil {
		respondError(c, http.StatusNotFound, err.Error())
		return
	}

	respondSuccess(c, service)
}

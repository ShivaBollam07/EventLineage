package api

import (
	"net/http"
	"time"

	"github.com/eventlineage/backend/internal/models"
	"github.com/eventlineage/backend/internal/store"
	"github.com/gin-gonic/gin"
)

// ChainHandler handles chain-related API endpoints.
type ChainHandler struct {
	chainRepo *store.ChainRepository
}

// NewChainHandler creates a new chain handler.
func NewChainHandler(chainRepo *store.ChainRepository) *ChainHandler {
	return &ChainHandler{chainRepo: chainRepo}
}

// ListChainsQuery represents query parameters for listing chains.
type ListChainsQuery struct {
	Service string `form:"service"`
	Since   string `form:"since"`
	Until   string `form:"until"`
	Status  string `form:"status"`
	Limit   int    `form:"limit,default=20"`
	Offset  int    `form:"offset,default=0"`
}

// ListChainsResponse is the response format for listing chains.
type ListChainsResponse struct {
	Chains  []models.ChainSummary `json:"chains"`
	HasMore bool                  `json:"hasMore"` // True if there are more results beyond this page
	Limit   int                   `json:"limit"`
	Offset  int                   `json:"offset"`
}

// ListChains returns paginated chain summaries.
// @Summary List chains
// @Description Get paginated list of chain summaries with optional filters
// @Tags chains
// @Accept json
// @Produce json
// @Param service query string false "Filter by service name"
// @Param since query string false "Start time (ISO8601), default: 24h ago"
// @Param until query string false "End time (ISO8601), default: now"
// @Param status query string false "Filter by status: ok, failed, pending"
// @Param limit query int false "Max results (default: 20, max: 100)"
// @Param offset query int false "Pagination offset (default: 0)"
// @Success 200 {object} ListChainsResponse
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/chains [get]
func (h *ChainHandler) ListChains(c *gin.Context) {
	var query ListChainsQuery
	if err := c.ShouldBindQuery(&query); err != nil {
		respondError(c, http.StatusBadRequest, "Invalid query parameters")
		return
	}

	// Parse time parameters
	params := &models.ChainQueryParams{
		Service: query.Service,
		Status:  query.Status,
		Limit:   query.Limit,
		Offset:  query.Offset,
	}

	if query.Since != "" {
		t, err := time.Parse(time.RFC3339, query.Since)
		if err != nil {
			respondError(c, http.StatusBadRequest, "Invalid 'since' format. Use ISO8601.")
			return
		}
		params.Since = t
	}

	if query.Until != "" {
		t, err := time.Parse(time.RFC3339, query.Until)
		if err != nil {
			respondError(c, http.StatusBadRequest, "Invalid 'until' format. Use ISO8601.")
			return
		}
		params.Until = t
	}

	chains, err := h.chainRepo.ListChains(c.Request.Context(), params)
	if err != nil {
		respondError(c, http.StatusInternalServerError, err.Error())
		return
	}

	// Check if there are more results (we fetch limit+1 to determine this)
	hasMore := len(chains) > params.Limit
	if hasMore {
		chains = chains[:params.Limit] // Trim to requested limit
	}

	respondSuccess(c, ListChainsResponse{
		Chains:  chains,
		HasMore: hasMore,
		Limit:   params.Limit,
		Offset:  params.Offset,
	})
}

// GetChain returns a complete chain by correlation ID.
// @Summary Get chain
// @Description Get complete chain with all nodes and edges
// @Tags chains
// @Accept json
// @Produce json
// @Param correlationId path string true "Correlation ID"
// @Success 200 {object} models.Chain
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/chains/{correlationId} [get]
func (h *ChainHandler) GetChain(c *gin.Context) {
	correlationID := c.Param("correlationId")
	if correlationID == "" {
		respondError(c, http.StatusBadRequest, "correlationId is required")
		return
	}

	chain, err := h.chainRepo.GetChain(c.Request.Context(), correlationID)
	if err != nil {
		respondError(c, http.StatusNotFound, err.Error())
		return
	}

	respondSuccess(c, chain)
}

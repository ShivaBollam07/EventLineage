// Package api provides the REST API layer.
package api

import (
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/eventlineage/backend/internal/store"
	"github.com/gin-gonic/gin"
)

// NewRouter creates and configures the Gin router.
func NewRouter(
	messageRepo *store.MessageRepository,
	chainRepo *store.ChainRepository,
	serviceRepo *store.ServiceRepository,
	neo4jStore *store.Neo4jStore,
	kafkaBrokers string,
) *gin.Engine {
	router := gin.Default()

	// Middleware
	router.Use(corsMiddleware())
	router.Use(requestLoggerMiddleware())

	// Health endpoints
	healthHandler := NewHealthHandler(neo4jStore, kafkaBrokers)
	router.GET("/health", healthHandler.Health)
	router.GET("/ready", healthHandler.Ready)

	// API v1
	v1 := router.Group("/api/v1")
	{
		// Chain endpoints
		chainHandler := NewChainHandler(chainRepo)
		v1.GET("/chains", chainHandler.ListChains)
		v1.GET("/chains/:correlationId", chainHandler.GetChain)

		// Message endpoints
		messageHandler := NewMessageHandler(messageRepo)
		v1.GET("/messages/:messageId", messageHandler.GetMessage)

		// Service endpoints
		serviceHandler := NewServiceHandler(serviceRepo)
		v1.GET("/services", serviceHandler.ListServices)
		v1.GET("/services/:name", serviceHandler.GetService)

		// Stats endpoint
		statsHandler := NewStatsHandler(serviceRepo, chainRepo)
		v1.GET("/stats", statsHandler.GetStats)
	}

	return router
}

// corsMiddleware adds CORS headers for frontend access.
// Reads allowed origins from CORS_ALLOWED_ORIGINS env var (comma-separated).
// Defaults to "*" if not set.
func corsMiddleware() gin.HandlerFunc {
	allowedOrigins := os.Getenv("CORS_ALLOWED_ORIGINS")
	if allowedOrigins == "" {
		allowedOrigins = "*"
	}

	// Parse comma-separated origins into a set for fast lookup
	originsSet := make(map[string]bool)
	for _, origin := range strings.Split(allowedOrigins, ",") {
		originsSet[strings.TrimSpace(origin)] = true
	}

	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")

		// Check if origin is allowed
		if allowedOrigins == "*" {
			c.Header("Access-Control-Allow-Origin", "*")
		} else if originsSet[origin] {
			c.Header("Access-Control-Allow-Origin", origin)
		}

		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Origin, Content-Type, Accept, Authorization")
		c.Header("Access-Control-Max-Age", "86400")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

// requestLoggerMiddleware logs request details.
func requestLoggerMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()

		gin.DefaultWriter.Write([]byte(
			c.Request.Method + " " +
				c.Request.URL.Path + " " +
				http.StatusText(status) + " " +
				latency.String() + "\n",
		))
	}
}

// ErrorResponse is the standard error response format.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
	Code    string `json:"code,omitempty"`
}

// respondError sends a JSON error response.
func respondError(c *gin.Context, status int, message string) {
	c.JSON(status, ErrorResponse{
		Error:   http.StatusText(status),
		Message: message,
	})
}

// respondSuccess sends a JSON success response.
func respondSuccess(c *gin.Context, data any) {
	c.JSON(http.StatusOK, data)
}

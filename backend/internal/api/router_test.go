package api

import (
	"testing"

	"github.com/gin-gonic/gin"
)

func TestAllRoutesRegistered(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// NewRouter accepts nil repos because route registration only stores them,
	// it does not call any methods on them.
	router := NewRouter(nil, nil, nil, nil, "")

	expectedRoutes := []struct {
		method string
		path   string
		desc   string
	}{
		{"GET", "/health", "liveness check"},
		{"GET", "/ready", "readiness check"},
		{"GET", "/api/v1/chains", "list chains"},
		{"GET", "/api/v1/chains/:correlationId", "get chain by correlation ID"},
		{"GET", "/api/v1/messages/:messageId", "get message by ID"},
		{"GET", "/api/v1/services", "list services"},
		{"GET", "/api/v1/services/:name", "get service by name"},
		{"GET", "/api/v1/stats", "get aggregate stats"},
	}

	routes := router.Routes()
	registered := make(map[string]bool)
	for _, r := range routes {
		key := r.Method + " " + r.Path
		registered[key] = true
	}

	for _, expected := range expectedRoutes {
		key := expected.method + " " + expected.path
		if !registered[key] {
			t.Errorf("Route %s %s (%s) is not registered", expected.method, expected.path, expected.desc)
		}
	}
}

func TestRouteCount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := NewRouter(nil, nil, nil, nil, "")

	routes := router.Routes()
	getRoutes := 0
	for _, r := range routes {
		if r.Method == "GET" {
			getRoutes++
		}
	}

	// 8 GET routes: health, ready, chains, chains/:correlationId,
	// messages/:messageId, services, services/:name, stats
	expectedGetRoutes := 8
	if getRoutes != expectedGetRoutes {
		t.Errorf("GET route count = %d, want %d", getRoutes, expectedGetRoutes)
		for _, r := range routes {
			if r.Method == "GET" {
				t.Logf("  GET %s", r.Path)
			}
		}
	}
}

func TestRoutesUseAPIv1Prefix(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := NewRouter(nil, nil, nil, nil, "")

	routes := router.Routes()
	apiRoutes := 0
	for _, r := range routes {
		if r.Method == "GET" && len(r.Path) > 7 && r.Path[:7] == "/api/v1" {
			apiRoutes++
		}
	}

	// 6 API v1 routes (excludes /health and /ready which are at root)
	expectedAPIRoutes := 6
	if apiRoutes != expectedAPIRoutes {
		t.Errorf("API v1 route count = %d, want %d", apiRoutes, expectedAPIRoutes)
	}
}

func TestHealthRoutesAtRoot(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := NewRouter(nil, nil, nil, nil, "")

	routes := router.Routes()
	registered := make(map[string]bool)
	for _, r := range routes {
		registered[r.Method+" "+r.Path] = true
	}

	// Health routes should be at root, not under /api/v1
	if !registered["GET /health"] {
		t.Error("/health route not registered at root level")
	}
	if !registered["GET /ready"] {
		t.Error("/ready route not registered at root level")
	}
}

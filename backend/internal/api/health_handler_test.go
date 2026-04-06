package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestHealthEndpointReturns200(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// HealthHandler with nil dependencies works because Health() does not use them
	handler := &HealthHandler{}
	router := gin.New()
	router.GET("/health", handler.Health)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}

	var resp HealthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.Status != "ok" {
		t.Errorf("Status = %q, want %q", resp.Status, "ok")
	}
}

func TestHealthResponseIncludesVersion(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := &HealthHandler{}
	router := gin.New()
	router.GET("/health", handler.Health)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var resp HealthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.Version == "" {
		t.Error("Version should not be empty")
	}
	if resp.Version != Version {
		t.Errorf("Version = %q, want %q", resp.Version, Version)
	}
}

func TestHealthResponseContentType(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := &HealthHandler{}
	router := gin.New()
	router.GET("/health", handler.Health)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q, want %q", contentType, "application/json; charset=utf-8")
	}
}

func TestHealthResponseJSONStructure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	handler := &HealthHandler{}
	router := gin.New()
	router.GET("/health", handler.Health)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Decode into a generic map to verify JSON structure
	var rawResp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&rawResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if _, ok := rawResp["status"]; !ok {
		t.Error("Response JSON missing 'status' field")
	}
	if _, ok := rawResp["version"]; !ok {
		t.Error("Response JSON missing 'version' field")
	}
}

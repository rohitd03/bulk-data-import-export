package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
)

// HealthHandler handles health check endpoints
type HealthHandler struct {
	db        *sqlx.DB
	startTime time.Time
}

// NewHealthHandler creates a new health handler
func NewHealthHandler(db *sqlx.DB) *HealthHandler {
	return &HealthHandler{
		db:        db,
		startTime: time.Now(),
	}
}

// HealthResponse represents the health check response
type HealthResponse struct {
	Status    string        `json:"status"`
	Timestamp string        `json:"timestamp"`
	Uptime    string        `json:"uptime"`
	Services  ServiceHealth `json:"services"`
}

// ServiceHealth represents health of individual services
type ServiceHealth struct {
	Database string `json:"database"`
}

// Health handles GET /health
func (h *HealthHandler) Health(c *gin.Context) {
	status := "healthy"
	dbStatus := "up"

	// Check database connection
	if err := h.db.Ping(); err != nil {
		status = "unhealthy"
		dbStatus = "down"
	}

	response := HealthResponse{
		Status:    status,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Uptime:    time.Since(h.startTime).String(),
		Services: ServiceHealth{
			Database: dbStatus,
		},
	}

	statusCode := http.StatusOK
	if status == "unhealthy" {
		statusCode = http.StatusServiceUnavailable
	}

	c.JSON(statusCode, response)
}

// Ready handles GET /ready
func (h *HealthHandler) Ready(c *gin.Context) {
	// Check if the service is ready to accept requests
	if err := h.db.Ping(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not ready"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// Live handles GET /live
func (h *HealthHandler) Live(c *gin.Context) {
	// Simple liveness check
	c.JSON(http.StatusOK, gin.H{"status": "alive"})
}

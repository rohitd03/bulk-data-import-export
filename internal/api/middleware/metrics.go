package middleware

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rohit/bulk-import-export/internal/metrics"
)

// Metrics returns a gin middleware for recording HTTP metrics
func Metrics(collector *metrics.Collector) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		// Process request
		c.Next()

		// Record metrics
		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())
		path := c.FullPath()
		if path == "" {
			path = "unknown"
		}

		collector.RecordHTTPRequest(c.Request.Method, path, status, duration)
	}
}

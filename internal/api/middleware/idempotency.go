package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rohit/bulk-import-export/internal/repository/postgres"
)

// IdempotencyKey header name
const IdempotencyKeyHeader = "Idempotency-Key"

// Idempotency returns a gin middleware for handling idempotent requests
func Idempotency(idempotencyRepo *postgres.IdempotencyRepository) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Only check POST requests
		if c.Request.Method != http.MethodPost {
			c.Next()
			return
		}

		idempotencyKey := c.GetHeader(IdempotencyKeyHeader)
		if idempotencyKey == "" {
			// No idempotency key provided, proceed normally
			c.Next()
			return
		}

		// Validate UUID format
		if _, err := uuid.Parse(idempotencyKey); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid idempotency key format"})
			c.Abort()
			return
		}

		// Check if key already exists
		existing, err := idempotencyRepo.GetByKey(c.Request.Context(), idempotencyKey)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to check idempotency key"})
			c.Abort()
			return
		}

		if existing != nil {
			// Return the same response as the original request
			c.JSON(existing.StatusCode, gin.H{
				"job_id":             existing.JobID.String(),
				"status":             "already_processed",
				"original_timestamp": existing.CreatedAt,
			})
			c.Abort()
			return
		}

		// Store the idempotency key in context for later use
		c.Set("idempotency_key", idempotencyKey)
		c.Next()
	}
}

package handlers

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rohit/bulk-import-export/internal/config"
	"github.com/rohit/bulk-import-export/internal/domain/models"
	"github.com/rohit/bulk-import-export/internal/repository/postgres"
	exportservice "github.com/rohit/bulk-import-export/internal/service/export"
	"github.com/rohit/bulk-import-export/internal/worker"
	"github.com/rs/zerolog"
)

// ExportHandler handles export-related HTTP requests
type ExportHandler struct {
	exportSvc  *exportservice.Service
	jobRepo    *postgres.JobRepository
	workerPool *worker.Pool
	logger     zerolog.Logger
	config     config.ExportConfig
}

// NewExportHandler creates a new export handler
func NewExportHandler(
	exportSvc *exportservice.Service,
	jobRepo *postgres.JobRepository,
	workerPool *worker.Pool,
	logger zerolog.Logger,
	cfg config.ExportConfig,
) *ExportHandler {
	return &ExportHandler{
		exportSvc:  exportSvc,
		jobRepo:    jobRepo,
		workerPool: workerPool,
		logger:     logger,
		config:     cfg,
	}
}

// StreamExport handles GET /v1/exports (streaming export)
func (h *ExportHandler) StreamExport(c *gin.Context) {
	// Get parameters
	resourceStr := c.Query("resource")
	if resourceStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "resource is required"})
		return
	}

	resource := models.ResourceType(resourceStr)
	if resource != models.ResourceTypeUsers &&
		resource != models.ResourceTypeArticles &&
		resource != models.ResourceTypeComments {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid resource type"})
		return
	}

	format := c.DefaultQuery("format", "ndjson")
	if format != "ndjson" && format != "json" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "format must be 'ndjson' or 'json'"})
		return
	}

	// Parse filters
	filters := h.parseFilters(c)

	// Set appropriate content type
	if format == "ndjson" {
		c.Header("Content-Type", "application/x-ndjson")
	} else {
		c.Header("Content-Type", "application/json")
	}
	c.Header("Transfer-Encoding", "chunked")

	// Get the response writer
	w := c.Writer

	var err error
	if format == "json" {
		err = h.exportSvc.StreamJSON(c.Request.Context(), w, resource, filters)
	} else {
		// Stream NDJSON
		switch resource {
		case models.ResourceTypeUsers:
			err = h.exportSvc.StreamUsers(c.Request.Context(), w, filters)
		case models.ResourceTypeArticles:
			err = h.exportSvc.StreamArticles(c.Request.Context(), w, filters)
		case models.ResourceTypeComments:
			err = h.exportSvc.StreamComments(c.Request.Context(), w, filters)
		}
	}

	if err != nil {
		h.logger.Error().Err(err).Msg("Export streaming failed")
		// Can't send error response after streaming started
		return
	}
}

// CreateAsyncExportRequest represents the request for async export
type CreateAsyncExportRequest struct {
	Resource string                 `json:"resource" binding:"required"`
	Format   string                 `json:"format,omitempty"`
	Filters  map[string]interface{} `json:"filters,omitempty"`
	Fields   []string               `json:"fields,omitempty"`
}

// CreateAsyncExportResponse represents the response for creating async export
type CreateAsyncExportResponse struct {
	JobID     string `json:"job_id"`
	Status    string `json:"status"`
	Resource  string `json:"resource"`
	CreatedAt string `json:"created_at"`
}

// CreateAsyncExport handles POST /v1/exports
func (h *ExportHandler) CreateAsyncExport(c *gin.Context) {
	var req CreateAsyncExportRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resource := models.ResourceType(req.Resource)
	if resource != models.ResourceTypeUsers &&
		resource != models.ResourceTypeArticles &&
		resource != models.ResourceTypeComments {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid resource type"})
		return
	}

	format := req.Format
	if format == "" {
		format = "ndjson"
	}
	if format != "ndjson" && format != "json" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "format must be 'ndjson' or 'json'"})
		return
	}

	// Create job
	job := &models.Job{
		ID:       uuid.New(),
		Type:     models.JobTypeExport,
		Resource: resource,
		Status:   models.JobStatusPending,
	}

	if err := h.jobRepo.Create(c.Request.Context(), job); err != nil {
		h.logger.Error().Err(err).Msg("Failed to create export job")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create job"})
		return
	}

	// Parse filters
	filters := h.parseFiltersFromMap(req.Filters)

	// Submit to worker pool
	h.workerPool.SubmitExportJob(job, filters)

	c.JSON(http.StatusAccepted, CreateAsyncExportResponse{
		JobID:     job.ID.String(),
		Status:    string(job.Status),
		Resource:  string(job.Resource),
		CreatedAt: job.CreatedAt.Format("2006-01-02T15:04:05Z"),
	})
}

// GetExportStatusResponse represents the response for export status
type GetExportStatusResponse struct {
	JobID       string      `json:"job_id"`
	Status      string      `json:"status"`
	Resource    string      `json:"resource"`
	Progress    JobProgress `json:"progress"`
	DownloadURL *string     `json:"download_url,omitempty"`
	ExpiresAt   *string     `json:"expires_at,omitempty"`
	CompletedAt *string     `json:"completed_at,omitempty"`
}

// GetExportStatus handles GET /v1/exports/:job_id
func (h *ExportHandler) GetExportStatus(c *gin.Context) {
	jobID, err := uuid.Parse(c.Param("job_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid job_id"})
		return
	}

	job, err := h.jobRepo.GetByID(c.Request.Context(), jobID)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get job")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get job"})
		return
	}
	if job == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}

	if job.Type != models.JobTypeExport {
		c.JSON(http.StatusBadRequest, gin.H{"error": "not an export job"})
		return
	}

	progress := job.CalculateProgress()

	response := GetExportStatusResponse{
		JobID:    job.ID.String(),
		Status:   string(job.Status),
		Resource: string(job.Resource),
		Progress: JobProgress{
			TotalRecords:      progress.TotalRecords,
			ProcessedRecords:  progress.ProcessedRecords,
			SuccessfulRecords: progress.SuccessfulRecords,
			FailedRecords:     progress.FailedRecords,
			Percentage:        progress.Percentage,
		},
	}

	if job.Status == models.JobStatusCompleted && job.FilePath != nil {
		downloadURL := fmt.Sprintf("/v1/exports/%s/download", job.ID.String())
		response.DownloadURL = &downloadURL

		// Set expiry (24 hours from completion)
		if job.CompletedAt != nil {
			expiresAt := job.CompletedAt.Add(24 * time.Hour).Format("2006-01-02T15:04:05Z")
			response.ExpiresAt = &expiresAt
		}
	}

	if job.CompletedAt != nil {
		completedAt := job.CompletedAt.Format("2006-01-02T15:04:05Z")
		response.CompletedAt = &completedAt
	}

	c.JSON(http.StatusOK, response)
}

// DownloadExport handles GET /v1/exports/:job_id/download
func (h *ExportHandler) DownloadExport(c *gin.Context) {
	jobID, err := uuid.Parse(c.Param("job_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid job_id"})
		return
	}

	filePath, err := h.exportSvc.GetExportFilePath(c.Request.Context(), jobID)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get export file")
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	// Check file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"error": "export file not found"})
		return
	}

	filename := filepath.Base(filePath)
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
	c.Header("Content-Type", "application/x-ndjson")
	c.File(filePath)
}

func (h *ExportHandler) parseFilters(c *gin.Context) *models.ExportFilters {
	filters := &models.ExportFilters{}

	if status := c.Query("status"); status != "" {
		filters.Status = &status
	}
	if role := c.Query("role"); role != "" {
		filters.Role = &role
	}
	if activeStr := c.Query("active"); activeStr != "" {
		active := strings.ToLower(activeStr) == "true"
		filters.Active = &active
	}
	if createdAfter := c.Query("created_after"); createdAfter != "" {
		if t, err := time.Parse(time.RFC3339, createdAfter); err == nil {
			filters.CreatedAfter = &t
		}
	}
	if createdBefore := c.Query("created_before"); createdBefore != "" {
		if t, err := time.Parse(time.RFC3339, createdBefore); err == nil {
			filters.CreatedBefore = &t
		}
	}
	if authorID := c.Query("author_id"); authorID != "" {
		if id, err := uuid.Parse(authorID); err == nil {
			filters.AuthorID = &id
		}
	}
	if articleID := c.Query("article_id"); articleID != "" {
		if id, err := uuid.Parse(articleID); err == nil {
			filters.ArticleID = &id
		}
	}
	if userID := c.Query("user_id"); userID != "" {
		if id, err := uuid.Parse(userID); err == nil {
			filters.UserID = &id
		}
	}

	return filters
}

func (h *ExportHandler) parseFiltersFromMap(m map[string]interface{}) *models.ExportFilters {
	if m == nil {
		return nil
	}

	filters := &models.ExportFilters{}

	if status, ok := m["status"].(string); ok {
		filters.Status = &status
	}
	if role, ok := m["role"].(string); ok {
		filters.Role = &role
	}
	if active, ok := m["active"].(bool); ok {
		filters.Active = &active
	}
	if createdAfter, ok := m["created_after"].(string); ok {
		if t, err := time.Parse(time.RFC3339, createdAfter); err == nil {
			filters.CreatedAfter = &t
		}
	}
	if createdBefore, ok := m["created_before"].(string); ok {
		if t, err := time.Parse(time.RFC3339, createdBefore); err == nil {
			filters.CreatedBefore = &t
		}
	}

	return filters
}

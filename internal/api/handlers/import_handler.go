package handlers

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rohit/bulk-import-export/internal/config"
	"github.com/rohit/bulk-import-export/internal/domain/errors"
	"github.com/rohit/bulk-import-export/internal/domain/models"
	"github.com/rohit/bulk-import-export/internal/repository/postgres"
	importservice "github.com/rohit/bulk-import-export/internal/service/import"
	"github.com/rohit/bulk-import-export/internal/worker"
	"github.com/rs/zerolog"
)

// ImportHandler handles import-related HTTP requests
type ImportHandler struct {
	importSvc       *importservice.Service
	jobRepo         *postgres.JobRepository
	idempotencyRepo *postgres.IdempotencyRepository
	workerPool      *worker.Pool
	logger          zerolog.Logger
	config          config.ImportConfig
}

// NewImportHandler creates a new import handler
func NewImportHandler(
	importSvc *importservice.Service,
	jobRepo *postgres.JobRepository,
	idempotencyRepo *postgres.IdempotencyRepository,
	workerPool *worker.Pool,
	logger zerolog.Logger,
	cfg config.ImportConfig,
) *ImportHandler {
	return &ImportHandler{
		importSvc:       importSvc,
		jobRepo:         jobRepo,
		idempotencyRepo: idempotencyRepo,
		workerPool:      workerPool,
		logger:          logger,
		config:          cfg,
	}
}

// CreateImportRequest represents the request body for creating an import
type CreateImportRequest struct {
	Resource string `json:"resource" binding:"required"`
	FileURL  string `json:"file_url,omitempty"`
}

// CreateImportResponse represents the response for creating an import
type CreateImportResponse struct {
	JobID     string `json:"job_id"`
	Status    string `json:"status"`
	Resource  string `json:"resource"`
	CreatedAt string `json:"created_at"`
	Links     Links  `json:"links"`
}

// Links represents HATEOAS links
type Links struct {
	Self   string `json:"self"`
	Errors string `json:"errors,omitempty"`
}

// CreateImport handles POST /v1/imports
func (h *ImportHandler) CreateImport(c *gin.Context) {
	// Check idempotency key
	idempotencyKey := c.GetHeader("Idempotency-Key")
	if idempotencyKey != "" {
		existingKey, err := h.idempotencyRepo.GetByKey(c.Request.Context(), idempotencyKey)
		if err != nil {
			h.logger.Error().Err(err).Msg("Failed to check idempotency key")
		}
		if existingKey != nil {
			// Return existing job
			job, err := h.jobRepo.GetByID(c.Request.Context(), existingKey.JobID)
			if err == nil && job != nil {
				c.JSON(http.StatusOK, CreateImportResponse{
					JobID:     job.ID.String(),
					Status:    string(job.Status),
					Resource:  string(job.Resource),
					CreatedAt: job.CreatedAt.Format("2006-01-02T15:04:05Z"),
					Links: Links{
						Self:   fmt.Sprintf("/v1/imports/%s", job.ID.String()),
						Errors: fmt.Sprintf("/v1/imports/%s/errors", job.ID.String()),
					},
				})
				return
			}
		}
	}

	// Get resource type from form or JSON
	var resource models.ResourceType
	var filePath string

	// Check if this is a multipart form upload
	contentType := c.ContentType()
	if contentType == "multipart/form-data" || c.Request.MultipartForm != nil {
		// Handle file upload
		resourceStr := c.PostForm("resource")
		if resourceStr == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "resource is required"})
			return
		}
		resource = models.ResourceType(resourceStr)

		// Validate resource type
		if resource != models.ResourceTypeUsers &&
			resource != models.ResourceTypeArticles &&
			resource != models.ResourceTypeComments {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid resource type"})
			return
		}

		// Get uploaded file
		file, header, err := c.Request.FormFile("file")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "file is required"})
			return
		}
		defer file.Close()

		// Check file size
		if header.Size > int64(h.config.MaxFileSizeMB)*1024*1024 {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("file too large, max %dMB", h.config.MaxFileSizeMB)})
			return
		}

		// Save file
		filePath, err = h.importSvc.SaveUploadedFile(file, header.Filename)
		if err != nil {
			h.logger.Error().Err(err).Msg("Failed to save uploaded file")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save file"})
			return
		}
	} else {
		// Handle JSON body with URL
		var req CreateImportRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		resource = models.ResourceType(req.Resource)
		if resource != models.ResourceTypeUsers &&
			resource != models.ResourceTypeArticles &&
			resource != models.ResourceTypeComments {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid resource type"})
			return
		}

		// Download file from URL
		if req.FileURL != "" {
			var err error
			filePath, err = h.importSvc.DownloadFileFromURL(req.FileURL)
			if err != nil {
				h.logger.Error().Err(err).Str("url", req.FileURL).Msg("Failed to download file from URL")
				c.JSON(http.StatusBadRequest, gin.H{"error": "failed to download file from URL: " + err.Error()})
				return
			}
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "file or file_url is required"})
			return
		}
	}

	// Create job
	job := &models.Job{
		ID:       uuid.New(),
		Type:     models.JobTypeImport,
		Resource: resource,
		Status:   models.JobStatusPending,
		FilePath: &filePath,
	}

	if idempotencyKey != "" {
		job.IdempotencyKey = &idempotencyKey
	}

	if err := h.jobRepo.Create(c.Request.Context(), job); err != nil {
		h.logger.Error().Err(err).Msg("Failed to create job")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create job"})
		return
	}

	// Store idempotency key
	if idempotencyKey != "" {
		idKey := &models.IdempotencyKey{
			Key:       idempotencyKey,
			JobID:     job.ID,
			ExpiresAt: job.CreatedAt.Add(config.IdempotencyTTL()),
		}
		if err := h.idempotencyRepo.Create(c.Request.Context(), idKey); err != nil {
			h.logger.Warn().Err(err).Msg("Failed to store idempotency key")
		}
	}

	// Submit job to worker pool
	source := worker.JobSource{FilePath: filePath}
	cleanup := func() {
		// Cleanup uploaded file after processing
		if filePath != "" && !strings.HasPrefix(filePath, "http") {
			os.Remove(filePath)
		}
	}
	h.workerPool.SubmitImportJob(job, source, cleanup)

	c.JSON(http.StatusAccepted, CreateImportResponse{
		JobID:     job.ID.String(),
		Status:    string(job.Status),
		Resource:  string(job.Resource),
		CreatedAt: job.CreatedAt.Format("2006-01-02T15:04:05Z"),
		Links: Links{
			Self:   fmt.Sprintf("/v1/imports/%s", job.ID.String()),
			Errors: fmt.Sprintf("/v1/imports/%s/errors", job.ID.String()),
		},
	})
}

// GetImportStatusResponse represents the response for getting import status
type GetImportStatusResponse struct {
	JobID           string      `json:"job_id"`
	Status          string      `json:"status"`
	Resource        string      `json:"resource"`
	Progress        JobProgress `json:"progress"`
	StartedAt       *string     `json:"started_at,omitempty"`
	CompletedAt     *string     `json:"completed_at,omitempty"`
	DurationSeconds float64     `json:"duration_seconds,omitempty"`
	RowsPerSecond   float64     `json:"rows_per_second,omitempty"`
	ErrorMessage    *string     `json:"error_message,omitempty"`
	Links           Links       `json:"links"`
}

// JobProgress represents job progress
type JobProgress struct {
	TotalRecords      int     `json:"total_records"`
	ProcessedRecords  int     `json:"processed_records"`
	SuccessfulRecords int     `json:"successful_records"`
	FailedRecords     int     `json:"failed_records"`
	Percentage        float64 `json:"percentage"`
}

// GetImportStatus handles GET /v1/imports/:job_id
func (h *ImportHandler) GetImportStatus(c *gin.Context) {
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

	progress := job.CalculateProgress()

	response := GetImportStatusResponse{
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
		ErrorMessage: job.ErrorMessage,
		Links: Links{
			Self:   fmt.Sprintf("/v1/imports/%s", job.ID.String()),
			Errors: fmt.Sprintf("/v1/imports/%s/errors", job.ID.String()),
		},
	}

	if job.StartedAt != nil {
		startedAt := job.StartedAt.Format("2006-01-02T15:04:05Z")
		response.StartedAt = &startedAt

		// Calculate duration
		endTime := job.CompletedAt
		if endTime == nil {
			now := c.Request.Context().Value("now")
			if now == nil {
				now = job.UpdatedAt
			}
		}
		if job.CompletedAt != nil {
			completedAt := job.CompletedAt.Format("2006-01-02T15:04:05Z")
			response.CompletedAt = &completedAt
			response.DurationSeconds = job.CompletedAt.Sub(*job.StartedAt).Seconds()
		} else {
			response.DurationSeconds = job.UpdatedAt.Sub(*job.StartedAt).Seconds()
		}

		// Calculate rows per second
		if response.DurationSeconds > 0 {
			response.RowsPerSecond = float64(job.ProcessedRecords) / response.DurationSeconds
		}
	}

	c.JSON(http.StatusOK, response)
}

// GetImportErrorsResponse represents the response for getting import errors
type GetImportErrorsResponse struct {
	JobID      string         `json:"job_id"`
	Errors     []JobErrorItem `json:"errors"`
	Pagination PaginationInfo `json:"pagination"`
}

// JobErrorItem represents an error item
type JobErrorItem struct {
	RowNumber        int     `json:"row_number"`
	RecordIdentifier *string `json:"record_identifier,omitempty"`
	FieldName        *string `json:"field_name,omitempty"`
	ErrorCode        string  `json:"error_code"`
	ErrorMessage     string  `json:"error_message"`
	RawData          *string `json:"raw_data,omitempty"`
}

// PaginationInfo represents pagination information
type PaginationInfo struct {
	Page        int   `json:"page"`
	PerPage     int   `json:"per_page"`
	TotalErrors int64 `json:"total_errors"`
	TotalPages  int   `json:"total_pages"`
}

// GetImportErrors handles GET /v1/imports/:job_id/errors
func (h *ImportHandler) GetImportErrors(c *gin.Context) {
	jobID, err := uuid.Parse(c.Param("job_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid job_id"})
		return
	}

	// Get pagination parameters
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	perPage, _ := strconv.Atoi(c.DefaultQuery("per_page", "100"))

	if page < 1 {
		page = 1
	}
	if perPage < 1 {
		perPage = 100
	}
	if perPage > 1000 {
		perPage = 1000
	}

	// Check job exists
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

	// Get errors
	jobErrors, total, err := h.importSvc.GetJobErrors(c.Request.Context(), jobID, page, perPage)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get job errors")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get errors"})
		return
	}

	// Convert to response format
	errorItems := make([]JobErrorItem, 0, len(jobErrors))
	for _, e := range jobErrors {
		errorItems = append(errorItems, JobErrorItem{
			RowNumber:        e.RowNumber,
			RecordIdentifier: e.RecordIdentifier,
			FieldName:        e.FieldName,
			ErrorCode:        e.ErrorCode,
			ErrorMessage:     e.ErrorMessage,
			RawData:          e.RawData,
		})
	}

	totalPages := int(total) / perPage
	if int(total)%perPage > 0 {
		totalPages++
	}

	c.JSON(http.StatusOK, GetImportErrorsResponse{
		JobID:  jobID.String(),
		Errors: errorItems,
		Pagination: PaginationInfo{
			Page:        page,
			PerPage:     perPage,
			TotalErrors: total,
			TotalPages:  totalPages,
		},
	})
}

// ErrorResponse creates a standard error response
func ErrorResponse(code, message string) *errors.AppError {
	return errors.NewAppError(code, message, http.StatusInternalServerError)
}

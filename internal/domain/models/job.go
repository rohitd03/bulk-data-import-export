package models

import (
	"time"

	"github.com/google/uuid"
)

// JobType represents the type of job
type JobType string

const (
	JobTypeImport JobType = "import"
	JobTypeExport JobType = "export"
)

// JobStatus represents the status of a job
type JobStatus string

const (
	JobStatusPending    JobStatus = "pending"
	JobStatusProcessing JobStatus = "processing"
	JobStatusCompleted  JobStatus = "completed"
	JobStatusFailed     JobStatus = "failed"
	JobStatusCancelled  JobStatus = "cancelled"
)

// ResourceType represents the resource being imported/exported
type ResourceType string

const (
	ResourceTypeUsers    ResourceType = "users"
	ResourceTypeArticles ResourceType = "articles"
	ResourceTypeComments ResourceType = "comments"
)

// Job represents an import or export job
type Job struct {
	ID                uuid.UUID    `json:"id" db:"id"`
	Type              JobType      `json:"type" db:"type"`
	Resource          ResourceType `json:"resource" db:"resource"`
	Status            JobStatus    `json:"status" db:"status"`
	IdempotencyKey    *string      `json:"idempotency_key,omitempty" db:"idempotency_key"`
	FilePath          *string      `json:"file_path,omitempty" db:"file_path"`
	FileURL           *string      `json:"file_url,omitempty" db:"file_url"`
	FileFormat        *string      `json:"file_format,omitempty" db:"file_format"`
	TotalRecords      int          `json:"total_records" db:"total_records"`
	ProcessedRecords  int          `json:"processed_records" db:"processed_records"`
	SuccessfulRecords int          `json:"successful_records" db:"successful_records"`
	FailedRecords     int          `json:"failed_records" db:"failed_records"`
	ErrorMessage      *string      `json:"error_message,omitempty" db:"error_message"`
	StartedAt         *time.Time   `json:"started_at,omitempty" db:"started_at"`
	CompletedAt       *time.Time   `json:"completed_at,omitempty" db:"completed_at"`
	CreatedAt         time.Time    `json:"created_at" db:"created_at"`
	UpdatedAt         time.Time    `json:"updated_at" db:"updated_at"`
}

// JobError represents an error that occurred during job processing
type JobError struct {
	ID               uuid.UUID `json:"id" db:"id"`
	JobID            uuid.UUID `json:"job_id" db:"job_id"`
	RowNumber        int       `json:"row_number" db:"row_number"`
	RecordIdentifier *string   `json:"record_identifier,omitempty" db:"record_identifier"`
	FieldName        *string   `json:"field_name,omitempty" db:"field_name"`
	ErrorCode        string    `json:"error_code" db:"error_code"`
	ErrorMessage     string    `json:"error_message" db:"error_message"`
	FieldValue       *string   `json:"field_value,omitempty" db:"field_value"`
	RawData          *string   `json:"raw_data,omitempty" db:"raw_data"`
	CreatedAt        time.Time `json:"created_at" db:"created_at"`
}

// IdempotencyKey represents an idempotency key record
type IdempotencyKey struct {
	Key          string    `json:"key" db:"idempotency_key"`
	JobID        uuid.UUID `json:"job_id" db:"job_id"`
	StatusCode   int       `json:"status_code" db:"status_code"`
	ResponseBody *string   `json:"response_body,omitempty" db:"response_body"`
	CreatedAt    time.Time `json:"created_at" db:"created_at"`
	ExpiresAt    time.Time `json:"expires_at" db:"expires_at"`
}

// JobProgress represents the progress of a job
type JobProgress struct {
	TotalRecords      int     `json:"total_records"`
	ProcessedRecords  int     `json:"processed_records"`
	SuccessfulRecords int     `json:"successful_records"`
	FailedRecords     int     `json:"failed_records"`
	Percentage        float64 `json:"percentage"`
}

// CalculateProgress calculates the job progress
func (j *Job) CalculateProgress() JobProgress {
	percentage := 0.0
	if j.TotalRecords > 0 {
		percentage = float64(j.ProcessedRecords) / float64(j.TotalRecords) * 100
	}

	// Don't show 100% until job is actually completed
	// This prevents showing 100% during the final database insert phase
	if percentage >= 100 && j.Status != JobStatusCompleted && j.Status != JobStatusFailed {
		percentage = 99.0
	}

	return JobProgress{
		TotalRecords:      j.TotalRecords,
		ProcessedRecords:  j.ProcessedRecords,
		SuccessfulRecords: j.SuccessfulRecords,
		FailedRecords:     j.FailedRecords,
		Percentage:        percentage,
	}
}

// CreateJobRequest represents a request to create a new job
type CreateJobRequest struct {
	Type           JobType      `json:"type"`
	Resource       ResourceType `json:"resource"`
	IdempotencyKey *string      `json:"idempotency_key,omitempty"`
	FilePath       *string      `json:"file_path,omitempty"`
	FileURL        *string      `json:"file_url,omitempty"`
}

// ExportFilters represents filters for export
type ExportFilters struct {
	Status        *string    `json:"status,omitempty"`
	Role          *string    `json:"role,omitempty"`
	Active        *bool      `json:"active,omitempty"`
	CreatedAfter  *time.Time `json:"created_after,omitempty"`
	CreatedBefore *time.Time `json:"created_before,omitempty"`
	AuthorID      *uuid.UUID `json:"author_id,omitempty"`
	ArticleID     *uuid.UUID `json:"article_id,omitempty"`
	UserID        *uuid.UUID `json:"user_id,omitempty"`
}

// ExportRequest represents a request to create an export job
type ExportRequest struct {
	Resource ResourceType   `json:"resource"`
	Format   string         `json:"format"` // ndjson, json
	Filters  *ExportFilters `json:"filters,omitempty"`
	Fields   []string       `json:"fields,omitempty"`
}

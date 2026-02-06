package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/rohit/bulk-import-export/internal/domain/models"
)

// JobRepository implements repository.JobRepository for PostgreSQL
type JobRepository struct {
	db *DB
}

// NewJobRepository creates a new JobRepository
func NewJobRepository(db *DB) *JobRepository {
	return &JobRepository{db: db}
}

// Create inserts a new job
func (r *JobRepository) Create(ctx context.Context, job *models.Job) error {
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now().UTC()
	}
	job.UpdatedAt = time.Now().UTC()

	query := `
		INSERT INTO jobs (
			id, type, resource, status, idempotency_key, file_path, file_url,
			total_records, processed_records, successful_records, failed_records,
			error_message, started_at, completed_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
	`
	_, err := r.db.ExecContext(ctx, query,
		job.ID, job.Type, job.Resource, job.Status, job.IdempotencyKey,
		job.FilePath, job.FileURL, job.TotalRecords, job.ProcessedRecords,
		job.SuccessfulRecords, job.FailedRecords, job.ErrorMessage,
		job.StartedAt, job.CompletedAt, job.CreatedAt, job.UpdatedAt,
	)
	return err
}

// GetByID retrieves a job by ID
func (r *JobRepository) GetByID(ctx context.Context, id uuid.UUID) (*models.Job, error) {
	var job models.Job
	err := r.db.GetContext(ctx, &job, "SELECT * FROM jobs WHERE id = $1", id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &job, err
}

// GetByIdempotencyKey retrieves a job by idempotency key
func (r *JobRepository) GetByIdempotencyKey(ctx context.Context, key string) (*models.Job, error) {
	var job models.Job
	err := r.db.GetContext(ctx, &job, "SELECT * FROM jobs WHERE idempotency_key = $1", key)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &job, err
}

// Update updates an existing job
func (r *JobRepository) Update(ctx context.Context, job *models.Job) error {
	job.UpdatedAt = time.Now().UTC()
	query := `
		UPDATE jobs SET
			status = $2, total_records = $3, processed_records = $4,
			successful_records = $5, failed_records = $6, error_message = $7,
			started_at = $8, completed_at = $9, updated_at = $10, file_path = $11
		WHERE id = $1
	`
	_, err := r.db.ExecContext(ctx, query,
		job.ID, job.Status, job.TotalRecords, job.ProcessedRecords,
		job.SuccessfulRecords, job.FailedRecords, job.ErrorMessage,
		job.StartedAt, job.CompletedAt, job.UpdatedAt, job.FilePath,
	)
	return err
}

// UpdateStatus updates the job status
func (r *JobRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status models.JobStatus) error {
	now := time.Now().UTC()
	query := `UPDATE jobs SET status = $2, updated_at = $3 WHERE id = $1`
	_, err := r.db.ExecContext(ctx, query, id, status, now)
	return err
}

// UpdateProgress updates the job progress counters
func (r *JobRepository) UpdateProgress(ctx context.Context, id uuid.UUID, processed, successful, failed int) error {
	now := time.Now().UTC()
	query := `
		UPDATE jobs SET
			processed_records = $2, successful_records = $3, failed_records = $4, updated_at = $5
		WHERE id = $1
	`
	_, err := r.db.ExecContext(ctx, query, id, processed, successful, failed, now)
	return err
}

// SetStarted sets the job as started
func (r *JobRepository) SetStarted(ctx context.Context, id uuid.UUID) error {
	now := time.Now().UTC()
	query := `
		UPDATE jobs SET status = $2, started_at = $3, updated_at = $3
		WHERE id = $1
	`
	_, err := r.db.ExecContext(ctx, query, id, models.JobStatusProcessing, now)
	return err
}

// SetCompleted sets the job as completed
func (r *JobRepository) SetCompleted(ctx context.Context, id uuid.UUID, successful, failed int) error {
	now := time.Now().UTC()
	query := `
		UPDATE jobs SET
			status = $2, successful_records = $3, failed_records = $4,
			completed_at = $5, updated_at = $5
		WHERE id = $1
	`
	_, err := r.db.ExecContext(ctx, query, id, models.JobStatusCompleted, successful, failed, now)
	return err
}

// SetFailed sets the job as failed
func (r *JobRepository) SetFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	now := time.Now().UTC()
	query := `
		UPDATE jobs SET
			status = $2, error_message = $3, completed_at = $4, updated_at = $4
		WHERE id = $1
	`
	_, err := r.db.ExecContext(ctx, query, id, models.JobStatusFailed, errorMessage, now)
	return err
}

// AddErrors adds job errors in batch
func (r *JobRepository) AddErrors(ctx context.Context, errors []*models.JobError) error {
	if len(errors) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO job_errors (id, job_id, row_number, record_identifier, field_name, error_code, error_message, raw_data, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, e := range errors {
		if e.ID == uuid.Nil {
			e.ID = uuid.New()
		}
		if e.CreatedAt.IsZero() {
			e.CreatedAt = time.Now().UTC()
		}
		_, err := stmt.ExecContext(ctx, e.ID, e.JobID, e.RowNumber, e.RecordIdentifier, e.FieldName, e.ErrorCode, e.ErrorMessage, e.RawData, e.CreatedAt)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetErrors retrieves job errors with pagination
func (r *JobRepository) GetErrors(ctx context.Context, jobID uuid.UUID, page, perPage int) ([]*models.JobError, int64, error) {
	if page < 1 {
		page = 1
	}
	if perPage < 1 {
		perPage = 100
	}
	if perPage > 1000 {
		perPage = 1000
	}

	offset := (page - 1) * perPage

	// Get total count
	var total int64
	err := r.db.GetContext(ctx, &total, "SELECT COUNT(*) FROM job_errors WHERE job_id = $1", jobID)
	if err != nil {
		return nil, 0, err
	}

	// Get errors
	var errors []*models.JobError
	query := `
		SELECT * FROM job_errors 
		WHERE job_id = $1 
		ORDER BY row_number ASC 
		LIMIT $2 OFFSET $3
	`
	err = r.db.SelectContext(ctx, &errors, query, jobID, perPage, offset)
	if err != nil {
		return nil, 0, err
	}

	return errors, total, nil
}

// GetPendingJobs retrieves pending jobs of a specific type
func (r *JobRepository) GetPendingJobs(ctx context.Context, jobType models.JobType, limit int) ([]*models.Job, error) {
	if limit < 1 {
		limit = 10
	}

	var jobs []*models.Job
	query := `
		SELECT * FROM jobs 
		WHERE type = $1 AND status = $2 
		ORDER BY created_at ASC 
		LIMIT $3
	`
	err := r.db.SelectContext(ctx, &jobs, query, jobType, models.JobStatusPending, limit)
	return jobs, err
}

// SetTotalRecords sets the total records count for a job
func (r *JobRepository) SetTotalRecords(ctx context.Context, id uuid.UUID, total int) error {
	now := time.Now().UTC()
	query := `UPDATE jobs SET total_records = $2, updated_at = $3 WHERE id = $1`
	_, err := r.db.ExecContext(ctx, query, id, total, now)
	return err
}

// IncrementProgress increments the processed records count
func (r *JobRepository) IncrementProgress(ctx context.Context, id uuid.UUID, successDelta, failedDelta int) error {
	now := time.Now().UTC()
	query := `
		UPDATE jobs SET
			processed_records = processed_records + $2 + $3,
			successful_records = successful_records + $2,
			failed_records = failed_records + $3,
			updated_at = $4
		WHERE id = $1
	`
	_, err := r.db.ExecContext(ctx, query, id, successDelta, failedDelta, now)
	return err
}

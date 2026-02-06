package repository

import (
	"context"

	"github.com/google/uuid"
	"github.com/rohit/bulk-import-export/internal/domain/models"
)

// UserRepository defines operations for user data access
type UserRepository interface {
	Create(ctx context.Context, user *models.User) error
	CreateBatch(ctx context.Context, users []*models.User) (int, error)
	GetByID(ctx context.Context, id uuid.UUID) (*models.User, error)
	GetByEmail(ctx context.Context, email string) (*models.User, error)
	GetAll(ctx context.Context, filters *models.ExportFilters) ([]*models.User, error)
	GetAllWithCursor(ctx context.Context, filters *models.ExportFilters, batchSize int, callback func([]*models.User) error) error
	Update(ctx context.Context, user *models.User) error
	Upsert(ctx context.Context, user *models.User) error
	UpsertBatch(ctx context.Context, users []*models.User) (int, int, error) // returns inserted, updated counts
	Delete(ctx context.Context, id uuid.UUID) error
	Exists(ctx context.Context, id uuid.UUID) (bool, error)
	EmailExists(ctx context.Context, email string, excludeID *uuid.UUID) (bool, error)
	Count(ctx context.Context, filters *models.ExportFilters) (int64, error)
}

// ArticleRepository defines operations for article data access
type ArticleRepository interface {
	Create(ctx context.Context, article *models.Article) error
	CreateBatch(ctx context.Context, articles []*models.Article) (int, error)
	GetByID(ctx context.Context, id uuid.UUID) (*models.Article, error)
	GetBySlug(ctx context.Context, slug string) (*models.Article, error)
	GetAll(ctx context.Context, filters *models.ExportFilters) ([]*models.Article, error)
	GetAllWithCursor(ctx context.Context, filters *models.ExportFilters, batchSize int, callback func([]*models.Article) error) error
	Update(ctx context.Context, article *models.Article) error
	Upsert(ctx context.Context, article *models.Article) error
	UpsertBatch(ctx context.Context, articles []*models.Article) (int, int, error)
	Delete(ctx context.Context, id uuid.UUID) error
	Exists(ctx context.Context, id uuid.UUID) (bool, error)
	SlugExists(ctx context.Context, slug string, excludeID *uuid.UUID) (bool, error)
	Count(ctx context.Context, filters *models.ExportFilters) (int64, error)
}

// CommentRepository defines operations for comment data access
type CommentRepository interface {
	Create(ctx context.Context, comment *models.Comment) error
	CreateBatch(ctx context.Context, comments []*models.Comment) (int, error)
	GetByID(ctx context.Context, id uuid.UUID) (*models.Comment, error)
	GetAll(ctx context.Context, filters *models.ExportFilters) ([]*models.Comment, error)
	GetAllWithCursor(ctx context.Context, filters *models.ExportFilters, batchSize int, callback func([]*models.Comment) error) error
	Update(ctx context.Context, comment *models.Comment) error
	Upsert(ctx context.Context, comment *models.Comment) error
	UpsertBatch(ctx context.Context, comments []*models.Comment) (int, int, error)
	Delete(ctx context.Context, id uuid.UUID) error
	Exists(ctx context.Context, id uuid.UUID) (bool, error)
	Count(ctx context.Context, filters *models.ExportFilters) (int64, error)
}

// JobRepository defines operations for job data access
type JobRepository interface {
	Create(ctx context.Context, job *models.Job) error
	GetByID(ctx context.Context, id uuid.UUID) (*models.Job, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*models.Job, error)
	Update(ctx context.Context, job *models.Job) error
	UpdateStatus(ctx context.Context, id uuid.UUID, status models.JobStatus) error
	UpdateProgress(ctx context.Context, id uuid.UUID, processed, successful, failed int) error
	SetStarted(ctx context.Context, id uuid.UUID) error
	SetCompleted(ctx context.Context, id uuid.UUID, successful, failed int) error
	SetFailed(ctx context.Context, id uuid.UUID, errorMessage string) error
	AddErrors(ctx context.Context, errors []*models.JobError) error
	GetErrors(ctx context.Context, jobID uuid.UUID, page, perPage int) ([]*models.JobError, int64, error)
	GetPendingJobs(ctx context.Context, jobType models.JobType, limit int) ([]*models.Job, error)
}

// StagingRepository defines operations for staging table data access
type StagingRepository interface {
	// User staging
	CreateStagingUsers(ctx context.Context, jobID uuid.UUID, users []StagingUser) error
	MarkDuplicateUsersInBatch(ctx context.Context, jobID uuid.UUID) (int, error)
	MarkDuplicateUsersAgainstExisting(ctx context.Context, jobID uuid.UUID) (int, error)
	GetValidStagingUsers(ctx context.Context, jobID uuid.UUID, batchSize int, callback func([]StagingUser) error) error
	UpdateStagingUserValidation(ctx context.Context, stagingID int64, isValid bool, errorMsg string) error
	CleanupStagingUsers(ctx context.Context, jobID uuid.UUID) error

	// Article staging
	CreateStagingArticles(ctx context.Context, jobID uuid.UUID, articles []StagingArticle) error
	MarkDuplicateArticlesInBatch(ctx context.Context, jobID uuid.UUID) (int, error)
	MarkDuplicateArticlesAgainstExisting(ctx context.Context, jobID uuid.UUID) (int, error)
	MarkInvalidAuthorFKArticles(ctx context.Context, jobID uuid.UUID) (int, error)
	GetValidStagingArticles(ctx context.Context, jobID uuid.UUID, batchSize int, callback func([]StagingArticle) error) error
	UpdateStagingArticleValidation(ctx context.Context, stagingID int64, isValid bool, errorMsg string) error
	CleanupStagingArticles(ctx context.Context, jobID uuid.UUID) error

	// Comment staging
	CreateStagingComments(ctx context.Context, jobID uuid.UUID, comments []StagingComment) error
	MarkDuplicateCommentsInBatch(ctx context.Context, jobID uuid.UUID) (int, error)
	MarkInvalidFKComments(ctx context.Context, jobID uuid.UUID) (int, error)
	GetValidStagingComments(ctx context.Context, jobID uuid.UUID, batchSize int, callback func([]StagingComment) error) error
	UpdateStagingCommentValidation(ctx context.Context, stagingID int64, isValid bool, errorMsg string) error
	CleanupStagingComments(ctx context.Context, jobID uuid.UUID) error
}

// StagingUser represents a user in the staging table
type StagingUser struct {
	StagingID       int64     `db:"staging_id"`
	JobID           uuid.UUID `db:"job_id"`
	RowNumber       int       `db:"row_number"`
	ID              *string   `db:"id"`
	Email           *string   `db:"email"`
	Name            *string   `db:"name"`
	Role            *string   `db:"role"`
	Active          *bool     `db:"active"`
	CreatedAt       *string   `db:"created_at"`
	UpdatedAt       *string   `db:"updated_at"`
	ValidationError *string   `db:"validation_error"`
	IsValid         bool      `db:"is_valid"`
	IsDuplicate     bool      `db:"is_duplicate"`
	Processed       bool      `db:"processed"`
}

// StagingArticle represents an article in the staging table
type StagingArticle struct {
	StagingID       int64     `db:"staging_id"`
	JobID           uuid.UUID `db:"job_id"`
	RowNumber       int       `db:"row_number"`
	ID              *string   `db:"id"`
	Slug            *string   `db:"slug"`
	Title           *string   `db:"title"`
	Body            *string   `db:"body"`
	AuthorID        *string   `db:"author_id"`
	Tags            *string   `db:"tags"`
	PublishedAt     *string   `db:"published_at"`
	Status          *string   `db:"status"`
	ValidationError *string   `db:"validation_error"`
	IsValid         bool      `db:"is_valid"`
	IsDuplicate     bool      `db:"is_duplicate"`
	Processed       bool      `db:"processed"`
}

// StagingComment represents a comment in the staging table
type StagingComment struct {
	StagingID       int64     `db:"staging_id"`
	JobID           uuid.UUID `db:"job_id"`
	RowNumber       int       `db:"row_number"`
	ID              *string   `db:"id"`
	ArticleID       *string   `db:"article_id"`
	UserID          *string   `db:"user_id"`
	Body            *string   `db:"body"`
	CreatedAt       *string   `db:"created_at"`
	ValidationError *string   `db:"validation_error"`
	IsValid         bool      `db:"is_valid"`
	IsDuplicate     bool      `db:"is_duplicate"`
	Processed       bool      `db:"processed"`
}

// IdempotencyRepository defines operations for idempotency key data access
type IdempotencyRepository interface {
	Create(ctx context.Context, key *models.IdempotencyKey) error
	GetByKey(ctx context.Context, key string) (*models.IdempotencyKey, error)
	Delete(ctx context.Context, key string) error
	CleanupExpired(ctx context.Context) (int64, error)
}

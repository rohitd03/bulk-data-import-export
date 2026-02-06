package importservice

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rohit/bulk-import-export/internal/config"
	"github.com/rohit/bulk-import-export/internal/domain/errors"
	"github.com/rohit/bulk-import-export/internal/domain/models"
	"github.com/rohit/bulk-import-export/internal/metrics"
	"github.com/rohit/bulk-import-export/internal/repository"
	"github.com/rohit/bulk-import-export/internal/repository/postgres"
	"github.com/rohit/bulk-import-export/internal/service/import/parsers"
	"github.com/rohit/bulk-import-export/internal/service/validation"
	"github.com/rs/zerolog"
)

// Service handles import operations
type Service struct {
	userRepo    *postgres.UserRepository
	articleRepo *postgres.ArticleRepository
	commentRepo *postgres.CommentRepository
	jobRepo     *postgres.JobRepository
	stagingRepo *postgres.StagingRepository
	metrics     *metrics.Collector
	logger      zerolog.Logger
	config      config.ImportConfig
	validator   *validation.Validator
	mu          sync.Mutex
}

// NewService creates a new import service
func NewService(
	userRepo *postgres.UserRepository,
	articleRepo *postgres.ArticleRepository,
	commentRepo *postgres.CommentRepository,
	jobRepo *postgres.JobRepository,
	stagingRepo *postgres.StagingRepository,
	metrics *metrics.Collector,
	logger zerolog.Logger,
	cfg config.ImportConfig,
) *Service {
	return &Service{
		userRepo:    userRepo,
		articleRepo: articleRepo,
		commentRepo: commentRepo,
		jobRepo:     jobRepo,
		stagingRepo: stagingRepo,
		metrics:     metrics,
		logger:      logger,
		config:      cfg,
		validator:   validation.NewValidator(),
	}
}

// ProcessJob processes an import job
func (s *Service) ProcessJob(ctx context.Context, job *models.Job) error {
	log := s.logger.With().
		Str("job_id", job.ID.String()).
		Str("resource", string(job.Resource)).
		Logger()

	log.Info().Msg("Starting import job processing")
	startTime := time.Now()

	// Update job status to processing
	if err := s.jobRepo.SetStarted(ctx, job.ID); err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	s.metrics.RecordImportJobStarted(string(job.Resource))

	// Open file
	filePath := ""
	if job.FilePath != nil {
		filePath = *job.FilePath
	}

	file, err := os.Open(filePath)
	if err != nil {
		s.handleJobFailure(ctx, job, log, "Failed to open file: "+err.Error())
		return err
	}
	defer file.Close()

	// Process based on resource type
	var processErr error
	switch job.Resource {
	case models.ResourceTypeUsers:
		processErr = s.processUsersImport(ctx, job, file, log)
	case models.ResourceTypeArticles:
		processErr = s.processArticlesImport(ctx, job, file, log)
	case models.ResourceTypeComments:
		processErr = s.processCommentsImport(ctx, job, file, log)
	default:
		processErr = fmt.Errorf("unknown resource type: %s", job.Resource)
	}

	duration := time.Since(startTime).Seconds()

	if processErr != nil {
		s.handleJobFailure(ctx, job, log, processErr.Error())
		s.metrics.RecordImportJobCompleted(string(job.Resource), "failed", duration)
		return processErr
	}

	// Get final counts
	finalJob, _ := s.jobRepo.GetByID(ctx, job.ID)
	if finalJob != nil {
		if err := s.jobRepo.SetCompleted(ctx, job.ID, finalJob.SuccessfulRecords, finalJob.FailedRecords); err != nil {
			log.Error().Err(err).Msg("Failed to set job as completed")
		}
	}

	s.metrics.RecordImportJobCompleted(string(job.Resource), "completed", duration)

	log.Info().
		Float64("duration_seconds", duration).
		Int("successful", finalJob.SuccessfulRecords).
		Int("failed", finalJob.FailedRecords).
		Msg("Import job completed successfully")

	return nil
}

// ProcessImport processes an import job with a provided file
func (s *Service) ProcessImport(ctx context.Context, file *os.File, job *models.Job, format string) error {
	log := s.logger.With().
		Str("job_id", job.ID.String()).
		Str("resource", string(job.Resource)).
		Str("format", format).
		Logger()

	log.Info().Msg("Starting import processing")
	startTime := time.Now()

	// Update job status to processing
	if err := s.jobRepo.SetStarted(ctx, job.ID); err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	s.metrics.RecordImportJobStarted(string(job.Resource))

	// Process based on resource type
	var processErr error
	switch job.Resource {
	case models.ResourceTypeUsers:
		processErr = s.processUsersImport(ctx, job, file, log)
	case models.ResourceTypeArticles:
		processErr = s.processArticlesImport(ctx, job, file, log)
	case models.ResourceTypeComments:
		processErr = s.processCommentsImport(ctx, job, file, log)
	default:
		processErr = fmt.Errorf("unknown resource type: %s", job.Resource)
	}

	duration := time.Since(startTime).Seconds()

	if processErr != nil {
		s.handleJobFailure(ctx, job, log, processErr.Error())
		s.metrics.RecordImportJobCompleted(string(job.Resource), "failed", duration)
		return processErr
	}

	// Get final counts
	finalJob, _ := s.jobRepo.GetByID(ctx, job.ID)
	if finalJob != nil {
		if err := s.jobRepo.SetCompleted(ctx, job.ID, finalJob.SuccessfulRecords, finalJob.FailedRecords); err != nil {
			log.Error().Err(err).Msg("Failed to set job as completed")
		}
		job.Status = models.JobStatusCompleted
		job.SuccessfulRecords = finalJob.SuccessfulRecords
		job.FailedRecords = finalJob.FailedRecords
	}

	s.metrics.RecordImportJobCompleted(string(job.Resource), "completed", duration)

	log.Info().
		Float64("duration_seconds", duration).
		Msg("Import processing completed successfully")

	return nil
}

func (s *Service) processUsersImport(ctx context.Context, job *models.Job, file *os.File, log zerolog.Logger) error {
	// Detect file format
	filePath := ""
	if job.FilePath != nil {
		filePath = *job.FilePath
	}
	format := parsers.DetectFormat(filePath)

	// First pass: parse and validate, store in staging
	stagingBatch := make([]repository.StagingUser, 0, s.config.BatchSize)
	var validationErrors []*errors.ValidationError
	totalRows := 0
	validRows := 0
	invalidRows := 0

	// Helper function to process a user record
	processUser := func(row int, user *models.UserImport, parseError bool) error {
		totalRows++

		stagingUser := repository.StagingUser{
			JobID:     job.ID,
			RowNumber: row,
		}

		if parseError || user == nil {
			stagingUser.IsValid = false
			errMsg := errors.ErrCodeFileParseError + ": Invalid record format"
			stagingUser.ValidationError = &errMsg
			invalidRows++
			stagingBatch = append(stagingBatch, stagingUser)
			return nil
		}

		// Validate user
		errs := s.validator.User.ValidateUserImport(row, user)

		if user.ID != "" {
			stagingUser.ID = &user.ID
		}
		if user.Email != "" {
			email := strings.ToLower(strings.TrimSpace(user.Email))
			stagingUser.Email = &email
		}
		if user.Name != "" {
			stagingUser.Name = &user.Name
		}
		if user.Role != "" {
			role := strings.ToLower(user.Role)
			stagingUser.Role = &role
		}
		if user.Active != "" {
			active := strings.ToLower(user.Active) == "true"
			stagingUser.Active = &active
		}
		if user.CreatedAt != "" {
			stagingUser.CreatedAt = &user.CreatedAt
		}
		if user.UpdatedAt != "" {
			stagingUser.UpdatedAt = &user.UpdatedAt
		}

		if len(errs) > 0 {
			stagingUser.IsValid = false
			errMsg := errs[0].Code + ": " + errs[0].Message
			stagingUser.ValidationError = &errMsg
			validationErrors = append(validationErrors, errs...)
			invalidRows++
		} else {
			stagingUser.IsValid = true
			validRows++
		}

		stagingBatch = append(stagingBatch, stagingUser)

		// Batch insert staging records
		if len(stagingBatch) >= s.config.BatchSize {
			if err := s.stagingRepo.CreateStagingUsers(ctx, job.ID, stagingBatch); err != nil {
				return fmt.Errorf("failed to create staging users: %w", err)
			}
			stagingBatch = stagingBatch[:0]

			// Update progress
			s.jobRepo.UpdateProgress(ctx, job.ID, totalRows, validRows, invalidRows)
		}

		return nil
	}

	var err error
	if format.IsNDJSON() {
		// Use NDJSON parser
		ndjsonParser := parsers.NewNDJSONParser(file)
		err = ndjsonParser.ParseUsers(func(row int, user *models.UserImport, rawJSON string) error {
			return processUser(row, user, user == nil)
		})
	} else {
		// Use CSV parser (default)
		csvParser, parserErr := parsers.NewCSVParser(file)
		if parserErr != nil {
			return fmt.Errorf("failed to create CSV parser: %w", parserErr)
		}
		err = csvParser.ParseUsers(func(row int, user *models.UserImport) error {
			return processUser(row, user, false)
		})
	}

	if err != nil {
		return err
	}

	// Insert remaining staging batch
	if len(stagingBatch) > 0 {
		if err := s.stagingRepo.CreateStagingUsers(ctx, job.ID, stagingBatch); err != nil {
			return fmt.Errorf("failed to create staging users: %w", err)
		}
	}

	// Set total records
	s.jobRepo.SetTotalRecords(ctx, job.ID, totalRows)

	log.Info().
		Int("total_rows", totalRows).
		Int("initial_valid", validRows).
		Int("initial_invalid", invalidRows).
		Msg("First pass complete, checking duplicates")

	// Mark duplicates within batch
	dupInBatch, err := s.stagingRepo.MarkDuplicateUsersInBatch(ctx, job.ID)
	if err != nil {
		return fmt.Errorf("failed to mark duplicates in batch: %w", err)
	}

	// Mark duplicates against existing data
	dupAgainstExisting, err := s.stagingRepo.MarkDuplicateUsersAgainstExisting(ctx, job.ID)
	if err != nil {
		return fmt.Errorf("failed to mark duplicates against existing: %w", err)
	}

	invalidRows += dupInBatch + dupAgainstExisting
	validRows -= dupInBatch + dupAgainstExisting

	log.Info().
		Int("duplicates_in_batch", dupInBatch).
		Int("duplicates_existing", dupAgainstExisting).
		Msg("Duplicate check complete")

	// Second pass: insert valid records to main table
	successfulInserts := 0
	err = s.stagingRepo.GetValidStagingUsers(ctx, job.ID, s.config.BatchSize, func(batch []repository.StagingUser) error {
		users := make([]*models.User, 0, len(batch))
		for _, su := range batch {
			if su.IsValid && !su.IsDuplicate {
				user, err := s.convertStagingToUser(&su)
				if err != nil {
					// Log error but continue
					log.Warn().Err(err).Int("row", su.RowNumber).Msg("Failed to convert staging user")
					continue
				}
				users = append(users, user)
			}
		}

		if len(users) > 0 {
			batchStart := time.Now()
			count, err := s.userRepo.CreateBatch(ctx, users)
			if err != nil {
				return fmt.Errorf("failed to insert users batch: %w", err)
			}
			successfulInserts += count
			s.metrics.RecordImportBatch(string(job.Resource), time.Since(batchStart).Seconds())
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Record validation errors
	s.recordValidationErrors(ctx, job.ID, string(job.Resource), validationErrors)

	// Cleanup staging table
	s.stagingRepo.CleanupStagingUsers(ctx, job.ID)

	// Update final counts
	s.jobRepo.UpdateProgress(ctx, job.ID, totalRows, successfulInserts, totalRows-successfulInserts)

	return nil
}

func (s *Service) processArticlesImport(ctx context.Context, job *models.Job, file *os.File, log zerolog.Logger) error {
	// Detect file format
	filePath := ""
	if job.FilePath != nil {
		filePath = *job.FilePath
	}
	format := parsers.DetectFormat(filePath)

	stagingBatch := make([]repository.StagingArticle, 0, s.config.BatchSize)
	var validationErrors []*errors.ValidationError
	totalRows := 0
	validRows := 0
	invalidRows := 0

	// Helper function to process an article record
	processArticle := func(row int, article *models.ArticleImport, parseError bool) error {
		totalRows++

		stagingArticle := repository.StagingArticle{
			JobID:     job.ID,
			RowNumber: row,
		}

		if parseError || article == nil {
			// Parse error
			stagingArticle.IsValid = false
			errMsg := errors.ErrCodeFileParseError + ": Invalid record format"
			stagingArticle.ValidationError = &errMsg
			invalidRows++
			stagingBatch = append(stagingBatch, stagingArticle)
			return nil
		}

		// Validate article
		errs := s.validator.Article.ValidateArticleImport(row, article)

		if article.ID != "" {
			stagingArticle.ID = &article.ID
		}
		if article.Slug != "" {
			slug := strings.ToLower(strings.TrimSpace(article.Slug))
			slug = strings.ReplaceAll(slug, " ", "-")
			stagingArticle.Slug = &slug
		}
		if article.Title != "" {
			stagingArticle.Title = &article.Title
		}
		if article.Body != "" {
			stagingArticle.Body = &article.Body
		}
		if article.AuthorID != "" {
			stagingArticle.AuthorID = &article.AuthorID
		}
		if article.Tags != nil {
			tagsJSON, _ := json.Marshal(article.Tags)
			tags := string(tagsJSON)
			stagingArticle.Tags = &tags
		}
		if article.PublishedAt != "" {
			stagingArticle.PublishedAt = &article.PublishedAt
		}
		if article.Status != "" {
			status := strings.ToLower(article.Status)
			stagingArticle.Status = &status
		}

		if len(errs) > 0 {
			stagingArticle.IsValid = false
			errMsg := errs[0].Code + ": " + errs[0].Message
			stagingArticle.ValidationError = &errMsg
			validationErrors = append(validationErrors, errs...)
			invalidRows++
		} else {
			stagingArticle.IsValid = true
			validRows++
		}

		stagingBatch = append(stagingBatch, stagingArticle)

		if len(stagingBatch) >= s.config.BatchSize {
			if err := s.stagingRepo.CreateStagingArticles(ctx, job.ID, stagingBatch); err != nil {
				return fmt.Errorf("failed to create staging articles: %w", err)
			}
			stagingBatch = stagingBatch[:0]
			s.jobRepo.UpdateProgress(ctx, job.ID, totalRows, validRows, invalidRows)
		}

		return nil
	}

	var err error
	if format.IsCSV() {
		// Use CSV parser
		csvParser, parserErr := parsers.NewCSVParser(file)
		if parserErr != nil {
			return fmt.Errorf("failed to create CSV parser: %w", parserErr)
		}
		err = csvParser.ParseArticles(func(row int, article *models.ArticleImport) error {
			return processArticle(row, article, false)
		})
	} else {
		// Use NDJSON parser (default for articles)
		ndjsonParser := parsers.NewNDJSONParser(file)
		err = ndjsonParser.ParseArticles(func(row int, article *models.ArticleImport, rawJSON string) error {
			return processArticle(row, article, article == nil)
		})
	}

	if err != nil {
		return err
	}

	// Insert remaining
	if len(stagingBatch) > 0 {
		if err := s.stagingRepo.CreateStagingArticles(ctx, job.ID, stagingBatch); err != nil {
			return fmt.Errorf("failed to create staging articles: %w", err)
		}
	}

	s.jobRepo.SetTotalRecords(ctx, job.ID, totalRows)

	// Mark duplicates
	dupInBatch, _ := s.stagingRepo.MarkDuplicateArticlesInBatch(ctx, job.ID)
	dupAgainstExisting, _ := s.stagingRepo.MarkDuplicateArticlesAgainstExisting(ctx, job.ID)

	// Validate foreign keys (author_id must exist in users table)
	invalidFKs, _ := s.stagingRepo.MarkInvalidAuthorFKArticles(ctx, job.ID)

	log.Info().
		Int("total_rows", totalRows).
		Int("duplicates_in_batch", dupInBatch).
		Int("duplicates_existing", dupAgainstExisting).
		Int("invalid_author_fks", invalidFKs).
		Msg("Validation and deduplication complete")

	// Insert valid records
	successfulInserts := 0
	err = s.stagingRepo.GetValidStagingArticles(ctx, job.ID, s.config.BatchSize, func(batch []repository.StagingArticle) error {
		articles := make([]*models.Article, 0, len(batch))
		for _, sa := range batch {
			if sa.IsValid && !sa.IsDuplicate {
				article, err := s.convertStagingToArticle(&sa)
				if err != nil {
					continue
				}
				articles = append(articles, article)
			}
		}

		if len(articles) > 0 {
			batchStart := time.Now()
			count, err := s.articleRepo.CreateBatch(ctx, articles)
			if err != nil {
				return err
			}
			successfulInserts += count
			s.metrics.RecordImportBatch(string(job.Resource), time.Since(batchStart).Seconds())
		}

		return nil
	})

	if err != nil {
		return err
	}

	s.recordValidationErrors(ctx, job.ID, string(job.Resource), validationErrors)
	s.stagingRepo.CleanupStagingArticles(ctx, job.ID)
	s.jobRepo.UpdateProgress(ctx, job.ID, totalRows, successfulInserts, totalRows-successfulInserts)

	return nil
}

func (s *Service) processCommentsImport(ctx context.Context, job *models.Job, file *os.File, log zerolog.Logger) error {
	// Detect file format
	filePath := ""
	if job.FilePath != nil {
		filePath = *job.FilePath
	}
	format := parsers.DetectFormat(filePath)

	stagingBatch := make([]repository.StagingComment, 0, s.config.BatchSize)
	var validationErrors []*errors.ValidationError
	totalRows := 0
	validRows := 0
	invalidRows := 0

	// Helper function to process a comment record
	processComment := func(row int, comment *models.CommentImport, parseError bool) error {
		totalRows++

		stagingComment := repository.StagingComment{
			JobID:     job.ID,
			RowNumber: row,
		}

		if parseError || comment == nil {
			stagingComment.IsValid = false
			errMsg := errors.ErrCodeFileParseError + ": Invalid record format"
			stagingComment.ValidationError = &errMsg
			invalidRows++
			stagingBatch = append(stagingBatch, stagingComment)
			return nil
		}

		errs := s.validator.Comment.ValidateCommentImport(row, comment)

		if comment.ID != "" {
			stagingComment.ID = &comment.ID
		}
		if comment.ArticleID != "" {
			stagingComment.ArticleID = &comment.ArticleID
		}
		if comment.UserID != "" {
			stagingComment.UserID = &comment.UserID
		}
		if comment.Body != "" {
			stagingComment.Body = &comment.Body
		}
		if comment.CreatedAt != "" {
			stagingComment.CreatedAt = &comment.CreatedAt
		}

		if len(errs) > 0 {
			stagingComment.IsValid = false
			errMsg := errs[0].Code + ": " + errs[0].Message
			stagingComment.ValidationError = &errMsg
			validationErrors = append(validationErrors, errs...)
			invalidRows++
		} else {
			stagingComment.IsValid = true
			validRows++
		}

		stagingBatch = append(stagingBatch, stagingComment)

		if len(stagingBatch) >= s.config.BatchSize {
			if err := s.stagingRepo.CreateStagingComments(ctx, job.ID, stagingBatch); err != nil {
				return err
			}
			stagingBatch = stagingBatch[:0]
			s.jobRepo.UpdateProgress(ctx, job.ID, totalRows, validRows, invalidRows)
		}

		return nil
	}

	var err error
	if format.IsCSV() {
		// Use CSV parser
		csvParser, parserErr := parsers.NewCSVParser(file)
		if parserErr != nil {
			return fmt.Errorf("failed to create CSV parser: %w", parserErr)
		}
		err = csvParser.ParseComments(func(row int, comment *models.CommentImport) error {
			return processComment(row, comment, false)
		})
	} else {
		// Use NDJSON parser (default for comments)
		ndjsonParser := parsers.NewNDJSONParser(file)
		err = ndjsonParser.ParseComments(func(row int, comment *models.CommentImport, rawJSON string) error {
			return processComment(row, comment, comment == nil)
		})
	}

	if err != nil {
		return err
	}

	if len(stagingBatch) > 0 {
		if err := s.stagingRepo.CreateStagingComments(ctx, job.ID, stagingBatch); err != nil {
			return err
		}
	}

	s.jobRepo.SetTotalRecords(ctx, job.ID, totalRows)

	dupInBatch, _ := s.stagingRepo.MarkDuplicateCommentsInBatch(ctx, job.ID)

	// Validate foreign keys (article_id and user_id must exist)
	invalidFKs, _ := s.stagingRepo.MarkInvalidFKComments(ctx, job.ID)

	log.Info().
		Int("total_rows", totalRows).
		Int("duplicates_in_batch", dupInBatch).
		Int("invalid_fks", invalidFKs).
		Msg("Validation and deduplication complete")

	// Insert valid records
	successfulInserts := 0
	err = s.stagingRepo.GetValidStagingComments(ctx, job.ID, s.config.BatchSize, func(batch []repository.StagingComment) error {
		comments := make([]*models.Comment, 0, len(batch))
		for _, sc := range batch {
			if sc.IsValid && !sc.IsDuplicate {
				comment, err := s.convertStagingToComment(&sc)
				if err != nil {
					continue
				}
				comments = append(comments, comment)
			}
		}

		if len(comments) > 0 {
			batchStart := time.Now()
			count, err := s.commentRepo.CreateBatch(ctx, comments)
			if err != nil {
				return err
			}
			successfulInserts += count
			s.metrics.RecordImportBatch(string(job.Resource), time.Since(batchStart).Seconds())
		}

		return nil
	})

	if err != nil {
		return err
	}

	s.recordValidationErrors(ctx, job.ID, string(job.Resource), validationErrors)
	s.stagingRepo.CleanupStagingComments(ctx, job.ID)
	s.jobRepo.UpdateProgress(ctx, job.ID, totalRows, successfulInserts, totalRows-successfulInserts)

	return nil
}

func (s *Service) handleJobFailure(ctx context.Context, job *models.Job, log zerolog.Logger, errMsg string) {
	log.Error().Str("error", errMsg).Msg("Import job failed")
	s.jobRepo.SetFailed(ctx, job.ID, errMsg)
}

func (s *Service) recordValidationErrors(ctx context.Context, jobID uuid.UUID, resource string, errs []*errors.ValidationError) {
	if len(errs) == 0 {
		return
	}

	jobErrors := make([]*models.JobError, 0, len(errs))
	for _, e := range errs {
		jobErrors = append(jobErrors, &models.JobError{
			JobID:            jobID,
			RowNumber:        e.RowNumber,
			RecordIdentifier: &e.RecordIdentifier,
			FieldName:        &e.FieldName,
			ErrorCode:        e.Code,
			ErrorMessage:     e.Message,
		})

		s.metrics.RecordImportError(resource, e.Code)
	}

	// Batch insert errors
	for i := 0; i < len(jobErrors); i += s.config.BatchSize {
		end := i + s.config.BatchSize
		if end > len(jobErrors) {
			end = len(jobErrors)
		}
		s.jobRepo.AddErrors(ctx, jobErrors[i:end])
	}
}

func (s *Service) convertStagingToUser(su *repository.StagingUser) (*models.User, error) {
	user := &models.User{
		Active: true,
	}

	if su.ID != nil && *su.ID != "" {
		id, err := uuid.Parse(*su.ID)
		if err != nil {
			return nil, err
		}
		user.ID = id
	} else {
		user.ID = uuid.New()
	}

	if su.Email != nil {
		user.Email = *su.Email
	}
	if su.Name != nil {
		user.Name = *su.Name
	}
	if su.Role != nil {
		user.Role = *su.Role
	}
	if su.Active != nil {
		user.Active = *su.Active
	}
	if su.CreatedAt != nil {
		t, err := time.Parse(time.RFC3339, *su.CreatedAt)
		if err == nil {
			user.CreatedAt = t
		} else {
			user.CreatedAt = time.Now().UTC()
		}
	} else {
		user.CreatedAt = time.Now().UTC()
	}
	if su.UpdatedAt != nil {
		t, err := time.Parse(time.RFC3339, *su.UpdatedAt)
		if err == nil {
			user.UpdatedAt = t
		} else {
			user.UpdatedAt = time.Now().UTC()
		}
	} else {
		user.UpdatedAt = time.Now().UTC()
	}

	return user, nil
}

func (s *Service) convertStagingToArticle(sa *repository.StagingArticle) (*models.Article, error) {
	article := &models.Article{
		Tags: json.RawMessage("[]"),
	}

	if sa.ID != nil && *sa.ID != "" {
		id, err := uuid.Parse(*sa.ID)
		if err != nil {
			return nil, err
		}
		article.ID = id
	} else {
		article.ID = uuid.New()
	}

	if sa.Slug != nil {
		article.Slug = *sa.Slug
	}
	if sa.Title != nil {
		article.Title = *sa.Title
	}
	if sa.Body != nil {
		article.Body = *sa.Body
	}
	if sa.AuthorID != nil {
		authorID, err := uuid.Parse(*sa.AuthorID)
		if err != nil {
			return nil, err
		}
		article.AuthorID = authorID
	}
	if sa.Tags != nil {
		article.Tags = json.RawMessage(*sa.Tags)
	}
	if sa.Status != nil {
		article.Status = *sa.Status
	}
	if sa.PublishedAt != nil {
		t, err := time.Parse(time.RFC3339, *sa.PublishedAt)
		if err == nil {
			article.PublishedAt = &t
		}
	}

	article.CreatedAt = time.Now().UTC()
	article.UpdatedAt = time.Now().UTC()

	return article, nil
}

func (s *Service) convertStagingToComment(sc *repository.StagingComment) (*models.Comment, error) {
	comment := &models.Comment{}

	if sc.ID != nil && *sc.ID != "" {
		id, err := uuid.Parse(*sc.ID)
		if err != nil {
			return nil, err
		}
		comment.ID = id
	} else {
		comment.ID = uuid.New()
	}

	if sc.ArticleID != nil {
		articleID, err := uuid.Parse(*sc.ArticleID)
		if err != nil {
			return nil, err
		}
		comment.ArticleID = articleID
	}
	if sc.UserID != nil {
		userID, err := uuid.Parse(*sc.UserID)
		if err != nil {
			return nil, err
		}
		comment.UserID = userID
	}
	if sc.Body != nil {
		comment.Body = *sc.Body
	}
	if sc.CreatedAt != nil {
		t, err := time.Parse(time.RFC3339, *sc.CreatedAt)
		if err == nil {
			comment.CreatedAt = t
		} else {
			comment.CreatedAt = time.Now().UTC()
		}
	} else {
		comment.CreatedAt = time.Now().UTC()
	}

	return comment, nil
}

// SaveUploadedFile saves an uploaded file to disk
func (s *Service) SaveUploadedFile(file io.Reader, filename string) (string, error) {
	// Create unique filename
	ext := filepath.Ext(filename)
	uniqueFilename := fmt.Sprintf("%s_%d%s", strings.TrimSuffix(filename, ext), time.Now().UnixNano(), ext)
	filePath := filepath.Join(s.config.UploadPath, uniqueFilename)

	// Create file
	dst, err := os.Create(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to create file: %w", err)
	}
	defer dst.Close()

	// Copy content
	if _, err := io.Copy(dst, file); err != nil {
		return "", fmt.Errorf("failed to save file: %w", err)
	}

	return filePath, nil
}

// DownloadFileFromURL downloads a file from a remote URL and saves it locally
func (s *Service) DownloadFileFromURL(fileURL string) (string, error) {
	// Validate URL
	parsedURL, err := url.Parse(fileURL)
	if err != nil {
		return "", fmt.Errorf("invalid URL: %w", err)
	}

	// Only allow http and https
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return "", fmt.Errorf("URL scheme must be http or https")
	}

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 5 * time.Minute, // Allow up to 5 minutes for large files
	}

	// Make request
	resp, err := client.Get(fileURL)
	if err != nil {
		return "", fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()

	// Check status
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to download file: server returned %d", resp.StatusCode)
	}

	// Extract filename from URL or Content-Disposition header
	filename := filepath.Base(parsedURL.Path)
	if filename == "" || filename == "." || filename == "/" {
		filename = "downloaded_file"
	}

	// Check Content-Disposition header for filename
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			if fn, ok := params["filename"]; ok {
				filename = fn
			}
		}
	}

	// Limit download size (default 500MB)
	maxSize := int64(500 * 1024 * 1024)
	limitedReader := io.LimitReader(resp.Body, maxSize)

	// Save file using existing method
	return s.SaveUploadedFile(limitedReader, filename)
}

// GetJobErrors retrieves errors for a job
func (s *Service) GetJobErrors(ctx context.Context, jobID uuid.UUID, page, perPage int) ([]*models.JobError, int64, error) {
	return s.jobRepo.GetErrors(ctx, jobID, page, perPage)
}

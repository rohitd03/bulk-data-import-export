package exportservice

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/rohit/bulk-import-export/internal/config"
	"github.com/rohit/bulk-import-export/internal/domain/models"
	"github.com/rohit/bulk-import-export/internal/metrics"
	"github.com/rohit/bulk-import-export/internal/repository/postgres"
	"github.com/rs/zerolog"
)

// Service handles export operations
type Service struct {
	userRepo    *postgres.UserRepository
	articleRepo *postgres.ArticleRepository
	commentRepo *postgres.CommentRepository
	jobRepo     *postgres.JobRepository
	metrics     *metrics.Collector
	logger      zerolog.Logger
	config      config.ExportConfig
}

// NewService creates a new export service
func NewService(
	userRepo *postgres.UserRepository,
	articleRepo *postgres.ArticleRepository,
	commentRepo *postgres.CommentRepository,
	jobRepo *postgres.JobRepository,
	metrics *metrics.Collector,
	logger zerolog.Logger,
	cfg config.ExportConfig,
) *Service {
	return &Service{
		userRepo:    userRepo,
		articleRepo: articleRepo,
		commentRepo: commentRepo,
		jobRepo:     jobRepo,
		metrics:     metrics,
		logger:      logger,
		config:      cfg,
	}
}

// StreamUsers streams users to a writer in NDJSON format
func (s *Service) StreamUsers(ctx context.Context, w io.Writer, filters *models.ExportFilters) error {
	startTime := time.Now()
	recordCount := 0

	s.metrics.RecordExportJobStarted("users")

	err := s.userRepo.GetAllWithCursor(ctx, filters, s.config.BatchSize, func(users []*models.User) error {
		for _, user := range users {
			data, err := json.Marshal(user)
			if err != nil {
				s.logger.Warn().Err(err).Str("user_id", user.ID.String()).Msg("Failed to marshal user")
				continue
			}
			if _, err := w.Write(append(data, '\n')); err != nil {
				return fmt.Errorf("failed to write user data: %w", err)
			}
			recordCount++
		}

		// Update metrics
		duration := time.Since(startTime).Seconds()
		if duration > 0 {
			s.metrics.RecordExportRate("users", "", float64(recordCount)/duration)
		}

		return nil
	})

	duration := time.Since(startTime).Seconds()
	status := "completed"
	if err != nil {
		status = "failed"
	}

	s.metrics.RecordExportJobCompleted("users", status, duration)
	s.metrics.RecordExportRecords("users", recordCount)

	s.logger.Info().
		Int("records", recordCount).
		Float64("duration_seconds", duration).
		Msg("User export completed")

	return err
}

// StreamArticles streams articles to a writer in NDJSON format
func (s *Service) StreamArticles(ctx context.Context, w io.Writer, filters *models.ExportFilters) error {
	startTime := time.Now()
	recordCount := 0

	s.metrics.RecordExportJobStarted("articles")

	err := s.articleRepo.GetAllWithCursor(ctx, filters, s.config.BatchSize, func(articles []*models.Article) error {
		for _, article := range articles {
			data, err := json.Marshal(article)
			if err != nil {
				s.logger.Warn().Err(err).Str("article_id", article.ID.String()).Msg("Failed to marshal article")
				continue
			}
			if _, err := w.Write(append(data, '\n')); err != nil {
				return fmt.Errorf("failed to write article data: %w", err)
			}
			recordCount++
		}

		duration := time.Since(startTime).Seconds()
		if duration > 0 {
			s.metrics.RecordExportRate("articles", "", float64(recordCount)/duration)
		}

		return nil
	})

	duration := time.Since(startTime).Seconds()
	status := "completed"
	if err != nil {
		status = "failed"
	}

	s.metrics.RecordExportJobCompleted("articles", status, duration)
	s.metrics.RecordExportRecords("articles", recordCount)

	s.logger.Info().
		Int("records", recordCount).
		Float64("duration_seconds", duration).
		Msg("Article export completed")

	return err
}

// StreamComments streams comments to a writer in NDJSON format
func (s *Service) StreamComments(ctx context.Context, w io.Writer, filters *models.ExportFilters) error {
	startTime := time.Now()
	recordCount := 0

	s.metrics.RecordExportJobStarted("comments")

	err := s.commentRepo.GetAllWithCursor(ctx, filters, s.config.BatchSize, func(comments []*models.Comment) error {
		for _, comment := range comments {
			data, err := json.Marshal(comment)
			if err != nil {
				s.logger.Warn().Err(err).Str("comment_id", comment.ID.String()).Msg("Failed to marshal comment")
				continue
			}
			if _, err := w.Write(append(data, '\n')); err != nil {
				return fmt.Errorf("failed to write comment data: %w", err)
			}
			recordCount++
		}

		duration := time.Since(startTime).Seconds()
		if duration > 0 {
			s.metrics.RecordExportRate("comments", "", float64(recordCount)/duration)
		}

		return nil
	})

	duration := time.Since(startTime).Seconds()
	status := "completed"
	if err != nil {
		status = "failed"
	}

	s.metrics.RecordExportJobCompleted("comments", status, duration)
	s.metrics.RecordExportRecords("comments", recordCount)

	s.logger.Info().
		Int("records", recordCount).
		Float64("duration_seconds", duration).
		Msg("Comment export completed")

	return err
}

// ProcessAsyncExport processes an async export job
func (s *Service) ProcessAsyncExport(ctx context.Context, job *models.Job, filters *models.ExportFilters) error {
	log := s.logger.With().
		Str("job_id", job.ID.String()).
		Str("resource", string(job.Resource)).
		Logger()

	log.Info().Msg("Starting async export job")
	startTime := time.Now()

	// Update job status
	if err := s.jobRepo.SetStarted(ctx, job.ID); err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	// Create output file
	filename := fmt.Sprintf("%s_%s_%d.ndjson", job.Resource, job.ID.String()[:8], time.Now().Unix())
	filePath := filepath.Join(s.config.OutputPath, filename)

	file, err := os.Create(filePath)
	if err != nil {
		s.handleJobFailure(ctx, job.ID, log, "Failed to create output file: "+err.Error())
		return err
	}
	defer file.Close()

	// Stream data to file
	var exportErr error
	switch job.Resource {
	case models.ResourceTypeUsers:
		exportErr = s.StreamUsers(ctx, file, filters)
	case models.ResourceTypeArticles:
		exportErr = s.StreamArticles(ctx, file, filters)
	case models.ResourceTypeComments:
		exportErr = s.StreamComments(ctx, file, filters)
	default:
		exportErr = fmt.Errorf("unknown resource type: %s", job.Resource)
	}

	duration := time.Since(startTime).Seconds()

	if exportErr != nil {
		s.handleJobFailure(ctx, job.ID, log, exportErr.Error())
		return exportErr
	}

	// Get file stats
	fileInfo, _ := file.Stat()
	recordCount := 0
	if fileInfo != nil {
		// Estimate records (rough count by file size / avg record size)
		recordCount = int(fileInfo.Size() / 200) // Approximate
	}

	// Update job with file path
	job.FilePath = &filePath
	job.TotalRecords = recordCount
	job.ProcessedRecords = recordCount
	job.SuccessfulRecords = recordCount
	if err := s.jobRepo.Update(ctx, job); err != nil {
		log.Error().Err(err).Msg("Failed to update job with file path")
	}

	if err := s.jobRepo.SetCompleted(ctx, job.ID, recordCount, 0); err != nil {
		log.Error().Err(err).Msg("Failed to set job as completed")
	}

	log.Info().
		Float64("duration_seconds", duration).
		Str("file_path", filePath).
		Int("records", recordCount).
		Msg("Async export completed")

	return nil
}

func (s *Service) handleJobFailure(ctx context.Context, jobID uuid.UUID, log zerolog.Logger, errMsg string) {
	log.Error().Str("error", errMsg).Msg("Export job failed")
	s.jobRepo.SetFailed(ctx, jobID, errMsg)
}

// GetExportFilePath returns the file path for a completed export job
func (s *Service) GetExportFilePath(ctx context.Context, jobID uuid.UUID) (string, error) {
	job, err := s.jobRepo.GetByID(ctx, jobID)
	if err != nil {
		return "", err
	}
	if job == nil {
		return "", fmt.Errorf("job not found")
	}
	if job.Status != models.JobStatusCompleted {
		return "", fmt.Errorf("job not completed")
	}
	if job.FilePath == nil {
		return "", fmt.Errorf("export file not available")
	}
	return *job.FilePath, nil
}

// StreamJSON streams data as a JSON array (not NDJSON)
func (s *Service) StreamJSON(ctx context.Context, w io.Writer, resource models.ResourceType, filters *models.ExportFilters) error {
	// Write opening bracket
	if _, err := w.Write([]byte("[\n")); err != nil {
		return err
	}

	first := true

	writeRecord := func(data []byte) error {
		if !first {
			if _, err := w.Write([]byte(",\n")); err != nil {
				return err
			}
		}
		first = false
		if _, err := w.Write(data); err != nil {
			return err
		}
		return nil
	}

	var err error
	switch resource {
	case models.ResourceTypeUsers:
		err = s.userRepo.GetAllWithCursor(ctx, filters, s.config.BatchSize, func(users []*models.User) error {
			for _, user := range users {
				data, e := json.Marshal(user)
				if e != nil {
					continue
				}
				if e := writeRecord(data); e != nil {
					return e
				}
			}
			return nil
		})
	case models.ResourceTypeArticles:
		err = s.articleRepo.GetAllWithCursor(ctx, filters, s.config.BatchSize, func(articles []*models.Article) error {
			for _, article := range articles {
				data, e := json.Marshal(article)
				if e != nil {
					continue
				}
				if e := writeRecord(data); e != nil {
					return e
				}
			}
			return nil
		})
	case models.ResourceTypeComments:
		err = s.commentRepo.GetAllWithCursor(ctx, filters, s.config.BatchSize, func(comments []*models.Comment) error {
			for _, comment := range comments {
				data, e := json.Marshal(comment)
				if e != nil {
					continue
				}
				if e := writeRecord(data); e != nil {
					return e
				}
			}
			return nil
		})
	}

	if err != nil {
		return err
	}

	// Write closing bracket
	if _, err := w.Write([]byte("\n]")); err != nil {
		return err
	}

	return nil
}

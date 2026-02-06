package worker

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rohit/bulk-import-export/internal/config"
	"github.com/rohit/bulk-import-export/internal/domain/models"
	"github.com/rohit/bulk-import-export/internal/metrics"
	"github.com/rohit/bulk-import-export/internal/repository/postgres"
	exportservice "github.com/rohit/bulk-import-export/internal/service/export"
	importservice "github.com/rohit/bulk-import-export/internal/service/import"
	"github.com/rs/zerolog"
)

// ImportJob represents an import job to be processed
type ImportJob struct {
	Job     *models.Job
	Source  JobSource
	Cleanup func()
}

// JobSource represents the source of import data
type JobSource struct {
	FilePath string
	URL      string
}

// ExportJob represents an export job to be processed
type ExportJob struct {
	Job     *models.Job
	Filters *models.ExportFilters
}

// Pool manages a pool of workers for processing jobs
type Pool struct {
	importChan chan *ImportJob
	exportChan chan *ExportJob
	wg         sync.WaitGroup
	quit       chan struct{}
	logger     zerolog.Logger
	importSvc  *importservice.Service
	exportSvc  *exportservice.Service
	jobRepo    *postgres.JobRepository
	metrics    *metrics.Collector
	cfg        config.WorkerConfig
	mu         sync.Mutex
	running    bool
}

// NewPool creates a new worker pool
func NewPool(
	importSvc *importservice.Service,
	exportSvc *exportservice.Service,
	jobRepo *postgres.JobRepository,
	metricsCollector *metrics.Collector,
	logger zerolog.Logger,
	cfg config.WorkerConfig,
) *Pool {
	return &Pool{
		importChan: make(chan *ImportJob, cfg.QueueSize),
		exportChan: make(chan *ExportJob, cfg.QueueSize),
		quit:       make(chan struct{}),
		logger:     logger,
		importSvc:  importSvc,
		exportSvc:  exportSvc,
		jobRepo:    jobRepo,
		metrics:    metricsCollector,
		cfg:        cfg,
	}
}

// Start starts the worker pool
func (p *Pool) Start(ctx context.Context) {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return
	}
	p.running = true
	p.mu.Unlock()

	// Start import workers
	for i := 0; i < p.cfg.ImportWorkers; i++ {
		p.wg.Add(1)
		go p.importWorker(ctx, i)
	}

	// Start export workers
	for i := 0; i < p.cfg.ExportWorkers; i++ {
		p.wg.Add(1)
		go p.exportWorker(ctx, i)
	}

	p.logger.Info().
		Int("import_workers", p.cfg.ImportWorkers).
		Int("export_workers", p.cfg.ExportWorkers).
		Int("queue_size", p.cfg.QueueSize).
		Msg("Worker pool started")
}

// Stop gracefully stops the worker pool
func (p *Pool) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	p.mu.Unlock()

	close(p.quit)
	p.wg.Wait()
	p.logger.Info().Msg("Worker pool stopped")
}

// SubmitImportJob submits an import job to the pool
func (p *Pool) SubmitImportJob(job *models.Job, source JobSource, cleanup func()) error {
	select {
	case p.importChan <- &ImportJob{Job: job, Source: source, Cleanup: cleanup}:
		return nil
	default:
		return fmt.Errorf("import job queue is full")
	}
}

// SubmitExportJob submits an export job to the pool
func (p *Pool) SubmitExportJob(job *models.Job, filters *models.ExportFilters) error {
	select {
	case p.exportChan <- &ExportJob{Job: job, Filters: filters}:
		return nil
	default:
		return fmt.Errorf("export job queue is full")
	}
}

func (p *Pool) importWorker(ctx context.Context, id int) {
	defer p.wg.Done()
	logger := p.logger.With().Int("worker_id", id).Str("type", "import").Logger()
	logger.Info().Msg("Import worker started")

	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("Import worker stopping (context cancelled)")
			return
		case <-p.quit:
			logger.Info().Msg("Import worker stopping")
			return
		case job := <-p.importChan:
			p.processImportJob(ctx, job, logger)
		}
	}
}

func (p *Pool) exportWorker(ctx context.Context, id int) {
	defer p.wg.Done()
	logger := p.logger.With().Int("worker_id", id).Str("type", "export").Logger()
	logger.Info().Msg("Export worker started")

	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("Export worker stopping (context cancelled)")
			return
		case <-p.quit:
			logger.Info().Msg("Export worker stopping")
			return
		case job := <-p.exportChan:
			p.processExportJob(ctx, job, logger)
		}
	}
}

func (p *Pool) processImportJob(ctx context.Context, importJob *ImportJob, logger zerolog.Logger) {
	job := importJob.Job
	startTime := time.Now()

	logger.Info().
		Str("job_id", job.ID.String()).
		Str("resource", string(job.Resource)).
		Msg("Processing import job")

	// Track active jobs
	if p.metrics != nil {
		p.metrics.SetActiveJobs(models.JobTypeImport, 1)
		defer p.metrics.SetActiveJobs(models.JobTypeImport, -1)
	}

	// Cleanup after processing
	if importJob.Cleanup != nil {
		defer importJob.Cleanup()
	}

	// Open the file
	var file *os.File
	var err error

	if importJob.Source.FilePath != "" {
		file, err = os.Open(importJob.Source.FilePath)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to open import file")
			p.failJob(ctx, job, fmt.Sprintf("failed to open file: %v", err))
			return
		}
		defer file.Close()
	} else if importJob.Source.URL != "" {
		// Download from URL - for now we support local files only
		logger.Error().Msg("URL imports not yet implemented")
		p.failJob(ctx, job, "URL imports not yet implemented")
		return
	}

	// Determine file format from job or detect it
	var format string
	if job.FileFormat != nil && *job.FileFormat != "" {
		format = *job.FileFormat
	} else {
		// Detect from file path
		if importJob.Source.FilePath != "" {
			if len(importJob.Source.FilePath) > 4 && importJob.Source.FilePath[len(importJob.Source.FilePath)-4:] == ".csv" {
				format = "csv"
			} else {
				format = "ndjson"
			}
		}
	}

	// Process the import
	err = p.importSvc.ProcessImport(ctx, file, job, format)
	if err != nil {
		logger.Error().Err(err).Msg("Import processing failed")
		// Job status is already updated by the service
	}

	duration := time.Since(startTime)
	logger.Info().
		Str("job_id", job.ID.String()).
		Str("status", string(job.Status)).
		Int64("duration_ms", duration.Milliseconds()).
		Msg("Import job completed")

	// Record metrics
	if p.metrics != nil {
		status := "success"
		if job.Status == models.JobStatusFailed {
			status = "error"
		}
		p.metrics.RecordJobDuration(models.JobTypeImport, status, duration.Seconds())
	}
}

func (p *Pool) processExportJob(ctx context.Context, exportJob *ExportJob, logger zerolog.Logger) {
	job := exportJob.Job
	startTime := time.Now()

	logger.Info().
		Str("job_id", job.ID.String()).
		Str("resource", string(job.Resource)).
		Msg("Processing export job")

	// Track active jobs
	if p.metrics != nil {
		p.metrics.SetActiveJobs(models.JobTypeExport, 1)
		defer p.metrics.SetActiveJobs(models.JobTypeExport, -1)
	}

	// Process the export
	err := p.exportSvc.ProcessAsyncExport(ctx, job, exportJob.Filters)
	if err != nil {
		logger.Error().Err(err).Msg("Export processing failed")
		// Job status is already updated by the service
	}

	duration := time.Since(startTime)
	logger.Info().
		Str("job_id", job.ID.String()).
		Str("status", string(job.Status)).
		Int64("duration_ms", duration.Milliseconds()).
		Msg("Export job completed")

	// Record metrics
	if p.metrics != nil {
		status := "success"
		if job.Status == models.JobStatusFailed {
			status = "error"
		}
		p.metrics.RecordJobDuration(models.JobTypeExport, status, duration.Seconds())
	}
}

func (p *Pool) failJob(ctx context.Context, job *models.Job, errorMsg string) {
	job.Status = models.JobStatusFailed
	job.ErrorMessage = &errorMsg
	now := time.Now()
	job.CompletedAt = &now

	if err := p.jobRepo.Update(ctx, job); err != nil {
		p.logger.Error().Err(err).Str("job_id", job.ID.String()).Msg("Failed to update job status")
	}
}

// GetQueueStats returns current queue statistics
func (p *Pool) GetQueueStats() map[string]int {
	return map[string]int{
		"import_queue_size": len(p.importChan),
		"import_queue_cap":  cap(p.importChan),
		"export_queue_size": len(p.exportChan),
		"export_queue_cap":  cap(p.exportChan),
	}
}

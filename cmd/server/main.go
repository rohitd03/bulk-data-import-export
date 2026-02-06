package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rohit/bulk-import-export/internal/api"
	"github.com/rohit/bulk-import-export/internal/config"
	"github.com/rohit/bulk-import-export/internal/metrics"
	"github.com/rohit/bulk-import-export/internal/repository/postgres"
	exportservice "github.com/rohit/bulk-import-export/internal/service/export"
	importservice "github.com/rohit/bulk-import-export/internal/service/import"
	"github.com/rohit/bulk-import-export/internal/worker"
	"github.com/rohit/bulk-import-export/pkg/logger"
)

func main() {
	// Initialize logger
	log := logger.New()

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// Initialize metrics
	metricsCollector := metrics.NewCollector()

	// Initialize database
	db, err := postgres.NewConnection(cfg.Database)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to database")
	}
	defer db.Close()

	// Initialize repositories
	userRepo := postgres.NewUserRepository(db)
	articleRepo := postgres.NewArticleRepository(db)
	commentRepo := postgres.NewCommentRepository(db)
	jobRepo := postgres.NewJobRepository(db)
	stagingRepo := postgres.NewStagingRepository(db)
	idempotencyRepo := postgres.NewIdempotencyRepository(db)

	// Initialize services
	importSvc := importservice.NewService(
		userRepo,
		articleRepo,
		commentRepo,
		jobRepo,
		stagingRepo,
		metricsCollector,
		log,
		cfg.Import,
	)

	exportSvc := exportservice.NewService(
		userRepo,
		articleRepo,
		commentRepo,
		jobRepo,
		metricsCollector,
		log,
		cfg.Export,
	)

	// Initialize worker pool
	workerPool := worker.NewPool(
		importSvc,
		exportSvc,
		jobRepo,
		metricsCollector,
		log,
		cfg.Worker,
	)

	// Start worker pool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workerPool.Start(ctx)

	// Initialize router
	router := api.NewRouter(
		db.DB,
		importSvc,
		exportSvc,
		jobRepo,
		idempotencyRepo,
		workerPool,
		metricsCollector,
		log,
		cfg,
	)

	// Create HTTP server
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.App.Port),
		Handler:      router.Engine(),
		ReadTimeout:  time.Duration(cfg.App.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(cfg.App.WriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(cfg.App.IdleTimeout) * time.Second,
	}

	// Start server in goroutine
	go func() {
		log.Info().
			Int("port", cfg.App.Port).
			Str("env", cfg.App.Env).
			Msg("Starting server")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("Server failed")
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info().Msg("Shutting down server...")

	// Graceful shutdown with new context
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Cancel worker context
	cancel()

	// Stop worker pool
	workerPool.Stop()

	// Shutdown HTTP server
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("Server forced to shutdown")
	}

	log.Info().Msg("Server exited")
}

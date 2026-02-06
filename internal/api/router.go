package api

import (
	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rohit/bulk-import-export/internal/api/handlers"
	"github.com/rohit/bulk-import-export/internal/api/middleware"
	"github.com/rohit/bulk-import-export/internal/config"
	"github.com/rohit/bulk-import-export/internal/metrics"
	"github.com/rohit/bulk-import-export/internal/repository/postgres"
	exportservice "github.com/rohit/bulk-import-export/internal/service/export"
	importservice "github.com/rohit/bulk-import-export/internal/service/import"
	"github.com/rohit/bulk-import-export/internal/worker"
	"github.com/rs/zerolog"
)

// Router holds all dependencies for the API router
type Router struct {
	engine           *gin.Engine
	logger           zerolog.Logger
	db               *sqlx.DB
	cfg              *config.Config
	metricsCollector *metrics.Collector
}

// NewRouter creates a new API router
func NewRouter(
	db *sqlx.DB,
	importSvc *importservice.Service,
	exportSvc *exportservice.Service,
	jobRepo *postgres.JobRepository,
	idempotencyRepo *postgres.IdempotencyRepository,
	workerPool *worker.Pool,
	metricsCollector *metrics.Collector,
	logger zerolog.Logger,
	cfg *config.Config,
) *Router {
	// Set gin mode
	if cfg.App.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	engine := gin.New()

	// Global middleware
	engine.Use(middleware.Recovery(logger))
	engine.Use(middleware.Logger(logger))
	engine.Use(middleware.CORS())

	if metricsCollector != nil {
		engine.Use(middleware.Metrics(metricsCollector))
	}

	// Create handlers
	healthHandler := handlers.NewHealthHandler(db)
	importHandler := handlers.NewImportHandler(
		importSvc,
		jobRepo,
		idempotencyRepo,
		workerPool,
		logger,
		cfg.Import,
	)
	exportHandler := handlers.NewExportHandler(
		exportSvc,
		jobRepo,
		workerPool,
		logger,
		cfg.Export,
	)

	// Health routes (no version prefix)
	engine.GET("/health", healthHandler.Health)
	engine.GET("/ready", healthHandler.Ready)
	engine.GET("/live", healthHandler.Live)

	// Metrics endpoint
	if cfg.Prometheus.Enabled {
		engine.GET("/metrics", gin.WrapH(promhttp.Handler()))
	}

	// API v1 routes
	v1 := engine.Group("/v1")
	{
		// Import routes
		imports := v1.Group("/imports")
		imports.Use(middleware.Idempotency(idempotencyRepo))
		{
			imports.POST("", importHandler.CreateImport)
			imports.GET("/:job_id", importHandler.GetImportStatus)
			imports.GET("/:job_id/errors", importHandler.GetImportErrors)
		}

		// Export routes
		exports := v1.Group("/exports")
		{
			exports.GET("", exportHandler.StreamExport)
			exports.POST("", exportHandler.CreateAsyncExport)
			exports.GET("/:job_id", exportHandler.GetExportStatus)
			exports.GET("/:job_id/download", exportHandler.DownloadExport)
		}
	}

	return &Router{
		engine:           engine,
		logger:           logger,
		db:               db,
		cfg:              cfg,
		metricsCollector: metricsCollector,
	}
}

// Engine returns the gin engine
func (r *Router) Engine() *gin.Engine {
	return r.engine
}

// Run starts the HTTP server
func (r *Router) Run(addr string) error {
	r.logger.Info().Str("addr", addr).Msg("Starting HTTP server")
	return r.engine.Run(addr)
}

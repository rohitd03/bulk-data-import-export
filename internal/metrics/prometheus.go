package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Collector holds all Prometheus metrics
type Collector struct {
	// Import metrics
	ImportJobsTotal     *prometheus.CounterVec
	ImportRecordsTotal  *prometheus.CounterVec
	ImportErrorsTotal   *prometheus.CounterVec
	ImportJobsActive    *prometheus.GaugeVec
	ImportJobDuration   *prometheus.HistogramVec
	ImportBatchDuration *prometheus.HistogramVec
	ImportRowsPerSecond *prometheus.GaugeVec

	// Export metrics
	ExportJobsTotal     *prometheus.CounterVec
	ExportRecordsTotal  *prometheus.CounterVec
	ExportJobsActive    *prometheus.GaugeVec
	ExportJobDuration   *prometheus.HistogramVec
	ExportRowsPerSecond *prometheus.GaugeVec

	// HTTP metrics
	HTTPRequestsTotal   *prometheus.CounterVec
	HTTPRequestDuration *prometheus.HistogramVec

	// Database metrics
	DBConnectionsActive prometheus.Gauge
	DBQueryDuration     *prometheus.HistogramVec
}

// NewCollector creates a new metrics collector
func NewCollector() *Collector {
	return &Collector{
		// Import metrics
		ImportJobsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "import_jobs_total",
				Help: "Total number of import jobs",
			},
			[]string{"resource", "status"},
		),
		ImportRecordsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "import_records_total",
				Help: "Total number of records processed during import",
			},
			[]string{"resource", "status"},
		),
		ImportErrorsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "import_errors_total",
				Help: "Total number of import errors by error code",
			},
			[]string{"resource", "error_code"},
		),
		ImportJobsActive: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "import_jobs_active",
				Help: "Number of currently active import jobs",
			},
			[]string{"resource"},
		),
		ImportJobDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "import_job_duration_seconds",
				Help:    "Duration of import jobs in seconds",
				Buckets: prometheus.ExponentialBuckets(1, 2, 15), // 1s to ~9h
			},
			[]string{"resource"},
		),
		ImportBatchDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "import_batch_duration_seconds",
				Help:    "Duration of batch processing in seconds",
				Buckets: prometheus.ExponentialBuckets(0.01, 2, 12), // 10ms to ~40s
			},
			[]string{"resource"},
		),
		ImportRowsPerSecond: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "import_rows_per_second",
				Help: "Current import processing rate",
			},
			[]string{"resource", "job_id"},
		),

		// Export metrics
		ExportJobsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "export_jobs_total",
				Help: "Total number of export jobs",
			},
			[]string{"resource", "status"},
		),
		ExportRecordsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "export_records_total",
				Help: "Total number of records exported",
			},
			[]string{"resource"},
		),
		ExportJobsActive: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "export_jobs_active",
				Help: "Number of currently active export jobs",
			},
			[]string{"resource"},
		),
		ExportJobDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "export_job_duration_seconds",
				Help:    "Duration of export jobs in seconds",
				Buckets: prometheus.ExponentialBuckets(1, 2, 15),
			},
			[]string{"resource"},
		),
		ExportRowsPerSecond: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "export_rows_per_second",
				Help: "Current export processing rate",
			},
			[]string{"resource", "job_id"},
		),

		// HTTP metrics
		HTTPRequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_requests_total",
				Help: "Total number of HTTP requests",
			},
			[]string{"method", "path", "status"},
		),
		HTTPRequestDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "http_request_duration_seconds",
				Help:    "Duration of HTTP requests in seconds",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
			},
			[]string{"method", "path"},
		),

		// Database metrics
		DBConnectionsActive: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "database_connections_active",
				Help: "Number of active database connections",
			},
		),
		DBQueryDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "database_query_duration_seconds",
				Help:    "Duration of database queries in seconds",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
			},
			[]string{"operation"},
		),
	}
}

// RecordImportJobStarted records when an import job starts
func (c *Collector) RecordImportJobStarted(resource string) {
	c.ImportJobsActive.WithLabelValues(resource).Inc()
}

// RecordImportJobCompleted records when an import job completes
func (c *Collector) RecordImportJobCompleted(resource, status string, duration float64) {
	c.ImportJobsTotal.WithLabelValues(resource, status).Inc()
	c.ImportJobsActive.WithLabelValues(resource).Dec()
	c.ImportJobDuration.WithLabelValues(resource).Observe(duration)
}

// RecordImportRecord records a processed import record
func (c *Collector) RecordImportRecord(resource, status string) {
	c.ImportRecordsTotal.WithLabelValues(resource, status).Inc()
}

// RecordImportError records an import error
func (c *Collector) RecordImportError(resource, errorCode string) {
	c.ImportErrorsTotal.WithLabelValues(resource, errorCode).Inc()
}

// RecordImportBatch records batch processing duration
func (c *Collector) RecordImportBatch(resource string, duration float64) {
	c.ImportBatchDuration.WithLabelValues(resource).Observe(duration)
}

// RecordImportRate records the current import rate
func (c *Collector) RecordImportRate(resource, jobID string, rowsPerSecond float64) {
	c.ImportRowsPerSecond.WithLabelValues(resource, jobID).Set(rowsPerSecond)
}

// RecordExportJobStarted records when an export job starts
func (c *Collector) RecordExportJobStarted(resource string) {
	c.ExportJobsActive.WithLabelValues(resource).Inc()
}

// RecordExportJobCompleted records when an export job completes
func (c *Collector) RecordExportJobCompleted(resource, status string, duration float64) {
	c.ExportJobsTotal.WithLabelValues(resource, status).Inc()
	c.ExportJobsActive.WithLabelValues(resource).Dec()
	c.ExportJobDuration.WithLabelValues(resource).Observe(duration)
}

// RecordExportRecords records exported records
func (c *Collector) RecordExportRecords(resource string, count int) {
	c.ExportRecordsTotal.WithLabelValues(resource).Add(float64(count))
}

// RecordExportRate records the current export rate
func (c *Collector) RecordExportRate(resource, jobID string, rowsPerSecond float64) {
	c.ExportRowsPerSecond.WithLabelValues(resource, jobID).Set(rowsPerSecond)
}

// RecordHTTPRequest records an HTTP request
func (c *Collector) RecordHTTPRequest(method, path, status string, duration float64) {
	c.HTTPRequestsTotal.WithLabelValues(method, path, status).Inc()
	c.HTTPRequestDuration.WithLabelValues(method, path).Observe(duration)
}

// RecordDBQuery records a database query
func (c *Collector) RecordDBQuery(operation string, duration float64) {
	c.DBQueryDuration.WithLabelValues(operation).Observe(duration)
}

// SetDBConnections sets the number of active database connections
func (c *Collector) SetDBConnections(count int) {
	c.DBConnectionsActive.Set(float64(count))
}

// SetActiveJobs adjusts the number of active jobs for a job type
func (c *Collector) SetActiveJobs(jobType interface{}, delta int) {
	// Convert jobType to string
	typeStr := "unknown"
	switch v := jobType.(type) {
	case string:
		typeStr = v
	default:
		typeStr = fmt.Sprintf("%v", v)
	}

	if typeStr == "import" {
		if delta > 0 {
			c.ImportJobsActive.WithLabelValues("all").Inc()
		} else {
			c.ImportJobsActive.WithLabelValues("all").Dec()
		}
	} else if typeStr == "export" {
		if delta > 0 {
			c.ExportJobsActive.WithLabelValues("all").Inc()
		} else {
			c.ExportJobsActive.WithLabelValues("all").Dec()
		}
	}
}

// RecordJobDuration records the duration of a completed job
func (c *Collector) RecordJobDuration(jobType interface{}, status string, duration float64) {
	typeStr := "unknown"
	switch v := jobType.(type) {
	case string:
		typeStr = v
	default:
		typeStr = fmt.Sprintf("%v", v)
	}

	if typeStr == "import" {
		c.ImportJobDuration.WithLabelValues("all").Observe(duration)
		c.ImportJobsTotal.WithLabelValues("all", status).Inc()
	} else if typeStr == "export" {
		c.ExportJobDuration.WithLabelValues("all").Observe(duration)
		c.ExportJobsTotal.WithLabelValues("all", status).Inc()
	}
}

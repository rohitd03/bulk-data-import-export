package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all configuration for the application
type Config struct {
	App        AppConfig
	Database   DatabaseConfig
	Import     ImportConfig
	Export     ExportConfig
	Worker     WorkerConfig
	Storage    StorageConfig
	Prometheus PrometheusConfig
}

// AppConfig holds application settings
type AppConfig struct {
	Env          string
	Port         int
	Name         string
	ReadTimeout  int
	WriteTimeout int
	IdleTimeout  int
}

// DatabaseConfig holds database settings
type DatabaseConfig struct {
	Host         string
	Port         int
	User         string
	Password     string
	Name         string
	SSLMode      string
	MaxOpenConns int
	MaxIdleConns int
}

// ImportConfig holds import settings
type ImportConfig struct {
	BatchSize     int
	WorkerCount   int
	MaxFileSizeMB int
	UploadPath    string
}

// ExportConfig holds export settings
type ExportConfig struct {
	BatchSize   int
	WorkerCount int
	OutputPath  string
}

// WorkerConfig holds worker pool settings
type WorkerConfig struct {
	ImportWorkers int
	ExportWorkers int
	QueueSize     int
}

// StorageConfig holds file storage settings
type StorageConfig struct {
	Type       string // local, s3
	LocalPath  string
	S3Endpoint string
	S3Region   string
	S3Bucket   string
}

// PrometheusConfig holds Prometheus settings
type PrometheusConfig struct {
	Enabled bool
	Port    int
}

// Load loads configuration from environment variables
func Load() (*Config, error) {
	cfg := &Config{
		App: AppConfig{
			Env:          getEnv("APP_ENV", "development"),
			Port:         getEnvAsInt("APP_PORT", 8080),
			Name:         getEnv("APP_NAME", "bulk-import-export"),
			ReadTimeout:  getEnvAsInt("APP_READ_TIMEOUT", 30),
			WriteTimeout: getEnvAsInt("APP_WRITE_TIMEOUT", 300), // Long timeout for exports
			IdleTimeout:  getEnvAsInt("APP_IDLE_TIMEOUT", 120),
		},
		Database: DatabaseConfig{
			Host:         getEnv("DB_HOST", "localhost"),
			Port:         getEnvAsInt("DB_PORT", 5432),
			User:         getEnv("DB_USER", "postgres"),
			Password:     getEnv("DB_PASSWORD", "postgres"),
			Name:         getEnv("DB_NAME", "bulk_import_export"),
			SSLMode:      getEnv("DB_SSL_MODE", "disable"),
			MaxOpenConns: getEnvAsInt("DB_MAX_OPEN_CONNS", 50),
			MaxIdleConns: getEnvAsInt("DB_MAX_IDLE_CONNS", 10),
		},
		Import: ImportConfig{
			BatchSize:     getEnvAsInt("IMPORT_BATCH_SIZE", 1000),
			WorkerCount:   getEnvAsInt("IMPORT_WORKER_COUNT", 4),
			MaxFileSizeMB: getEnvAsInt("MAX_FILE_SIZE_MB", 500),
			UploadPath:    getEnv("UPLOAD_PATH", "./uploads"),
		},
		Export: ExportConfig{
			BatchSize:   getEnvAsInt("EXPORT_BATCH_SIZE", 5000),
			WorkerCount: getEnvAsInt("EXPORT_WORKER_COUNT", 2),
			OutputPath:  getEnv("EXPORT_PATH", "./exports"),
		},
		Worker: WorkerConfig{
			ImportWorkers: getEnvAsInt("IMPORT_WORKER_COUNT", 4),
			ExportWorkers: getEnvAsInt("EXPORT_WORKER_COUNT", 2),
			QueueSize:     getEnvAsInt("WORKER_QUEUE_SIZE", 100),
		},
		Storage: StorageConfig{
			Type:       getEnv("STORAGE_TYPE", "local"),
			LocalPath:  getEnv("STORAGE_PATH", "./storage"),
			S3Endpoint: getEnv("AWS_ENDPOINT", "http://localhost:4566"),
			S3Region:   getEnv("AWS_REGION", "us-east-1"),
			S3Bucket:   getEnv("AWS_BUCKET", "bulk-imports"),
		},
		Prometheus: PrometheusConfig{
			Enabled: getEnvAsBool("PROMETHEUS_ENABLED", true),
			Port:    getEnvAsInt("PROMETHEUS_PORT", 9090),
		},
	}

	// Ensure directories exist
	if err := os.MkdirAll(cfg.Import.UploadPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create upload directory: %w", err)
	}
	if err := os.MkdirAll(cfg.Export.OutputPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create export directory: %w", err)
	}
	if cfg.Storage.Type == "local" {
		if err := os.MkdirAll(cfg.Storage.LocalPath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create storage directory: %w", err)
		}
	}

	return cfg, nil
}

// DSN returns the database connection string
func (c *DatabaseConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.Name, c.SSLMode,
	)
}

// IdempotencyTTL returns the TTL for idempotency keys
func IdempotencyTTL() time.Duration {
	hours := getEnvAsInt("IDEMPOTENCY_TTL_HOURS", 24)
	return time.Duration(hours) * time.Hour
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	strValue := getEnv(key, "")
	if strValue == "" {
		return defaultValue
	}
	intValue, err := strconv.Atoi(strValue)
	if err != nil {
		return defaultValue
	}
	return intValue
}

func getEnvAsBool(key string, defaultValue bool) bool {
	strValue := getEnv(key, "")
	if strValue == "" {
		return defaultValue
	}
	boolValue, err := strconv.ParseBool(strValue)
	if err != nil {
		return defaultValue
	}
	return boolValue
}

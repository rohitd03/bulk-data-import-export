package logger

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

// New creates a new logger instance
func New() zerolog.Logger {
	// Configure zerolog
	zerolog.TimeFieldFormat = time.RFC3339Nano

	// Create console writer for development
	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}

	// Check environment
	env := os.Getenv("APP_ENV")
	if env == "production" {
		// Use JSON format in production
		return zerolog.New(os.Stdout).
			With().
			Timestamp().
			Caller().
			Logger()
	}

	// Use console format in development
	return zerolog.New(output).
		With().
		Timestamp().
		Caller().
		Logger()
}

// WithRequestID returns a logger with request ID
func WithRequestID(logger zerolog.Logger, requestID string) zerolog.Logger {
	return logger.With().Str("request_id", requestID).Logger()
}

// WithJobID returns a logger with job ID
func WithJobID(logger zerolog.Logger, jobID string) zerolog.Logger {
	return logger.With().Str("job_id", jobID).Logger()
}

// WithResource returns a logger with resource type
func WithResource(logger zerolog.Logger, resource string) zerolog.Logger {
	return logger.With().Str("resource", resource).Logger()
}

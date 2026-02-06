# Bulk Data Import/Export System

A high-performance bulk data import/export system built with Go, PostgreSQL, and Prometheus metrics.

## Features

- **Bulk Import**: Import users, articles, and comments (both CSV and NDJSON supported)
- **Remote URL Import**: Import files directly from remote URLs
- **Bulk Export**: Stream or async export with filtering support
- **High Performance**: Handles up to 1M records efficiently
- **Validation**: Comprehensive validation with detailed error reporting
- **Idempotency**: Support for idempotent import requests
- **Metrics**: Prometheus metrics for monitoring
- **Staging Tables**: Duplicate detection using PostgreSQL staging tables

## Quick Start

### Prerequisites

- Go 1.21+
- Docker & Docker Compose
- Make (optional)

### Setup

1. **Clone and setup**:

```bash
git clone <repository-url>
cd bulk-data-import-export
make setup
```

2. **Start services**:

```bash
make dev
```

This starts:

- App: http://localhost:8080
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)

3. **Verify**:

```bash
curl http://localhost:8080/health
```

### Manual Setup (without Docker)

1. **Install dependencies**:

```bash
go mod download
```

2. **Setup PostgreSQL**:

```bash
# Create database
createdb bulk_import_export

# Run migrations
psql -d bulk_import_export -f migrations/001_initial_schema.sql
```

3. **Configure environment**:

```bash
cp .env.example .env
# Edit .env with your settings
```

4. **Run the application**:

```bash
go run cmd/server/main.go
```

## API Endpoints

### Health Checks

| Endpoint  | Method | Description        |
| --------- | ------ | ------------------ |
| `/health` | GET    | Full health status |
| `/ready`  | GET    | Readiness check    |
| `/live`   | GET    | Liveness check     |

### Import

| Endpoint                     | Method | Description       |
| ---------------------------- | ------ | ----------------- |
| `/v1/imports`                | POST   | Create import job |
| `/v1/imports/:job_id`        | GET    | Get import status |
| `/v1/imports/:job_id/errors` | GET    | Get import errors |

### Export

| Endpoint                       | Method | Description          |
| ------------------------------ | ------ | -------------------- |
| `/v1/exports`                  | GET    | Stream export        |
| `/v1/exports`                  | POST   | Create async export  |
| `/v1/exports/:job_id`          | GET    | Get export status    |
| `/v1/exports/:job_id/download` | GET    | Download export file |

### Metrics

| Endpoint   | Method | Description        |
| ---------- | ------ | ------------------ |
| `/metrics` | GET    | Prometheus metrics |

## Usage Examples

### Import Users (CSV)

```bash
curl -X POST http://localhost:8080/v1/imports \
  -H "Idempotency-Key: $(uuidgen)" \
  -F "file=@import_testdata_all_in_one/users_huge.csv" \
  -F "resource=users"
```

### Import Articles (NDJSON)

```bash
curl -X POST http://localhost:8080/v1/imports \
  -H "Idempotency-Key: $(uuidgen)" \
  -F "file=@import_testdata_all_in_one/articles_huge.ndjson" \
  -F "resource=articles"
```

### Import Comments (NDJSON)

```bash
curl -X POST http://localhost:8080/v1/imports \
  -H "Idempotency-Key: $(uuidgen)" \
  -F "file=@import_testdata_all_in_one/comments_huge.ndjson" \
  -F "resource=comments"
```

### Import from Remote URL

```bash
curl -X POST http://localhost:8080/v1/imports \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"resource": "users", "file_url": "https://example.com/users.csv"}'
```

### Check Import Status

```bash
curl http://localhost:8080/v1/imports/{job_id}
```

### Get Import Errors

```bash
curl "http://localhost:8080/v1/imports/{job_id}/errors?limit=50&offset=0"
```

### Stream Export Users

```bash
curl "http://localhost:8080/v1/exports?resource=users&format=ndjson"
```

### Stream Export with Filters

```bash
curl "http://localhost:8080/v1/exports?resource=users&format=ndjson&role=admin&active=true"
```

### Create Async Export

```bash
curl -X POST http://localhost:8080/v1/exports \
  -H "Content-Type: application/json" \
  -d '{"resource": "users", "format": "ndjson", "filters": {"active": true}}'
```

## Resource Schemas

All resources support both **CSV** and **NDJSON** file formats. The format is detected automatically based on file extension:

- `.csv` → CSV format
- `.ndjson`, `.jsonl`, `.json` → NDJSON format

### Users

| Field  | Type    | Constraints                             |
| ------ | ------- | --------------------------------------- |
| name   | string  | Required                                |
| email  | string  | Required, valid email, unique           |
| role   | string  | Required, one of: admin, author, reader |
| active | boolean | Required                                |

### Articles

| Field        | Type     | Constraints                        |
| ------------ | -------- | ---------------------------------- |
| title        | string   | Required                           |
| slug         | string   | Required, kebab-case, unique       |
| content      | string   | Required                           |
| author_id    | UUID     | Required, must exist in users      |
| status       | string   | Required, one of: draft, published |
| published_at | datetime | Required if status=published       |
| tags         | string[] | Optional                           |

### Comments

| Field      | Type   | Constraints                      |
| ---------- | ------ | -------------------------------- |
| article_id | UUID   | Required, must exist in articles |
| user_id    | UUID   | Required, must exist in users    |
| body       | string | Required, max 500 words          |

## Configuration

| Environment Variable     | Default            | Description                          |
| ------------------------ | ------------------ | ------------------------------------ |
| APP_ENV                  | development        | Environment (development/production) |
| APP_PORT                 | 8080               | HTTP server port                     |
| DB_HOST                  | localhost          | PostgreSQL host                      |
| DB_PORT                  | 5432               | PostgreSQL port                      |
| DB_USER                  | postgres           | Database user                        |
| DB_PASSWORD              | postgres           | Database password                    |
| DB_NAME                  | bulk_import_export | Database name                        |
| IMPORT_BATCH_SIZE        | 1000               | Records per batch for imports        |
| IMPORT_MAX_FILE_SIZE     | 104857600          | Max file size (100MB)                |
| EXPORT_STREAM_BATCH_SIZE | 5000               | Records per batch for exports        |
| WORKER_IMPORT_WORKERS    | 4                  | Number of import workers             |
| WORKER_EXPORT_WORKERS    | 2                  | Number of export workers             |
| PROMETHEUS_ENABLED       | true               | Enable Prometheus metrics            |

## Prometheus Metrics

| Metric                                           | Type      | Labels                 | Description             |
| ------------------------------------------------ | --------- | ---------------------- | ----------------------- |
| bulk_import_export_http_requests_total           | Counter   | method, path, status   | Total HTTP requests     |
| bulk_import_export_http_request_duration_seconds | Histogram | method, path, status   | HTTP request duration   |
| bulk_import_export_jobs_total                    | Counter   | type, resource, status | Total jobs processed    |
| bulk_import_export_job_duration_seconds          | Histogram | type, status           | Job processing duration |
| bulk_import_export_records_processed_total       | Counter   | type, resource, status | Total records processed |
| bulk_import_export_active_jobs                   | Gauge     | type                   | Currently active jobs   |

## Make Commands

```bash
make build          # Build the application
make run            # Run locally
make test           # Run tests
make docker-build   # Build Docker image
make docker-up      # Start Docker containers
make docker-down    # Stop Docker containers
make docker-logs    # View container logs
make migrate        # Run database migrations
make dev            # Start development environment
make help           # Show all commands
```

## Project Structure

```
.
├── cmd/server/              # Application entry point
├── internal/
│   ├── api/                 # HTTP handlers and router
│   │   ├── handlers/        # Request handlers
│   │   └── middleware/      # HTTP middleware
│   ├── config/              # Configuration
│   ├── domain/              # Domain models and errors
│   │   ├── models/          # Data models
│   │   └── errors/          # Error definitions
│   ├── metrics/             # Prometheus metrics
│   ├── repository/          # Data access layer
│   │   └── postgres/        # PostgreSQL implementations
│   ├── service/             # Business logic
│   │   ├── import/          # Import service and parsers
│   │   ├── export/          # Export service
│   │   └── validation/      # Validators
│   └── worker/              # Background job workers
├── migrations/              # Database migrations
├── pkg/logger/              # Logging utilities
├── docker-compose.yml       # Docker Compose configuration
├── Dockerfile               # Docker build file
├── Makefile                 # Build automation
└── postman_collection.json  # Postman API collection
```

## Testing with Test Data

The `import_testdata_all_in_one/` directory contains test files:

- `users_huge.csv` - 10,000 user records
- `articles_huge.ndjson` - 15,000 article records
- `comments_huge.ndjson` - 20,000 comment records

Import order matters due to foreign key constraints:

1. Users first
2. Articles second (references users)
3. Comments last (references both users and articles)

## Postman Collection

Import `postman_collection.json` into Postman for a complete API testing environment. Set the `base_url` variable to your server address (default: `http://localhost:8080`).

## Performance

- Import: Processes 1000 records per batch
- Export: Streams 5000 records per batch
- Target: 5000 rows/second for exports
- Memory: O(1) memory usage through streaming

## License

MIT

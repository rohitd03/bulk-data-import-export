.PHONY: build run test clean docker-build docker-up docker-down migrate lint fmt deps help

# Variables
APP_NAME=bulk-import-export
MAIN_PATH=./cmd/server
DOCKER_COMPOSE=docker-compose

# Default target
.DEFAULT_GOAL := help

## build: Build the application
build:
	@echo "Building $(APP_NAME)..."
	go build -o bin/$(APP_NAME) $(MAIN_PATH)

## run: Run the application locally
run:
	@echo "Running $(APP_NAME)..."
	go run $(MAIN_PATH)

## test: Run all tests
test:
	@echo "Running tests..."
	go test -v -race -cover ./...

## test-coverage: Run tests with coverage report
test-coverage:
	@echo "Running tests with coverage..."
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

## clean: Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -rf bin/
	rm -f coverage.out coverage.html

## docker-build: Build Docker image
docker-build:
	@echo "Building Docker image..."
	docker build -t $(APP_NAME):latest .

## docker-up: Start all Docker containers
docker-up:
	@echo "Starting Docker containers..."
	$(DOCKER_COMPOSE) up -d

## docker-down: Stop all Docker containers
docker-down:
	@echo "Stopping Docker containers..."
	$(DOCKER_COMPOSE) down

## docker-logs: View Docker container logs
docker-logs:
	$(DOCKER_COMPOSE) logs -f

## docker-ps: List running containers
docker-ps:
	$(DOCKER_COMPOSE) ps

## migrate: Run database migrations
migrate:
	@echo "Running migrations..."
	@if [ -f .env ]; then export $$(cat .env | xargs); fi && \
	psql "postgresql://$$DB_USER:$$DB_PASSWORD@$$DB_HOST:$$DB_PORT/$$DB_NAME?sslmode=$$DB_SSLMODE" -f migrations/001_initial_schema.sql

## migrate-docker: Run migrations inside Docker
migrate-docker:
	@echo "Running migrations in Docker..."
	docker exec -i bulk-import-export-db psql -U postgres -d bulk_import_export < migrations/001_initial_schema.sql

## lint: Run linter
lint:
	@echo "Running linter..."
	golangci-lint run ./...

## fmt: Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...
	goimports -w .

## deps: Download dependencies
deps:
	@echo "Downloading dependencies..."
	go mod download
	go mod tidy

## deps-update: Update dependencies
deps-update:
	@echo "Updating dependencies..."
	go get -u ./...
	go mod tidy

## setup: Initial project setup
setup: deps
	@echo "Setting up project..."
	mkdir -p uploads exports
	cp -n .env.example .env 2>/dev/null || true
	@echo "Setup complete! Don't forget to update .env with your settings."

## dev: Start development environment
dev: docker-up
	@echo "Development environment started!"
	@echo "App: http://localhost:8080"
	@echo "Prometheus: http://localhost:9090"
	@echo "Grafana: http://localhost:3000 (admin/admin)"

## help: Show this help
help:
	@echo "Available targets:"
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'

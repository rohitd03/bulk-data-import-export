package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/rohit/bulk-import-export/internal/domain/models"
)

// IdempotencyRepository implements repository.IdempotencyRepository for PostgreSQL
type IdempotencyRepository struct {
	db *DB
}

// NewIdempotencyRepository creates a new IdempotencyRepository
func NewIdempotencyRepository(db *DB) *IdempotencyRepository {
	return &IdempotencyRepository{db: db}
}

// Create inserts a new idempotency key
func (r *IdempotencyRepository) Create(ctx context.Context, key *models.IdempotencyKey) error {
	if key.CreatedAt.IsZero() {
		key.CreatedAt = time.Now().UTC()
	}

	query := `
		INSERT INTO idempotency_keys (key, job_id, created_at, expires_at)
		VALUES ($1, $2, $3, $4)
	`
	_, err := r.db.ExecContext(ctx, query, key.Key, key.JobID, key.CreatedAt, key.ExpiresAt)
	return err
}

// GetByKey retrieves an idempotency key record
func (r *IdempotencyRepository) GetByKey(ctx context.Context, key string) (*models.IdempotencyKey, error) {
	var record models.IdempotencyKey
	err := r.db.GetContext(ctx, &record, "SELECT * FROM idempotency_keys WHERE key = $1 AND expires_at > NOW()", key)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &record, err
}

// Delete removes an idempotency key
func (r *IdempotencyRepository) Delete(ctx context.Context, key string) error {
	_, err := r.db.ExecContext(ctx, "DELETE FROM idempotency_keys WHERE key = $1", key)
	return err
}

// CleanupExpired removes expired idempotency keys
func (r *IdempotencyRepository) CleanupExpired(ctx context.Context) (int64, error) {
	result, err := r.db.ExecContext(ctx, "DELETE FROM idempotency_keys WHERE expires_at < NOW()")
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

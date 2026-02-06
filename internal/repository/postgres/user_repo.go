package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/rohit/bulk-import-export/internal/domain/models"
)

// UserRepository implements repository.UserRepository for PostgreSQL
type UserRepository struct {
	db *DB
}

// NewUserRepository creates a new UserRepository
func NewUserRepository(db *DB) *UserRepository {
	return &UserRepository{db: db}
}

// Create inserts a new user
func (r *UserRepository) Create(ctx context.Context, user *models.User) error {
	if user.ID == uuid.Nil {
		user.ID = uuid.New()
	}
	if user.CreatedAt.IsZero() {
		user.CreatedAt = time.Now().UTC()
	}
	if user.UpdatedAt.IsZero() {
		user.UpdatedAt = time.Now().UTC()
	}

	query := `
		INSERT INTO users (id, email, name, role, active, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err := r.db.ExecContext(ctx, query,
		user.ID, user.Email, user.Name, user.Role, user.Active, user.CreatedAt, user.UpdatedAt)
	return err
}

// CreateBatch inserts multiple users using COPY
func (r *UserRepository) CreateBatch(ctx context.Context, users []*models.User) (int, error) {
	if len(users) == 0 {
		return 0, nil
	}

	tx, err := r.db.BeginTx(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	// Prepare batch insert
	valueStrings := make([]string, 0, len(users))
	valueArgs := make([]interface{}, 0, len(users)*7)

	for i, user := range users {
		if user.ID == uuid.Nil {
			user.ID = uuid.New()
		}
		if user.CreatedAt.IsZero() {
			user.CreatedAt = time.Now().UTC()
		}
		if user.UpdatedAt.IsZero() {
			user.UpdatedAt = time.Now().UTC()
		}

		base := i * 7
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7))
		valueArgs = append(valueArgs, user.ID, user.Email, user.Name, user.Role, user.Active, user.CreatedAt, user.UpdatedAt)
	}

	query := fmt.Sprintf(`
		INSERT INTO users (id, email, name, role, active, created_at, updated_at)
		VALUES %s
		ON CONFLICT (id) DO UPDATE SET
			email = EXCLUDED.email,
			name = EXCLUDED.name,
			role = EXCLUDED.role,
			active = EXCLUDED.active,
			updated_at = EXCLUDED.updated_at
	`, strings.Join(valueStrings, ","))

	result, err := tx.ExecContext(ctx, query, valueArgs...)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	affected, _ := result.RowsAffected()
	return int(affected), nil
}

// GetByID retrieves a user by ID
func (r *UserRepository) GetByID(ctx context.Context, id uuid.UUID) (*models.User, error) {
	var user models.User
	err := r.db.GetContext(ctx, &user, "SELECT * FROM users WHERE id = $1", id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &user, err
}

// GetByEmail retrieves a user by email
func (r *UserRepository) GetByEmail(ctx context.Context, email string) (*models.User, error) {
	var user models.User
	err := r.db.GetContext(ctx, &user, "SELECT * FROM users WHERE email = $1", email)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &user, err
}

// GetAll retrieves all users with optional filters
func (r *UserRepository) GetAll(ctx context.Context, filters *models.ExportFilters) ([]*models.User, error) {
	query, args := r.buildSelectQuery(filters)
	var users []*models.User
	err := r.db.SelectContext(ctx, &users, query, args...)
	return users, err
}

// GetAllWithCursor streams users using a cursor for memory efficiency
func (r *UserRepository) GetAllWithCursor(ctx context.Context, filters *models.ExportFilters, batchSize int, callback func([]*models.User) error) error {
	query, args := r.buildSelectQuery(filters)

	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	batch := make([]*models.User, 0, batchSize)
	for rows.Next() {
		var user models.User
		if err := rows.StructScan(&user); err != nil {
			return err
		}
		batch = append(batch, &user)

		if len(batch) >= batchSize {
			if err := callback(batch); err != nil {
				return err
			}
			batch = make([]*models.User, 0, batchSize)
		}
	}

	if len(batch) > 0 {
		if err := callback(batch); err != nil {
			return err
		}
	}

	return rows.Err()
}

// Update updates an existing user
func (r *UserRepository) Update(ctx context.Context, user *models.User) error {
	user.UpdatedAt = time.Now().UTC()
	query := `
		UPDATE users 
		SET email = $2, name = $3, role = $4, active = $5, updated_at = $6
		WHERE id = $1
	`
	_, err := r.db.ExecContext(ctx, query, user.ID, user.Email, user.Name, user.Role, user.Active, user.UpdatedAt)
	return err
}

// Upsert inserts or updates a user
func (r *UserRepository) Upsert(ctx context.Context, user *models.User) error {
	if user.ID == uuid.Nil {
		user.ID = uuid.New()
	}
	if user.CreatedAt.IsZero() {
		user.CreatedAt = time.Now().UTC()
	}
	user.UpdatedAt = time.Now().UTC()

	query := `
		INSERT INTO users (id, email, name, role, active, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (email) DO UPDATE SET
			name = EXCLUDED.name,
			role = EXCLUDED.role,
			active = EXCLUDED.active,
			updated_at = EXCLUDED.updated_at
	`
	_, err := r.db.ExecContext(ctx, query,
		user.ID, user.Email, user.Name, user.Role, user.Active, user.CreatedAt, user.UpdatedAt)
	return err
}

// UpsertBatch upserts multiple users
func (r *UserRepository) UpsertBatch(ctx context.Context, users []*models.User) (int, int, error) {
	if len(users) == 0 {
		return 0, 0, nil
	}

	// Use CreateBatch which already has ON CONFLICT handling
	count, err := r.CreateBatch(ctx, users)
	return count, 0, err // For simplicity, we don't track updates separately
}

// Delete deletes a user by ID
func (r *UserRepository) Delete(ctx context.Context, id uuid.UUID) error {
	_, err := r.db.ExecContext(ctx, "DELETE FROM users WHERE id = $1", id)
	return err
}

// Exists checks if a user exists by ID
func (r *UserRepository) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	var exists bool
	err := r.db.GetContext(ctx, &exists, "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)", id)
	return exists, err
}

// EmailExists checks if an email exists, optionally excluding a specific user
func (r *UserRepository) EmailExists(ctx context.Context, email string, excludeID *uuid.UUID) (bool, error) {
	var exists bool
	var err error
	if excludeID != nil {
		err = r.db.GetContext(ctx, &exists,
			"SELECT EXISTS(SELECT 1 FROM users WHERE email = $1 AND id != $2)", email, *excludeID)
	} else {
		err = r.db.GetContext(ctx, &exists,
			"SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)", email)
	}
	return exists, err
}

// Count returns the number of users matching the filters
func (r *UserRepository) Count(ctx context.Context, filters *models.ExportFilters) (int64, error) {
	query := "SELECT COUNT(*) FROM users"
	args := []interface{}{}
	conditions := []string{}

	if filters != nil {
		if filters.Role != nil {
			conditions = append(conditions, fmt.Sprintf("role = $%d", len(args)+1))
			args = append(args, *filters.Role)
		}
		if filters.Active != nil {
			conditions = append(conditions, fmt.Sprintf("active = $%d", len(args)+1))
			args = append(args, *filters.Active)
		}
		if filters.CreatedAfter != nil {
			conditions = append(conditions, fmt.Sprintf("created_at >= $%d", len(args)+1))
			args = append(args, *filters.CreatedAfter)
		}
		if filters.CreatedBefore != nil {
			conditions = append(conditions, fmt.Sprintf("created_at <= $%d", len(args)+1))
			args = append(args, *filters.CreatedBefore)
		}
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	var count int64
	err := r.db.GetContext(ctx, &count, query, args...)
	return count, err
}

func (r *UserRepository) buildSelectQuery(filters *models.ExportFilters) (string, []interface{}) {
	query := "SELECT * FROM users"
	args := []interface{}{}
	conditions := []string{}

	if filters != nil {
		if filters.Role != nil {
			conditions = append(conditions, fmt.Sprintf("role = $%d", len(args)+1))
			args = append(args, *filters.Role)
		}
		if filters.Active != nil {
			conditions = append(conditions, fmt.Sprintf("active = $%d", len(args)+1))
			args = append(args, *filters.Active)
		}
		if filters.CreatedAfter != nil {
			conditions = append(conditions, fmt.Sprintf("created_at >= $%d", len(args)+1))
			args = append(args, *filters.CreatedAfter)
		}
		if filters.CreatedBefore != nil {
			conditions = append(conditions, fmt.Sprintf("created_at <= $%d", len(args)+1))
			args = append(args, *filters.CreatedBefore)
		}
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	query += " ORDER BY created_at ASC"

	return query, args
}

// GetByIDs retrieves multiple users by their IDs
func (r *UserRepository) GetByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*models.User, error) {
	if len(ids) == 0 {
		return make(map[uuid.UUID]*models.User), nil
	}

	query, args, err := sqlx.In("SELECT * FROM users WHERE id IN (?)", ids)
	if err != nil {
		return nil, err
	}

	query = r.db.Rebind(query)
	var users []*models.User
	if err := r.db.SelectContext(ctx, &users, query, args...); err != nil {
		return nil, err
	}

	result := make(map[uuid.UUID]*models.User)
	for _, user := range users {
		result[user.ID] = user
	}
	return result, nil
}

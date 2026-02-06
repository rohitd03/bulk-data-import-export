package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rohit/bulk-import-export/internal/domain/models"
)

// CommentRepository implements repository.CommentRepository for PostgreSQL
type CommentRepository struct {
	db *DB
}

// NewCommentRepository creates a new CommentRepository
func NewCommentRepository(db *DB) *CommentRepository {
	return &CommentRepository{db: db}
}

// Create inserts a new comment
func (r *CommentRepository) Create(ctx context.Context, comment *models.Comment) error {
	if comment.ID == uuid.Nil {
		comment.ID = uuid.New()
	}
	if comment.CreatedAt.IsZero() {
		comment.CreatedAt = time.Now().UTC()
	}

	query := `
		INSERT INTO comments (id, article_id, user_id, body, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := r.db.ExecContext(ctx, query, comment.ID, comment.ArticleID, comment.UserID, comment.Body, comment.CreatedAt)
	return err
}

// CreateBatch inserts multiple comments
func (r *CommentRepository) CreateBatch(ctx context.Context, comments []*models.Comment) (int, error) {
	if len(comments) == 0 {
		return 0, nil
	}

	tx, err := r.db.BeginTx(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	valueStrings := make([]string, 0, len(comments))
	valueArgs := make([]interface{}, 0, len(comments)*5)

	for i, comment := range comments {
		if comment.ID == uuid.Nil {
			comment.ID = uuid.New()
		}
		if comment.CreatedAt.IsZero() {
			comment.CreatedAt = time.Now().UTC()
		}

		base := i * 5
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5))
		valueArgs = append(valueArgs, comment.ID, comment.ArticleID, comment.UserID, comment.Body, comment.CreatedAt)
	}

	query := fmt.Sprintf(`
		INSERT INTO comments (id, article_id, user_id, body, created_at)
		VALUES %s
		ON CONFLICT (id) DO UPDATE SET
			article_id = EXCLUDED.article_id,
			user_id = EXCLUDED.user_id,
			body = EXCLUDED.body
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

// GetByID retrieves a comment by ID
func (r *CommentRepository) GetByID(ctx context.Context, id uuid.UUID) (*models.Comment, error) {
	var comment models.Comment
	err := r.db.GetContext(ctx, &comment, "SELECT * FROM comments WHERE id = $1", id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &comment, err
}

// GetAll retrieves all comments with optional filters
func (r *CommentRepository) GetAll(ctx context.Context, filters *models.ExportFilters) ([]*models.Comment, error) {
	query, args := r.buildSelectQuery(filters)
	var comments []*models.Comment
	err := r.db.SelectContext(ctx, &comments, query, args...)
	return comments, err
}

// GetAllWithCursor streams comments using a cursor for memory efficiency
func (r *CommentRepository) GetAllWithCursor(ctx context.Context, filters *models.ExportFilters, batchSize int, callback func([]*models.Comment) error) error {
	query, args := r.buildSelectQuery(filters)

	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	batch := make([]*models.Comment, 0, batchSize)
	for rows.Next() {
		var comment models.Comment
		if err := rows.StructScan(&comment); err != nil {
			return err
		}
		batch = append(batch, &comment)

		if len(batch) >= batchSize {
			if err := callback(batch); err != nil {
				return err
			}
			batch = make([]*models.Comment, 0, batchSize)
		}
	}

	if len(batch) > 0 {
		if err := callback(batch); err != nil {
			return err
		}
	}

	return rows.Err()
}

// Update updates an existing comment
func (r *CommentRepository) Update(ctx context.Context, comment *models.Comment) error {
	query := `
		UPDATE comments 
		SET article_id = $2, user_id = $3, body = $4
		WHERE id = $1
	`
	_, err := r.db.ExecContext(ctx, query, comment.ID, comment.ArticleID, comment.UserID, comment.Body)
	return err
}

// Upsert inserts or updates a comment
func (r *CommentRepository) Upsert(ctx context.Context, comment *models.Comment) error {
	if comment.ID == uuid.Nil {
		comment.ID = uuid.New()
	}
	if comment.CreatedAt.IsZero() {
		comment.CreatedAt = time.Now().UTC()
	}

	query := `
		INSERT INTO comments (id, article_id, user_id, body, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (id) DO UPDATE SET
			article_id = EXCLUDED.article_id,
			user_id = EXCLUDED.user_id,
			body = EXCLUDED.body
	`
	_, err := r.db.ExecContext(ctx, query, comment.ID, comment.ArticleID, comment.UserID, comment.Body, comment.CreatedAt)
	return err
}

// UpsertBatch upserts multiple comments
func (r *CommentRepository) UpsertBatch(ctx context.Context, comments []*models.Comment) (int, int, error) {
	if len(comments) == 0 {
		return 0, 0, nil
	}
	count, err := r.CreateBatch(ctx, comments)
	return count, 0, err
}

// Delete deletes a comment by ID
func (r *CommentRepository) Delete(ctx context.Context, id uuid.UUID) error {
	_, err := r.db.ExecContext(ctx, "DELETE FROM comments WHERE id = $1", id)
	return err
}

// Exists checks if a comment exists by ID
func (r *CommentRepository) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	var exists bool
	err := r.db.GetContext(ctx, &exists, "SELECT EXISTS(SELECT 1 FROM comments WHERE id = $1)", id)
	return exists, err
}

// Count returns the number of comments matching the filters
func (r *CommentRepository) Count(ctx context.Context, filters *models.ExportFilters) (int64, error) {
	query := "SELECT COUNT(*) FROM comments"
	args := []interface{}{}
	conditions := []string{}

	if filters != nil {
		if filters.ArticleID != nil {
			conditions = append(conditions, fmt.Sprintf("article_id = $%d", len(args)+1))
			args = append(args, *filters.ArticleID)
		}
		if filters.UserID != nil {
			conditions = append(conditions, fmt.Sprintf("user_id = $%d", len(args)+1))
			args = append(args, *filters.UserID)
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

func (r *CommentRepository) buildSelectQuery(filters *models.ExportFilters) (string, []interface{}) {
	query := "SELECT * FROM comments"
	args := []interface{}{}
	conditions := []string{}

	if filters != nil {
		if filters.ArticleID != nil {
			conditions = append(conditions, fmt.Sprintf("article_id = $%d", len(args)+1))
			args = append(args, *filters.ArticleID)
		}
		if filters.UserID != nil {
			conditions = append(conditions, fmt.Sprintf("user_id = $%d", len(args)+1))
			args = append(args, *filters.UserID)
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

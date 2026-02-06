package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/rohit/bulk-import-export/internal/domain/models"
)

// ArticleRepository implements repository.ArticleRepository for PostgreSQL
type ArticleRepository struct {
	db *DB
}

// NewArticleRepository creates a new ArticleRepository
func NewArticleRepository(db *DB) *ArticleRepository {
	return &ArticleRepository{db: db}
}

// Create inserts a new article
func (r *ArticleRepository) Create(ctx context.Context, article *models.Article) error {
	if article.ID == uuid.Nil {
		article.ID = uuid.New()
	}
	if article.CreatedAt.IsZero() {
		article.CreatedAt = time.Now().UTC()
	}
	if article.UpdatedAt.IsZero() {
		article.UpdatedAt = time.Now().UTC()
	}
	if article.Tags == nil {
		article.Tags = json.RawMessage("[]")
	}

	query := `
		INSERT INTO articles (id, slug, title, body, author_id, tags, published_at, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	_, err := r.db.ExecContext(ctx, query,
		article.ID, article.Slug, article.Title, article.Body, article.AuthorID,
		article.Tags, article.PublishedAt, article.Status, article.CreatedAt, article.UpdatedAt)
	return err
}

// CreateBatch inserts multiple articles
func (r *ArticleRepository) CreateBatch(ctx context.Context, articles []*models.Article) (int, error) {
	if len(articles) == 0 {
		return 0, nil
	}

	tx, err := r.db.BeginTx(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	valueStrings := make([]string, 0, len(articles))
	valueArgs := make([]interface{}, 0, len(articles)*10)

	for i, article := range articles {
		if article.ID == uuid.Nil {
			article.ID = uuid.New()
		}
		if article.CreatedAt.IsZero() {
			article.CreatedAt = time.Now().UTC()
		}
		if article.UpdatedAt.IsZero() {
			article.UpdatedAt = time.Now().UTC()
		}
		if article.Tags == nil {
			article.Tags = json.RawMessage("[]")
		}

		base := i * 10
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10))
		valueArgs = append(valueArgs, article.ID, article.Slug, article.Title, article.Body, article.AuthorID,
			article.Tags, article.PublishedAt, article.Status, article.CreatedAt, article.UpdatedAt)
	}

	query := fmt.Sprintf(`
		INSERT INTO articles (id, slug, title, body, author_id, tags, published_at, status, created_at, updated_at)
		VALUES %s
		ON CONFLICT (id) DO UPDATE SET
			slug = EXCLUDED.slug,
			title = EXCLUDED.title,
			body = EXCLUDED.body,
			author_id = EXCLUDED.author_id,
			tags = EXCLUDED.tags,
			published_at = EXCLUDED.published_at,
			status = EXCLUDED.status,
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

// GetByID retrieves an article by ID
func (r *ArticleRepository) GetByID(ctx context.Context, id uuid.UUID) (*models.Article, error) {
	var article models.Article
	err := r.db.GetContext(ctx, &article, "SELECT * FROM articles WHERE id = $1", id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &article, err
}

// GetBySlug retrieves an article by slug
func (r *ArticleRepository) GetBySlug(ctx context.Context, slug string) (*models.Article, error) {
	var article models.Article
	err := r.db.GetContext(ctx, &article, "SELECT * FROM articles WHERE slug = $1", slug)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &article, err
}

// GetAll retrieves all articles with optional filters
func (r *ArticleRepository) GetAll(ctx context.Context, filters *models.ExportFilters) ([]*models.Article, error) {
	query, args := r.buildSelectQuery(filters)
	var articles []*models.Article
	err := r.db.SelectContext(ctx, &articles, query, args...)
	return articles, err
}

// GetAllWithCursor streams articles using a cursor for memory efficiency
func (r *ArticleRepository) GetAllWithCursor(ctx context.Context, filters *models.ExportFilters, batchSize int, callback func([]*models.Article) error) error {
	query, args := r.buildSelectQuery(filters)

	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	batch := make([]*models.Article, 0, batchSize)
	for rows.Next() {
		var article models.Article
		if err := rows.StructScan(&article); err != nil {
			return err
		}
		batch = append(batch, &article)

		if len(batch) >= batchSize {
			if err := callback(batch); err != nil {
				return err
			}
			batch = make([]*models.Article, 0, batchSize)
		}
	}

	if len(batch) > 0 {
		if err := callback(batch); err != nil {
			return err
		}
	}

	return rows.Err()
}

// Update updates an existing article
func (r *ArticleRepository) Update(ctx context.Context, article *models.Article) error {
	article.UpdatedAt = time.Now().UTC()
	query := `
		UPDATE articles 
		SET slug = $2, title = $3, body = $4, author_id = $5, tags = $6, 
		    published_at = $7, status = $8, updated_at = $9
		WHERE id = $1
	`
	_, err := r.db.ExecContext(ctx, query, article.ID, article.Slug, article.Title,
		article.Body, article.AuthorID, article.Tags, article.PublishedAt, article.Status, article.UpdatedAt)
	return err
}

// Upsert inserts or updates an article
func (r *ArticleRepository) Upsert(ctx context.Context, article *models.Article) error {
	if article.ID == uuid.Nil {
		article.ID = uuid.New()
	}
	if article.CreatedAt.IsZero() {
		article.CreatedAt = time.Now().UTC()
	}
	article.UpdatedAt = time.Now().UTC()
	if article.Tags == nil {
		article.Tags = json.RawMessage("[]")
	}

	query := `
		INSERT INTO articles (id, slug, title, body, author_id, tags, published_at, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (slug) DO UPDATE SET
			title = EXCLUDED.title,
			body = EXCLUDED.body,
			author_id = EXCLUDED.author_id,
			tags = EXCLUDED.tags,
			published_at = EXCLUDED.published_at,
			status = EXCLUDED.status,
			updated_at = EXCLUDED.updated_at
	`
	_, err := r.db.ExecContext(ctx, query,
		article.ID, article.Slug, article.Title, article.Body, article.AuthorID,
		article.Tags, article.PublishedAt, article.Status, article.CreatedAt, article.UpdatedAt)
	return err
}

// UpsertBatch upserts multiple articles
func (r *ArticleRepository) UpsertBatch(ctx context.Context, articles []*models.Article) (int, int, error) {
	if len(articles) == 0 {
		return 0, 0, nil
	}
	count, err := r.CreateBatch(ctx, articles)
	return count, 0, err
}

// Delete deletes an article by ID
func (r *ArticleRepository) Delete(ctx context.Context, id uuid.UUID) error {
	_, err := r.db.ExecContext(ctx, "DELETE FROM articles WHERE id = $1", id)
	return err
}

// Exists checks if an article exists by ID
func (r *ArticleRepository) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	var exists bool
	err := r.db.GetContext(ctx, &exists, "SELECT EXISTS(SELECT 1 FROM articles WHERE id = $1)", id)
	return exists, err
}

// SlugExists checks if a slug exists, optionally excluding a specific article
func (r *ArticleRepository) SlugExists(ctx context.Context, slug string, excludeID *uuid.UUID) (bool, error) {
	var exists bool
	var err error
	if excludeID != nil {
		err = r.db.GetContext(ctx, &exists,
			"SELECT EXISTS(SELECT 1 FROM articles WHERE slug = $1 AND id != $2)", slug, *excludeID)
	} else {
		err = r.db.GetContext(ctx, &exists,
			"SELECT EXISTS(SELECT 1 FROM articles WHERE slug = $1)", slug)
	}
	return exists, err
}

// Count returns the number of articles matching the filters
func (r *ArticleRepository) Count(ctx context.Context, filters *models.ExportFilters) (int64, error) {
	query := "SELECT COUNT(*) FROM articles"
	args := []interface{}{}
	conditions := []string{}

	if filters != nil {
		if filters.Status != nil {
			conditions = append(conditions, fmt.Sprintf("status = $%d", len(args)+1))
			args = append(args, *filters.Status)
		}
		if filters.AuthorID != nil {
			conditions = append(conditions, fmt.Sprintf("author_id = $%d", len(args)+1))
			args = append(args, *filters.AuthorID)
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

func (r *ArticleRepository) buildSelectQuery(filters *models.ExportFilters) (string, []interface{}) {
	query := "SELECT * FROM articles"
	args := []interface{}{}
	conditions := []string{}

	if filters != nil {
		if filters.Status != nil {
			conditions = append(conditions, fmt.Sprintf("status = $%d", len(args)+1))
			args = append(args, *filters.Status)
		}
		if filters.AuthorID != nil {
			conditions = append(conditions, fmt.Sprintf("author_id = $%d", len(args)+1))
			args = append(args, *filters.AuthorID)
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

// GetByIDs retrieves multiple articles by their IDs
func (r *ArticleRepository) GetByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*models.Article, error) {
	if len(ids) == 0 {
		return make(map[uuid.UUID]*models.Article), nil
	}

	query, args, err := sqlx.In("SELECT * FROM articles WHERE id IN (?)", ids)
	if err != nil {
		return nil, err
	}

	query = r.db.Rebind(query)
	var articles []*models.Article
	if err := r.db.SelectContext(ctx, &articles, query, args...); err != nil {
		return nil, err
	}

	result := make(map[uuid.UUID]*models.Article)
	for _, article := range articles {
		result[article.ID] = article
	}
	return result, nil
}

package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/rohit/bulk-import-export/internal/repository"
)

// StagingRepository implements repository.StagingRepository for PostgreSQL
type StagingRepository struct {
	db *DB
}

// NewStagingRepository creates a new StagingRepository
func NewStagingRepository(db *DB) *StagingRepository {
	return &StagingRepository{db: db}
}

// CreateStagingUsers inserts users into the staging table
func (r *StagingRepository) CreateStagingUsers(ctx context.Context, jobID uuid.UUID, users []repository.StagingUser) error {
	if len(users) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Build batch insert query
	valueStrings := make([]string, 0, len(users))
	valueArgs := make([]interface{}, 0, len(users)*11)

	for i, user := range users {
		base := i * 11
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10, base+11,
		))
		valueArgs = append(valueArgs,
			jobID, user.RowNumber, user.ID, user.Email, user.Name, user.Role,
			user.Active, user.CreatedAt, user.UpdatedAt, user.ValidationError, user.IsValid,
		)
	}

	query := fmt.Sprintf(`
		INSERT INTO staging_users (job_id, row_number, id, email, name, role, active, created_at, updated_at, validation_error, is_valid)
		VALUES %s
	`, strings.Join(valueStrings, ","))

	_, err = tx.ExecContext(ctx, query, valueArgs...)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// MarkDuplicateUsersInBatch marks duplicate emails within the same batch
func (r *StagingRepository) MarkDuplicateUsersInBatch(ctx context.Context, jobID uuid.UUID) (int, error) {
	query := `
		UPDATE staging_users s1
		SET is_duplicate = true, 
		    validation_error = 'DUPLICATE_EMAIL',
		    is_valid = false
		WHERE job_id = $1
		AND EXISTS (
			SELECT 1 FROM staging_users s2
			WHERE s2.job_id = s1.job_id
			AND LOWER(s2.email) = LOWER(s1.email)
			AND s2.staging_id < s1.staging_id
		)
	`
	result, err := r.db.ExecContext(ctx, query, jobID)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	return int(affected), nil
}

// MarkDuplicateUsersAgainstExisting marks users that already exist in the main table
func (r *StagingRepository) MarkDuplicateUsersAgainstExisting(ctx context.Context, jobID uuid.UUID) (int, error) {
	query := `
		UPDATE staging_users s
		SET is_duplicate = true,
		    validation_error = 'DUPLICATE_EMAIL',
		    is_valid = false
		WHERE job_id = $1
		AND is_valid = true
		AND EXISTS (
			SELECT 1 FROM users u WHERE LOWER(u.email) = LOWER(s.email)
		)
		AND (s.id IS NULL OR NOT EXISTS (SELECT 1 FROM users u2 WHERE u2.id::text = s.id))
	`
	result, err := r.db.ExecContext(ctx, query, jobID)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	return int(affected), nil
}

// GetValidStagingUsers retrieves valid staging users in batches
func (r *StagingRepository) GetValidStagingUsers(ctx context.Context, jobID uuid.UUID, batchSize int, callback func([]repository.StagingUser) error) error {
	query := `
		SELECT * FROM staging_users 
		WHERE job_id = $1 AND is_valid = true AND is_duplicate = false AND processed = false
		ORDER BY staging_id ASC
	`
	rows, err := r.db.QueryxContext(ctx, query, jobID)
	if err != nil {
		return err
	}
	defer rows.Close()

	batch := make([]repository.StagingUser, 0, batchSize)
	for rows.Next() {
		var user repository.StagingUser
		if err := rows.StructScan(&user); err != nil {
			return err
		}
		batch = append(batch, user)

		if len(batch) >= batchSize {
			if err := callback(batch); err != nil {
				return err
			}
			batch = make([]repository.StagingUser, 0, batchSize)
		}
	}

	if len(batch) > 0 {
		if err := callback(batch); err != nil {
			return err
		}
	}

	return rows.Err()
}

// UpdateStagingUserValidation updates the validation status of a staging user
func (r *StagingRepository) UpdateStagingUserValidation(ctx context.Context, stagingID int64, isValid bool, errorMsg string) error {
	query := `UPDATE staging_users SET is_valid = $2, validation_error = $3 WHERE staging_id = $1`
	_, err := r.db.ExecContext(ctx, query, stagingID, isValid, errorMsg)
	return err
}

// CleanupStagingUsers removes staging users for a completed job
func (r *StagingRepository) CleanupStagingUsers(ctx context.Context, jobID uuid.UUID) error {
	_, err := r.db.ExecContext(ctx, "DELETE FROM staging_users WHERE job_id = $1", jobID)
	return err
}

// CreateStagingArticles inserts articles into the staging table
func (r *StagingRepository) CreateStagingArticles(ctx context.Context, jobID uuid.UUID, articles []repository.StagingArticle) error {
	if len(articles) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	valueStrings := make([]string, 0, len(articles))
	valueArgs := make([]interface{}, 0, len(articles)*12)

	for i, article := range articles {
		base := i * 12
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9, base+10, base+11, base+12,
		))
		valueArgs = append(valueArgs,
			jobID, article.RowNumber, article.ID, article.Slug, article.Title, article.Body,
			article.AuthorID, article.Tags, article.PublishedAt, article.Status, article.ValidationError, article.IsValid,
		)
	}

	query := fmt.Sprintf(`
		INSERT INTO staging_articles (job_id, row_number, id, slug, title, body, author_id, tags, published_at, status, validation_error, is_valid)
		VALUES %s
	`, strings.Join(valueStrings, ","))

	_, err = tx.ExecContext(ctx, query, valueArgs...)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// MarkDuplicateArticlesInBatch marks duplicate slugs within the same batch
func (r *StagingRepository) MarkDuplicateArticlesInBatch(ctx context.Context, jobID uuid.UUID) (int, error) {
	query := `
		UPDATE staging_articles s1
		SET is_duplicate = true,
		    validation_error = 'DUPLICATE_SLUG',
		    is_valid = false
		WHERE job_id = $1
		AND EXISTS (
			SELECT 1 FROM staging_articles s2
			WHERE s2.job_id = s1.job_id
			AND LOWER(s2.slug) = LOWER(s1.slug)
			AND s2.staging_id < s1.staging_id
		)
	`
	result, err := r.db.ExecContext(ctx, query, jobID)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	return int(affected), nil
}

// MarkDuplicateArticlesAgainstExisting marks articles that already exist in the main table
func (r *StagingRepository) MarkDuplicateArticlesAgainstExisting(ctx context.Context, jobID uuid.UUID) (int, error) {
	query := `
		UPDATE staging_articles s
		SET is_duplicate = true,
		    validation_error = 'DUPLICATE_SLUG',
		    is_valid = false
		WHERE job_id = $1
		AND is_valid = true
		AND EXISTS (
			SELECT 1 FROM articles a WHERE LOWER(a.slug) = LOWER(s.slug)
		)
		AND (s.id IS NULL OR NOT EXISTS (SELECT 1 FROM articles a2 WHERE a2.id::text = s.id))
	`
	result, err := r.db.ExecContext(ctx, query, jobID)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	return int(affected), nil
}

// MarkInvalidAuthorFKArticles marks articles where author_id doesn't exist in users table
func (r *StagingRepository) MarkInvalidAuthorFKArticles(ctx context.Context, jobID uuid.UUID) (int, error) {
	query := `
		UPDATE staging_articles s
		SET is_valid = false,
		    validation_error = 'INVALID_AUTHOR_FK'
		WHERE job_id = $1
		AND is_valid = true
		AND s.author_id IS NOT NULL
		AND NOT EXISTS (
			SELECT 1 FROM users u WHERE u.id::text = s.author_id
		)
	`
	result, err := r.db.ExecContext(ctx, query, jobID)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	return int(affected), nil
}

// GetValidStagingArticles retrieves valid staging articles in batches
func (r *StagingRepository) GetValidStagingArticles(ctx context.Context, jobID uuid.UUID, batchSize int, callback func([]repository.StagingArticle) error) error {
	query := `
		SELECT * FROM staging_articles
		WHERE job_id = $1 AND is_valid = true AND is_duplicate = false AND processed = false
		ORDER BY staging_id ASC
	`
	rows, err := r.db.QueryxContext(ctx, query, jobID)
	if err != nil {
		return err
	}
	defer rows.Close()

	batch := make([]repository.StagingArticle, 0, batchSize)
	for rows.Next() {
		var article repository.StagingArticle
		if err := rows.StructScan(&article); err != nil {
			return err
		}
		batch = append(batch, article)

		if len(batch) >= batchSize {
			if err := callback(batch); err != nil {
				return err
			}
			batch = make([]repository.StagingArticle, 0, batchSize)
		}
	}

	if len(batch) > 0 {
		if err := callback(batch); err != nil {
			return err
		}
	}

	return rows.Err()
}

// UpdateStagingArticleValidation updates the validation status of a staging article
func (r *StagingRepository) UpdateStagingArticleValidation(ctx context.Context, stagingID int64, isValid bool, errorMsg string) error {
	query := `UPDATE staging_articles SET is_valid = $2, validation_error = $3 WHERE staging_id = $1`
	_, err := r.db.ExecContext(ctx, query, stagingID, isValid, errorMsg)
	return err
}

// CleanupStagingArticles removes staging articles for a completed job
func (r *StagingRepository) CleanupStagingArticles(ctx context.Context, jobID uuid.UUID) error {
	_, err := r.db.ExecContext(ctx, "DELETE FROM staging_articles WHERE job_id = $1", jobID)
	return err
}

// CreateStagingComments inserts comments into the staging table
func (r *StagingRepository) CreateStagingComments(ctx context.Context, jobID uuid.UUID, comments []repository.StagingComment) error {
	if len(comments) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	valueStrings := make([]string, 0, len(comments))
	valueArgs := make([]interface{}, 0, len(comments)*9)

	for i, comment := range comments {
		base := i * 9
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8, base+9,
		))
		valueArgs = append(valueArgs,
			jobID, comment.RowNumber, comment.ID, comment.ArticleID, comment.UserID,
			comment.Body, comment.CreatedAt, comment.ValidationError, comment.IsValid,
		)
	}

	query := fmt.Sprintf(`
		INSERT INTO staging_comments (job_id, row_number, id, article_id, user_id, body, created_at, validation_error, is_valid)
		VALUES %s
	`, strings.Join(valueStrings, ","))

	_, err = tx.ExecContext(ctx, query, valueArgs...)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// MarkDuplicateCommentsInBatch marks duplicate comments within the same batch
func (r *StagingRepository) MarkDuplicateCommentsInBatch(ctx context.Context, jobID uuid.UUID) (int, error) {
	// Comments can have duplicates based on ID only
	query := `
		UPDATE staging_comments s1
		SET is_duplicate = true,
		    validation_error = 'DUPLICATE_ID',
		    is_valid = false
		WHERE job_id = $1
		AND s1.id IS NOT NULL
		AND EXISTS (
			SELECT 1 FROM staging_comments s2
			WHERE s2.job_id = s1.job_id
			AND s2.id = s1.id
			AND s2.staging_id < s1.staging_id
		)
	`
	result, err := r.db.ExecContext(ctx, query, jobID)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	return int(affected), nil
}

// MarkInvalidFKComments marks comments where article_id or user_id don't exist
func (r *StagingRepository) MarkInvalidFKComments(ctx context.Context, jobID uuid.UUID) (int, error) {
	query := `
		UPDATE staging_comments s
		SET is_valid = false,
		    validation_error = CASE
		        WHEN s.article_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM articles a WHERE a.id::text = s.article_id) THEN 'INVALID_ARTICLE_FK'
		        WHEN s.user_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM users u WHERE u.id::text = s.user_id) THEN 'INVALID_USER_FK'
		        ELSE 'INVALID_FK'
		    END
		WHERE job_id = $1
		AND is_valid = true
		AND (
		    (s.article_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM articles a WHERE a.id::text = s.article_id))
		    OR (s.user_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM users u WHERE u.id::text = s.user_id))
		)
	`
	result, err := r.db.ExecContext(ctx, query, jobID)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	return int(affected), nil
}

// GetValidStagingComments retrieves valid staging comments in batches
func (r *StagingRepository) GetValidStagingComments(ctx context.Context, jobID uuid.UUID, batchSize int, callback func([]repository.StagingComment) error) error {
	query := `
		SELECT * FROM staging_comments
		WHERE job_id = $1 AND is_valid = true AND is_duplicate = false AND processed = false
		ORDER BY staging_id ASC
	`
	rows, err := r.db.QueryxContext(ctx, query, jobID)
	if err != nil {
		return err
	}
	defer rows.Close()

	batch := make([]repository.StagingComment, 0, batchSize)
	for rows.Next() {
		var comment repository.StagingComment
		if err := rows.StructScan(&comment); err != nil {
			return err
		}
		batch = append(batch, comment)

		if len(batch) >= batchSize {
			if err := callback(batch); err != nil {
				return err
			}
			batch = make([]repository.StagingComment, 0, batchSize)
		}
	}

	if len(batch) > 0 {
		if err := callback(batch); err != nil {
			return err
		}
	}

	return rows.Err()
}

// UpdateStagingCommentValidation updates the validation status of a staging comment
func (r *StagingRepository) UpdateStagingCommentValidation(ctx context.Context, stagingID int64, isValid bool, errorMsg string) error {
	query := `UPDATE staging_comments SET is_valid = $2, validation_error = $3 WHERE staging_id = $1`
	_, err := r.db.ExecContext(ctx, query, stagingID, isValid, errorMsg)
	return err
}

// CleanupStagingComments removes staging comments for a completed job
func (r *StagingRepository) CleanupStagingComments(ctx context.Context, jobID uuid.UUID) error {
	_, err := r.db.ExecContext(ctx, "DELETE FROM staging_comments WHERE job_id = $1", jobID)
	return err
}

// GetInvalidStagingUsers retrieves invalid staging users for error reporting
func (r *StagingRepository) GetInvalidStagingUsers(ctx context.Context, jobID uuid.UUID) ([]repository.StagingUser, error) {
	var users []repository.StagingUser
	query := `SELECT * FROM staging_users WHERE job_id = $1 AND (is_valid = false OR is_duplicate = true) ORDER BY row_number ASC`
	err := r.db.SelectContext(ctx, &users, query, jobID)
	return users, err
}

// GetInvalidStagingArticles retrieves invalid staging articles for error reporting
func (r *StagingRepository) GetInvalidStagingArticles(ctx context.Context, jobID uuid.UUID) ([]repository.StagingArticle, error) {
	var articles []repository.StagingArticle
	query := `SELECT * FROM staging_articles WHERE job_id = $1 AND (is_valid = false OR is_duplicate = true) ORDER BY row_number ASC`
	err := r.db.SelectContext(ctx, &articles, query, jobID)
	return articles, err
}

// GetInvalidStagingComments retrieves invalid staging comments for error reporting
func (r *StagingRepository) GetInvalidStagingComments(ctx context.Context, jobID uuid.UUID) ([]repository.StagingComment, error) {
	var comments []repository.StagingComment
	query := `SELECT * FROM staging_comments WHERE job_id = $1 AND (is_valid = false OR is_duplicate = true) ORDER BY row_number ASC`
	err := r.db.SelectContext(ctx, &comments, query, jobID)
	return comments, err
}

// MarkProcessed marks staging records as processed
func (r *StagingRepository) MarkUsersProcessed(ctx context.Context, jobID uuid.UUID, stagingIDs []int64) error {
	if len(stagingIDs) == 0 {
		return nil
	}

	query := `UPDATE staging_users SET processed = true WHERE job_id = $1 AND staging_id = ANY($2)`
	_, err := r.db.ExecContext(ctx, query, jobID, stagingIDs)
	return err
}

func (r *StagingRepository) MarkArticlesProcessed(ctx context.Context, jobID uuid.UUID, stagingIDs []int64) error {
	if len(stagingIDs) == 0 {
		return nil
	}

	query := `UPDATE staging_articles SET processed = true WHERE job_id = $1 AND staging_id = ANY($2)`
	_, err := r.db.ExecContext(ctx, query, jobID, stagingIDs)
	return err
}

func (r *StagingRepository) MarkCommentsProcessed(ctx context.Context, jobID uuid.UUID, stagingIDs []int64) error {
	if len(stagingIDs) == 0 {
		return nil
	}

	query := `UPDATE staging_comments SET processed = true WHERE job_id = $1 AND staging_id = ANY($2)`
	_, err := r.db.ExecContext(ctx, query, jobID, stagingIDs)
	return err
}

// CountStagingUsers counts staging users for a job
func (r *StagingRepository) CountStagingUsers(ctx context.Context, jobID uuid.UUID) (total, valid, invalid int, err error) {
	query := `
		SELECT 
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE is_valid = true AND is_duplicate = false) as valid,
			COUNT(*) FILTER (WHERE is_valid = false OR is_duplicate = true) as invalid
		FROM staging_users WHERE job_id = $1
	`
	var result struct {
		Total   int `db:"total"`
		Valid   int `db:"valid"`
		Invalid int `db:"invalid"`
	}
	err = r.db.GetContext(ctx, &result, query, jobID)
	return result.Total, result.Valid, result.Invalid, err
}

// CountStagingArticles counts staging articles for a job
func (r *StagingRepository) CountStagingArticles(ctx context.Context, jobID uuid.UUID) (total, valid, invalid int, err error) {
	query := `
		SELECT 
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE is_valid = true AND is_duplicate = false) as valid,
			COUNT(*) FILTER (WHERE is_valid = false OR is_duplicate = true) as invalid
		FROM staging_articles WHERE job_id = $1
	`
	var result struct {
		Total   int `db:"total"`
		Valid   int `db:"valid"`
		Invalid int `db:"invalid"`
	}
	err = r.db.GetContext(ctx, &result, query, jobID)
	return result.Total, result.Valid, result.Invalid, err
}

// CountStagingComments counts staging comments for a job
func (r *StagingRepository) CountStagingComments(ctx context.Context, jobID uuid.UUID) (total, valid, invalid int, err error) {
	query := `
		SELECT 
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE is_valid = true AND is_duplicate = false) as valid,
			COUNT(*) FILTER (WHERE is_valid = false OR is_duplicate = true) as invalid
		FROM staging_comments WHERE job_id = $1
	`
	var result struct {
		Total   int `db:"total"`
		Valid   int `db:"valid"`
		Invalid int `db:"invalid"`
	}
	err = r.db.GetContext(ctx, &result, query, jobID)
	return result.Total, result.Valid, result.Invalid, err
}

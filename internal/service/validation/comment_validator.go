package validation

import (
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/rohit/bulk-import-export/internal/domain/errors"
	"github.com/rohit/bulk-import-export/internal/domain/models"
)

// CommentValidator validates comment data during import
type CommentValidator struct{}

// NewCommentValidator creates a new CommentValidator
func NewCommentValidator() *CommentValidator {
	return &CommentValidator{}
}

// ValidateCommentImport validates a comment import record
func (v *CommentValidator) ValidateCommentImport(row int, comment *models.CommentImport) []*errors.ValidationError {
	var errs []*errors.ValidationError
	identifier := comment.ID
	if identifier == "" {
		identifier = "row-" + string(rune(row))
	}

	// Validate ID (optional but must be valid UUID if provided)
	if comment.ID != "" {
		if _, err := uuid.Parse(comment.ID); err != nil {
			errs = append(errs, errors.NewValidationError(row, identifier, "id", errors.ErrCodeInvalidUUID, "Invalid UUID format"))
		}
	}

	// Validate article_id (required, must be valid UUID)
	if comment.ArticleID == "" {
		errs = append(errs, errors.NewValidationError(row, identifier, "article_id", errors.ErrCodeMissingField, "Article ID is required"))
	} else if _, err := uuid.Parse(comment.ArticleID); err != nil {
		errs = append(errs, errors.NewValidationError(row, identifier, "article_id", errors.ErrCodeInvalidArticle, "Invalid article UUID format"))
	}

	// Validate user_id (required, must be valid UUID)
	if comment.UserID == "" {
		errs = append(errs, errors.NewValidationError(row, identifier, "user_id", errors.ErrCodeMissingField, "User ID is required"))
	} else if _, err := uuid.Parse(comment.UserID); err != nil {
		errs = append(errs, errors.NewValidationError(row, identifier, "user_id", errors.ErrCodeInvalidUser, "Invalid user UUID format"))
	}

	// Validate body (required, max 500 words)
	if comment.Body == "" {
		errs = append(errs, errors.NewValidationError(row, identifier, "body", errors.ErrCodeBodyEmpty, "Comment body is required"))
	} else {
		wordCount := countWords(comment.Body)
		if wordCount > models.MaxCommentWords {
			errs = append(errs, errors.NewValidationError(row, identifier, "body", errors.ErrCodeBodyTooLong,
				"Comment body exceeds maximum of 500 words"))
		}
	}

	// Validate created_at (optional, must be valid ISO8601 if provided)
	if comment.CreatedAt != "" {
		if _, err := time.Parse(time.RFC3339, comment.CreatedAt); err != nil {
			errs = append(errs, errors.NewValidationError(row, identifier, "created_at", errors.ErrCodeInvalidTimestamp, "Invalid timestamp format (expected ISO8601/RFC3339)"))
		}
	}

	return errs
}

// ConvertToComment converts a validated CommentImport to a Comment model
func (v *CommentValidator) ConvertToComment(ci *models.CommentImport) (*models.Comment, error) {
	comment := &models.Comment{
		Body: strings.TrimSpace(ci.Body),
	}

	// Parse ID
	if ci.ID != "" {
		id, err := uuid.Parse(ci.ID)
		if err != nil {
			return nil, err
		}
		comment.ID = id
	} else {
		comment.ID = uuid.New()
	}

	// Parse article_id
	articleID, err := uuid.Parse(ci.ArticleID)
	if err != nil {
		return nil, err
	}
	comment.ArticleID = articleID

	// Parse user_id
	userID, err := uuid.Parse(ci.UserID)
	if err != nil {
		return nil, err
	}
	comment.UserID = userID

	// Parse created_at
	if ci.CreatedAt != "" {
		t, err := time.Parse(time.RFC3339, ci.CreatedAt)
		if err != nil {
			return nil, err
		}
		comment.CreatedAt = t
	} else {
		comment.CreatedAt = time.Now().UTC()
	}

	return comment, nil
}

// countWords counts the number of words in a string
func countWords(s string) int {
	if s == "" {
		return 0
	}

	count := 0
	inWord := false

	for _, r := range s {
		if unicode.IsSpace(r) || unicode.IsPunct(r) {
			if inWord {
				inWord = false
			}
		} else {
			if !inWord {
				inWord = true
				count++
			}
		}
	}

	return count
}

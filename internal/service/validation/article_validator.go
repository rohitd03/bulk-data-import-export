package validation

import (
	"encoding/json"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rohit/bulk-import-export/internal/domain/errors"
	"github.com/rohit/bulk-import-export/internal/domain/models"
)

// ArticleValidator validates article data during import
type ArticleValidator struct{}

// NewArticleValidator creates a new ArticleValidator
func NewArticleValidator() *ArticleValidator {
	return &ArticleValidator{}
}

// Kebab-case slug pattern
var slugRegex = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// IsValidSlug checks if a string is a valid kebab-case slug
func (v *ArticleValidator) IsValidSlug(slug string) bool {
	if slug == "" {
		return false
	}
	return slugRegex.MatchString(slug)
}

// ValidateArticleImport validates an article import record
func (v *ArticleValidator) ValidateArticleImport(row int, article *models.ArticleImport) []*errors.ValidationError {
	var errs []*errors.ValidationError
	identifier := article.Slug
	if identifier == "" && article.ID != "" {
		identifier = article.ID
	}

	// Validate ID (optional but must be valid UUID if provided)
	if article.ID != "" {
		if _, err := uuid.Parse(article.ID); err != nil {
			errs = append(errs, errors.NewValidationError(row, identifier, "id", errors.ErrCodeInvalidUUID, "Invalid UUID format"))
		}
	}

	// Validate slug (required, must be kebab-case)
	if article.Slug == "" {
		errs = append(errs, errors.NewValidationError(row, identifier, "slug", errors.ErrCodeMissingField, "Slug is required"))
	} else if !v.IsValidSlug(article.Slug) {
		errs = append(errs, errors.NewValidationError(row, identifier, "slug", errors.ErrCodeInvalidSlug, "Slug must be in kebab-case format (lowercase letters, numbers, and hyphens only)"))
	} else if len(article.Slug) > 255 {
		errs = append(errs, errors.NewValidationError(row, identifier, "slug", errors.ErrCodeInvalidSlug, "Slug must be at most 255 characters"))
	}

	// Validate title (required, max 500 chars)
	if article.Title == "" {
		errs = append(errs, errors.NewValidationError(row, identifier, "title", errors.ErrCodeMissingField, "Title is required"))
	} else if len(article.Title) > 500 {
		errs = append(errs, errors.NewValidationError(row, identifier, "title", errors.ErrCodeInvalidTitle, "Title must be at most 500 characters"))
	}

	// Validate body (required)
	if article.Body == "" {
		errs = append(errs, errors.NewValidationError(row, identifier, "body", errors.ErrCodeMissingField, "Body is required"))
	}

	// Validate author_id (required, must be valid UUID)
	if article.AuthorID == "" {
		errs = append(errs, errors.NewValidationError(row, identifier, "author_id", errors.ErrCodeMissingField, "Author ID is required"))
	} else if _, err := uuid.Parse(article.AuthorID); err != nil {
		errs = append(errs, errors.NewValidationError(row, identifier, "author_id", errors.ErrCodeInvalidAuthor, "Invalid author UUID format"))
	}

	// Validate status (must be one of allowed statuses)
	if article.Status == "" {
		errs = append(errs, errors.NewValidationError(row, identifier, "status", errors.ErrCodeMissingField, "Status is required"))
	} else if !models.AllowedArticleStatuses[strings.ToLower(article.Status)] {
		errs = append(errs, errors.NewValidationError(row, identifier, "status", errors.ErrCodeInvalidStatus, "Status must be one of: draft, published, archived"))
	}

	// Validate published_at constraint (draft must not have published_at)
	if strings.ToLower(article.Status) == "draft" && article.PublishedAt != "" {
		errs = append(errs, errors.NewValidationError(row, identifier, "published_at", errors.ErrCodeDraftWithPublished, "Draft articles must not have a published_at date"))
	}

	// Validate published articles must have published_at
	if strings.ToLower(article.Status) == "published" && article.PublishedAt == "" {
		errs = append(errs, errors.NewValidationError(row, identifier, "published_at", errors.ErrCodeMissingPublishedAt, "Published articles must have a published_at date"))
	}

	// Validate published_at format (if provided)
	if article.PublishedAt != "" {
		if _, err := time.Parse(time.RFC3339, article.PublishedAt); err != nil {
			errs = append(errs, errors.NewValidationError(row, identifier, "published_at", errors.ErrCodeInvalidTimestamp, "Invalid timestamp format (expected ISO8601/RFC3339)"))
		}
	}

	// Validate tags (must be valid JSON array if provided)
	if article.Tags != nil && len(article.Tags) > 0 {
		// Tags are already parsed from JSON, so they're valid
		// Just check for reasonable limits
		if len(article.Tags) > 100 {
			errs = append(errs, errors.NewValidationError(row, identifier, "tags", errors.ErrCodeInvalidTags, "Maximum 100 tags allowed"))
		}
		for _, tag := range article.Tags {
			if len(tag) > 50 {
				errs = append(errs, errors.NewValidationError(row, identifier, "tags", errors.ErrCodeInvalidTags, "Each tag must be at most 50 characters"))
				break
			}
		}
	}

	return errs
}

// ConvertToArticle converts a validated ArticleImport to an Article model
func (v *ArticleValidator) ConvertToArticle(ai *models.ArticleImport) (*models.Article, error) {
	article := &models.Article{
		Title:  strings.TrimSpace(ai.Title),
		Body:   ai.Body,
		Status: strings.ToLower(strings.TrimSpace(ai.Status)),
	}

	// Parse ID
	if ai.ID != "" {
		id, err := uuid.Parse(ai.ID)
		if err != nil {
			return nil, err
		}
		article.ID = id
	} else {
		article.ID = uuid.New()
	}

	// Process slug - convert to kebab-case
	slug := strings.ToLower(strings.TrimSpace(ai.Slug))
	slug = strings.ReplaceAll(slug, " ", "-")
	// Remove multiple consecutive hyphens
	for strings.Contains(slug, "--") {
		slug = strings.ReplaceAll(slug, "--", "-")
	}
	article.Slug = slug

	// Parse author_id
	authorID, err := uuid.Parse(ai.AuthorID)
	if err != nil {
		return nil, err
	}
	article.AuthorID = authorID

	// Convert tags to JSON
	if ai.Tags != nil && len(ai.Tags) > 0 {
		tagsJSON, err := json.Marshal(ai.Tags)
		if err != nil {
			return nil, err
		}
		article.Tags = tagsJSON
	} else {
		article.Tags = json.RawMessage("[]")
	}

	// Parse published_at
	if ai.PublishedAt != "" {
		t, err := time.Parse(time.RFC3339, ai.PublishedAt)
		if err != nil {
			return nil, err
		}
		article.PublishedAt = &t
	}

	// Set timestamps
	article.CreatedAt = time.Now().UTC()
	article.UpdatedAt = time.Now().UTC()

	return article, nil
}

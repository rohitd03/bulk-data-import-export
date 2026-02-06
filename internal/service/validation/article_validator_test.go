package validation

import (
	"testing"

	"github.com/rohit/bulk-import-export/internal/domain/models"
)

func TestArticleValidator_ValidateArticleImport(t *testing.T) {
	validator := NewArticleValidator()

	tests := []struct {
		name        string
		article     *models.ArticleImport
		wantValid   bool
		wantErrCode string
	}{
		{
			name: "valid draft article",
			article: &models.ArticleImport{
				ID:       "33e0ef10-374c-4c7c-839c-58d8a772c143",
				Slug:     "streaming-import-design-robust",
				Title:    "Test Article",
				Body:     "This is the body of the article",
				AuthorID: "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Status:   "draft",
			},
			wantValid: true,
		},
		{
			name: "valid published article with published_at",
			article: &models.ArticleImport{
				ID:          "66c86e41-cb9a-4ad6-a927-1d2aef7edfde",
				Slug:        "my-published-article",
				Title:       "Published Article",
				Body:        "Content here",
				AuthorID:    "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Status:      "published",
				PublishedAt: "2024-01-01T00:00:00Z",
				Tags:        []string{"test", "article"},
			},
			wantValid: true,
		},
		{
			name: "invalid slug - contains space",
			article: &models.ArticleImport{
				ID:       "33e0ef10-374c-4c7c-839c-58d8a772c143",
				Slug:     "Draft Fast",
				Title:    "Test Article",
				Body:     "Content",
				AuthorID: "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Status:   "draft",
			},
			wantValid:   false,
			wantErrCode: "INVALID_SLUG",
		},
		{
			name: "invalid slug - uppercase",
			article: &models.ArticleImport{
				ID:       "33e0ef10-374c-4c7c-839c-58d8a772c143",
				Slug:     "Invalid-Slug",
				Title:    "Test Article",
				Body:     "Content",
				AuthorID: "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Status:   "draft",
			},
			wantValid:   false,
			wantErrCode: "INVALID_SLUG",
		},
		{
			name: "missing slug",
			article: &models.ArticleImport{
				ID:       "33e0ef10-374c-4c7c-839c-58d8a772c143",
				Slug:     "",
				Title:    "Test Article",
				Body:     "Content",
				AuthorID: "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Status:   "draft",
			},
			wantValid:   false,
			wantErrCode: "MISSING_FIELD",
		},
		{
			name: "missing title",
			article: &models.ArticleImport{
				ID:       "33e0ef10-374c-4c7c-839c-58d8a772c143",
				Slug:     "valid-slug",
				Title:    "",
				Body:     "Content",
				AuthorID: "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Status:   "draft",
			},
			wantValid:   false,
			wantErrCode: "MISSING_FIELD",
		},
		{
			name: "missing body",
			article: &models.ArticleImport{
				ID:       "33e0ef10-374c-4c7c-839c-58d8a772c143",
				Slug:     "valid-slug",
				Title:    "Test Article",
				Body:     "",
				AuthorID: "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Status:   "draft",
			},
			wantValid:   false,
			wantErrCode: "MISSING_FIELD",
		},
		{
			name: "missing author_id",
			article: &models.ArticleImport{
				ID:       "33e0ef10-374c-4c7c-839c-58d8a772c143",
				Slug:     "valid-slug",
				Title:    "Test Article",
				Body:     "Content",
				AuthorID: "",
				Status:   "draft",
			},
			wantValid:   false,
			wantErrCode: "MISSING_FIELD",
		},
		{
			name: "invalid author_id UUID",
			article: &models.ArticleImport{
				ID:       "33e0ef10-374c-4c7c-839c-58d8a772c143",
				Slug:     "valid-slug",
				Title:    "Test Article",
				Body:     "Content",
				AuthorID: "invalid-uuid",
				Status:   "draft",
			},
			wantValid:   false,
			wantErrCode: "INVALID_AUTHOR",
		},
		{
			name: "invalid status",
			article: &models.ArticleImport{
				ID:       "33e0ef10-374c-4c7c-839c-58d8a772c143",
				Slug:     "valid-slug",
				Title:    "Test Article",
				Body:     "Content",
				AuthorID: "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Status:   "pending",
			},
			wantValid:   false,
			wantErrCode: "INVALID_STATUS",
		},
		{
			name: "draft with published_at (invalid)",
			article: &models.ArticleImport{
				ID:          "33e0ef10-374c-4c7c-839c-58d8a772c143",
				Slug:        "valid-slug",
				Title:       "Test Article",
				Body:        "Content",
				AuthorID:    "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Status:      "draft",
				PublishedAt: "2024-01-01T00:00:00Z",
			},
			wantValid:   false,
			wantErrCode: "INVALID_PUBLISHED_AT",
		},
		{
			name: "published without published_at (invalid)",
			article: &models.ArticleImport{
				ID:       "33e0ef10-374c-4c7c-839c-58d8a772c143",
				Slug:     "valid-slug",
				Title:    "Test Article",
				Body:     "Content",
				AuthorID: "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Status:   "published",
			},
			wantValid:   false,
			wantErrCode: "MISSING_PUBLISHED_AT",
		},
		{
			name: "invalid UUID for id",
			article: &models.ArticleImport{
				ID:       "not-a-valid-uuid",
				Slug:     "valid-slug",
				Title:    "Test Article",
				Body:     "Content",
				AuthorID: "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Status:   "draft",
			},
			wantValid:   false,
			wantErrCode: "INVALID_UUID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validator.ValidateArticleImport(1, tt.article)

			if tt.wantValid && len(errs) > 0 {
				t.Errorf("ValidateArticleImport() expected valid, got errors: %v", errs)
			}

			if !tt.wantValid {
				if len(errs) == 0 {
					t.Errorf("ValidateArticleImport() expected errors, got none")
				} else {
					found := false
					for _, err := range errs {
						if err.Code == tt.wantErrCode {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("ValidateArticleImport() expected error code %s, got codes: ", tt.wantErrCode)
						for _, err := range errs {
							t.Logf("  - %s: %s", err.Code, err.Message)
						}
					}
				}
			}
		})
	}
}

func TestArticleValidator_IsValidSlug(t *testing.T) {
	validator := NewArticleValidator()

	validSlugs := []string{
		"hello-world",
		"my-article-title",
		"test123",
		"a-b-c",
		"single",
	}

	invalidSlugs := []string{
		"Hello World",
		"hello world",
		"UPPERCASE",
		"has_underscore",
		"has.dot",
		"",
	}

	for _, slug := range validSlugs {
		if !validator.IsValidSlug(slug) {
			t.Errorf("IsValidSlug(%q) = false, want true", slug)
		}
	}

	for _, slug := range invalidSlugs {
		if validator.IsValidSlug(slug) {
			t.Errorf("IsValidSlug(%q) = true, want false", slug)
		}
	}
}

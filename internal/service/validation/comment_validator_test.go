package validation

import (
	"strings"
	"testing"

	"github.com/rohit/bulk-import-export/internal/domain/models"
)

func TestCommentValidator_ValidateCommentImport(t *testing.T) {
	validator := NewCommentValidator()

	tests := []struct {
		name        string
		comment     *models.CommentImport
		wantValid   bool
		wantErrCode string
	}{
		{
			name: "valid comment",
			comment: &models.CommentImport{
				ID:        "27d7a89e-d996-4d21-8a07-a7ac4cda5c0b",
				ArticleID: "de9f2098-3528-42a8-bc6a-1f13ee5f6247",
				UserID:    "16b0c588-6f4b-4812-8fea-a39692850695",
				Body:      "This is a valid comment body with some text.",
			},
			wantValid: true,
		},
		{
			name: "missing body",
			comment: &models.CommentImport{
				ID:        "27d7a89e-d996-4d21-8a07-a7ac4cda5c0b",
				ArticleID: "de9f2098-3528-42a8-bc6a-1f13ee5f6247",
				UserID:    "16b0c588-6f4b-4812-8fea-a39692850695",
				Body:      "",
			},
			wantValid:   false,
			wantErrCode: "BODY_EMPTY",
		},
		{
			name: "missing article_id",
			comment: &models.CommentImport{
				ID:        "27d7a89e-d996-4d21-8a07-a7ac4cda5c0b",
				ArticleID: "",
				UserID:    "16b0c588-6f4b-4812-8fea-a39692850695",
				Body:      "Some comment body",
			},
			wantValid:   false,
			wantErrCode: "MISSING_FIELD",
		},
		{
			name: "missing user_id",
			comment: &models.CommentImport{
				ID:        "27d7a89e-d996-4d21-8a07-a7ac4cda5c0b",
				ArticleID: "de9f2098-3528-42a8-bc6a-1f13ee5f6247",
				UserID:    "",
				Body:      "Some comment body",
			},
			wantValid:   false,
			wantErrCode: "MISSING_FIELD",
		},
		{
			name: "invalid article_id UUID",
			comment: &models.CommentImport{
				ID:        "27d7a89e-d996-4d21-8a07-a7ac4cda5c0b",
				ArticleID: "a-missing-12510", // Invalid UUID format
				UserID:    "16b0c588-6f4b-4812-8fea-a39692850695",
				Body:      "Some comment body",
			},
			wantValid:   false,
			wantErrCode: "INVALID_ARTICLE",
		},
		{
			name: "invalid user_id UUID",
			comment: &models.CommentImport{
				ID:        "27d7a89e-d996-4d21-8a07-a7ac4cda5c0b",
				ArticleID: "de9f2098-3528-42a8-bc6a-1f13ee5f6247",
				UserID:    "invalid-user-id",
				Body:      "Some comment body",
			},
			wantValid:   false,
			wantErrCode: "INVALID_USER",
		},
		{
			name: "body too long (over 500 words)",
			comment: &models.CommentImport{
				ID:        "27d7a89e-d996-4d21-8a07-a7ac4cda5c0b",
				ArticleID: "de9f2098-3528-42a8-bc6a-1f13ee5f6247",
				UserID:    "16b0c588-6f4b-4812-8fea-a39692850695",
				Body:      strings.Repeat("word ", 501), // 501 words
			},
			wantValid:   false,
			wantErrCode: "BODY_TOO_LONG",
		},
		{
			name: "body at exactly 500 words (valid)",
			comment: &models.CommentImport{
				ID:        "27d7a89e-d996-4d21-8a07-a7ac4cda5c0b",
				ArticleID: "de9f2098-3528-42a8-bc6a-1f13ee5f6247",
				UserID:    "16b0c588-6f4b-4812-8fea-a39692850695",
				Body:      strings.TrimSpace(strings.Repeat("word ", 500)), // 500 words
			},
			wantValid: true,
		},
		{
			name: "comment without ID (valid - ID is optional)",
			comment: &models.CommentImport{
				ID:        "",
				ArticleID: "de9f2098-3528-42a8-bc6a-1f13ee5f6247",
				UserID:    "16b0c588-6f4b-4812-8fea-a39692850695",
				Body:      "Valid comment",
			},
			wantValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validator.ValidateCommentImport(1, tt.comment)

			if tt.wantValid && len(errs) > 0 {
				t.Errorf("ValidateCommentImport() expected valid, got errors: %v", errs)
			}

			if !tt.wantValid {
				if len(errs) == 0 {
					t.Errorf("ValidateCommentImport() expected errors, got none")
				} else {
					found := false
					for _, err := range errs {
						if err.Code == tt.wantErrCode {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("ValidateCommentImport() expected error code %s, got codes: ", tt.wantErrCode)
						for _, err := range errs {
							t.Logf("  - %s: %s", err.Code, err.Message)
						}
					}
				}
			}
		})
	}
}

func TestCommentValidator_WordCount(t *testing.T) {
	tests := []struct {
		text      string
		wantCount int
	}{
		{"hello world", 2},
		{"one two three four five", 5},
		{"", 0},
		{"   ", 0},
		{"word", 1},
		{"  multiple   spaces   between   words  ", 4},
		{strings.Repeat("word ", 500), 500},
	}

	for _, tt := range tests {
		t.Run(tt.text[:min(20, len(tt.text))], func(t *testing.T) {
			words := strings.Fields(tt.text)
			if len(words) != tt.wantCount {
				t.Errorf("WordCount(%q) = %d, want %d", tt.text[:min(20, len(tt.text))], len(words), tt.wantCount)
			}
		})
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

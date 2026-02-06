package parsers

import (
	"strings"
	"testing"

	"github.com/rohit/bulk-import-export/internal/domain/models"
)

func TestNDJSONParser_ParseArticles(t *testing.T) {
	ndjson := `{"id":"de9f2098-3528-42a8-bc6a-1f13ee5f6247","title":"Test Article","slug":"test-article","body":"Article body content","author_id":"16b0c588-6f4b-4812-8fea-a39692850695","status":"published","published_at":"2024-01-15T10:30:00Z"}
{"id":"ab123456-1234-5678-90ab-cdef12345678","title":"Second Article","slug":"second-article","body":"Second article body","author_id":"27c1d699-7f5c-5823-9feb-b40793961706","status":"draft"}`

	reader := strings.NewReader(ndjson)
	parser := NewNDJSONParser(reader)

	var articles []*models.ArticleImport
	err := parser.ParseArticles(func(row int, article *models.ArticleImport, rawJSON string) error {
		if article != nil {
			articles = append(articles, article)
		}
		return nil
	})

	if err != nil {
		t.Errorf("ParseArticles() error: %v", err)
	}

	if len(articles) != 2 {
		t.Fatalf("ParseArticles() got %d articles, want 2", len(articles))
	}

	// Verify first article
	if articles[0].Slug != "test-article" {
		t.Errorf("First article slug = %s, want test-article", articles[0].Slug)
	}
	if articles[0].Status != "published" {
		t.Errorf("First article status = %s, want published", articles[0].Status)
	}

	// Verify second article
	if articles[1].Status != "draft" {
		t.Errorf("Second article status = %s, want draft", articles[1].Status)
	}
}

func TestNDJSONParser_ParseArticles_InvalidSlug(t *testing.T) {
	// Test with invalid slug (contains space like "Draft Fast" from test data)
	ndjson := `{"id":"de9f2098-3528-42a8-bc6a-1f13ee5f6247","title":"Test","slug":"Draft Fast","body":"Body","author_id":"16b0c588-6f4b-4812-8fea-a39692850695","status":"draft"}`

	reader := strings.NewReader(ndjson)
	parser := NewNDJSONParser(reader)

	var articles []*models.ArticleImport
	err := parser.ParseArticles(func(row int, article *models.ArticleImport, rawJSON string) error {
		if article != nil {
			articles = append(articles, article)
		}
		return nil
	})

	if err != nil {
		t.Errorf("ParseArticles() error: %v", err)
	}

	// Parser should return the article (validation happens separately)
	if len(articles) != 1 {
		t.Fatalf("ParseArticles() got %d articles, want 1", len(articles))
	}

	// Should preserve the invalid slug for validation layer
	if articles[0].Slug != "Draft Fast" {
		t.Errorf("Article slug = %s, want 'Draft Fast'", articles[0].Slug)
	}
}

func TestNDJSONParser_ParseArticles_MalformedJSON(t *testing.T) {
	// Test with malformed JSON
	ndjson := `{"id":"valid","title":"Test","slug":"test-slug"
not valid json at all
{"id":"also-valid","title":"Another","slug":"another-slug","status":"draft"}`

	reader := strings.NewReader(ndjson)
	parser := NewNDJSONParser(reader)

	var articles []*models.ArticleImport
	var parseErrors int

	err := parser.ParseArticles(func(row int, article *models.ArticleImport, rawJSON string) error {
		if article == nil {
			parseErrors++
		} else {
			articles = append(articles, article)
		}
		return nil
	})

	if err != nil {
		t.Errorf("ParseArticles() error: %v", err)
	}

	// Should have 2 parse errors (2 malformed lines)
	if parseErrors != 2 {
		t.Errorf("ParseArticles() got %d parse errors, want 2", parseErrors)
	}

	// Should still parse the valid line
	if len(articles) != 1 {
		t.Errorf("ParseArticles() got %d articles, want 1", len(articles))
	}
}

func TestNDJSONParser_ParseArticles_EmptyLines(t *testing.T) {
	// Test with empty lines - they should be skipped
	ndjson := `{"id":"valid-1","title":"First","slug":"first","status":"draft"}

{"id":"valid-2","title":"Second","slug":"second","status":"draft"}

`

	reader := strings.NewReader(ndjson)
	parser := NewNDJSONParser(reader)

	var articles []*models.ArticleImport
	err := parser.ParseArticles(func(row int, article *models.ArticleImport, rawJSON string) error {
		if article != nil {
			articles = append(articles, article)
		}
		return nil
	})

	if err != nil {
		t.Errorf("ParseArticles() error: %v", err)
	}

	if len(articles) != 2 {
		t.Errorf("ParseArticles() got %d articles, want 2", len(articles))
	}
}

func TestNDJSONParser_ParseComments(t *testing.T) {
	ndjson := `{"id":"cm_27d7a89e-d996-4d21-8a07-a7ac4cda5c0b","article_id":"de9f2098-3528-42a8-bc6a-1f13ee5f6247","user_id":"16b0c588-6f4b-4812-8fea-a39692850695","body":"This is a comment"}
{"id":"cm_38e8b90f-e107-5e32-9b18-c51804962817","article_id":"ab123456-1234-5678-90ab-cdef12345678","user_id":"27c1d699-7f5c-5823-9feb-b40793961706","body":"Another comment"}`

	reader := strings.NewReader(ndjson)
	parser := NewNDJSONParser(reader)

	var comments []*models.CommentImport
	err := parser.ParseComments(func(row int, comment *models.CommentImport, rawJSON string) error {
		if comment != nil {
			comments = append(comments, comment)
		}
		return nil
	})

	if err != nil {
		t.Errorf("ParseComments() error: %v", err)
	}

	if len(comments) != 2 {
		t.Fatalf("ParseComments() got %d comments, want 2", len(comments))
	}

	// Verify first comment
	if comments[0].Body != "This is a comment" {
		t.Errorf("First comment body = %s, want 'This is a comment'", comments[0].Body)
	}

	// Verify second comment
	if comments[1].Body != "Another comment" {
		t.Errorf("Second comment body = %s, want 'Another comment'", comments[1].Body)
	}
}

func TestNDJSONParser_ParseComments_MissingBody(t *testing.T) {
	// Test with missing body (as seen in test data)
	ndjson := `{"id":"cm_test","article_id":"de9f2098-3528-42a8-bc6a-1f13ee5f6247","user_id":"16b0c588-6f4b-4812-8fea-a39692850695"}
{"id":"cm_test2","article_id":"ab123456-1234-5678-90ab-cdef12345678","user_id":"27c1d699-7f5c-5823-9feb-b40793961706","body":""}`

	reader := strings.NewReader(ndjson)
	parser := NewNDJSONParser(reader)

	var comments []*models.CommentImport
	err := parser.ParseComments(func(row int, comment *models.CommentImport, rawJSON string) error {
		if comment != nil {
			comments = append(comments, comment)
		}
		return nil
	})

	if err != nil {
		t.Errorf("ParseComments() error: %v", err)
	}

	// Parser returns all records (validation happens separately)
	if len(comments) != 2 {
		t.Fatalf("ParseComments() got %d comments, want 2", len(comments))
	}

	// First comment should have empty body
	if comments[0].Body != "" {
		t.Errorf("First comment body = %s, want empty string", comments[0].Body)
	}

	// Second comment should also have empty body
	if comments[1].Body != "" {
		t.Errorf("Second comment body = %s, want empty string", comments[1].Body)
	}
}

func TestNDJSONParser_ParseGeneric(t *testing.T) {
	ndjson := `{"type":"user","id":"123","name":"Test"}
{"type":"article","id":"456","title":"Article Title"}`

	reader := strings.NewReader(ndjson)
	parser := NewNDJSONParser(reader)

	var records []map[string]interface{}
	err := parser.ParseGeneric(func(row int, data map[string]interface{}, rawJSON string) error {
		if data != nil {
			records = append(records, data)
		}
		return nil
	})

	if err != nil {
		t.Errorf("ParseGeneric() error: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("ParseGeneric() got %d records, want 2", len(records))
	}

	if records[0]["type"] != "user" {
		t.Errorf("First record type = %v, want 'user'", records[0]["type"])
	}
	if records[1]["type"] != "article" {
		t.Errorf("Second record type = %v, want 'article'", records[1]["type"])
	}
}

func TestNDJSONParser_TotalLines(t *testing.T) {
	ndjson := `{"line":1}
{"line":2}
{"line":3}`

	reader := strings.NewReader(ndjson)
	parser := NewNDJSONParser(reader)

	// Before parsing, line number should be 0
	if parser.TotalLines() != 0 {
		t.Errorf("TotalLines() before parsing = %d, want 0", parser.TotalLines())
	}

	err := parser.ParseGeneric(func(row int, data map[string]interface{}, rawJSON string) error {
		return nil
	})

	if err != nil {
		t.Errorf("ParseGeneric() error: %v", err)
	}

	// After parsing, line number should be 3
	if parser.TotalLines() != 3 {
		t.Errorf("TotalLines() after parsing = %d, want 3", parser.TotalLines())
	}
}

func TestNDJSONParser_ParseUsers(t *testing.T) {
	ndjson := `{"id":"16b0c588-6f4b-4812-8fea-a39692850695","email":"test@example.com","name":"Test User","role":"admin","active":"true","created_at":"2024-01-01T00:00:00Z"}
{"id":"27c1d699-7f5c-5823-9feb-b40793961706","email":"user2@example.com","name":"User Two","role":"reader","active":"false"}`

	reader := strings.NewReader(ndjson)
	parser := NewNDJSONParser(reader)

	var users []*models.UserImport
	err := parser.ParseUsers(func(row int, user *models.UserImport, rawJSON string) error {
		if user != nil {
			users = append(users, user)
		}
		return nil
	})

	if err != nil {
		t.Errorf("ParseUsers() error: %v", err)
	}

	if len(users) != 2 {
		t.Fatalf("ParseUsers() got %d users, want 2", len(users))
	}

	// Verify first user
	if users[0].Email != "test@example.com" {
		t.Errorf("First user email = %s, want test@example.com", users[0].Email)
	}
	if users[0].Role != "admin" {
		t.Errorf("First user role = %s, want admin", users[0].Role)
	}
	if users[0].Active != "true" {
		t.Errorf("First user active = %s, want true", users[0].Active)
	}

	// Verify second user
	if users[1].Email != "user2@example.com" {
		t.Errorf("Second user email = %s, want user2@example.com", users[1].Email)
	}
	if users[1].Role != "reader" {
		t.Errorf("Second user role = %s, want reader", users[1].Role)
	}
}

func TestNDJSONParser_ParseUsers_MalformedJSON(t *testing.T) {
	ndjson := `{"id":"valid-1","email":"valid@test.com","name":"Valid","role":"admin","active":"true"}
{invalid json line}
{"id":"valid-2","email":"valid2@test.com","name":"Valid2","role":"reader","active":"false"}`

	reader := strings.NewReader(ndjson)
	parser := NewNDJSONParser(reader)

	var validUsers int
	var parseErrors int

	err := parser.ParseUsers(func(row int, user *models.UserImport, rawJSON string) error {
		if user == nil {
			parseErrors++
		} else {
			validUsers++
		}
		return nil
	})

	if err != nil {
		t.Errorf("ParseUsers() error: %v", err)
	}

	if validUsers != 2 {
		t.Errorf("ParseUsers() got %d valid users, want 2", validUsers)
	}
	if parseErrors != 1 {
		t.Errorf("ParseUsers() got %d parse errors, want 1", parseErrors)
	}
}

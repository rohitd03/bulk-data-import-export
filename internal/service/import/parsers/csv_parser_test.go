package parsers

import (
	"strings"
	"testing"

	"github.com/rohit/bulk-import-export/internal/domain/models"
)

func TestCSVParser_NewCSVParser(t *testing.T) {
	tests := []struct {
		name    string
		csv     string
		wantErr bool
	}{
		{
			name:    "valid headers",
			csv:     "id,email,name,role,active,created_at\n",
			wantErr: false,
		},
		{
			name:    "empty CSV",
			csv:     "",
			wantErr: true,
		},
		{
			name:    "headers with whitespace",
			csv:     " id , email , name , role \n",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.csv)
			parser, err := NewCSVParser(reader)

			if tt.wantErr {
				if err == nil {
					t.Errorf("NewCSVParser() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("NewCSVParser() unexpected error: %v", err)
				}
				if parser == nil {
					t.Errorf("NewCSVParser() returned nil parser")
				}
			}
		})
	}
}

func TestCSVParser_ParseUsers(t *testing.T) {
	csvData := `id,email,name,role,active,created_at
16b0c588-6f4b-4812-8fea-a39692850695,alice@example.com,Alice Smith,admin,true,2024-01-15T10:30:00Z
27c1d699-7f5c-5823-9feb-b40793961706,bob@example.com,Bob Jones,reader,false,2024-02-20T14:45:00Z`

	reader := strings.NewReader(csvData)
	parser, err := NewCSVParser(reader)
	if err != nil {
		t.Fatalf("NewCSVParser() error: %v", err)
	}

	var users []*models.UserImport

	err = parser.ParseUsers(func(row int, user *models.UserImport) error {
		users = append(users, user)
		return nil
	})

	if err != nil {
		t.Errorf("ParseUsers() error: %v", err)
	}

	if len(users) != 2 {
		t.Errorf("ParseUsers() got %d users, want 2", len(users))
	}

	// Verify first user
	if len(users) > 0 {
		if users[0].Email != "alice@example.com" {
			t.Errorf("First user email = %s, want alice@example.com", users[0].Email)
		}
		if users[0].Role != "admin" {
			t.Errorf("First user role = %s, want admin", users[0].Role)
		}
	}

	// Verify second user
	if len(users) > 1 {
		if users[1].Email != "bob@example.com" {
			t.Errorf("Second user email = %s, want bob@example.com", users[1].Email)
		}
		if users[1].Active != "false" {
			t.Errorf("Second user active = %s, want false", users[1].Active)
		}
	}
}

func TestCSVParser_ParseUsers_InvalidData(t *testing.T) {
	// Test CSV with missing fields (as seen in test data - first row has empty id)
	csvData := `id,email,name,role,active,created_at
,foo@bar,Test User,manager,true,2024-01-01T00:00:00Z
valid-id,valid@email.com,Valid User,admin,true,2024-01-01T00:00:00Z`

	reader := strings.NewReader(csvData)
	parser, err := NewCSVParser(reader)
	if err != nil {
		t.Fatalf("NewCSVParser() error: %v", err)
	}

	var users []*models.UserImport
	err = parser.ParseUsers(func(row int, user *models.UserImport) error {
		users = append(users, user)
		return nil
	})

	if err != nil {
		t.Errorf("ParseUsers() error: %v", err)
	}

	// Parser should still return both records (validation happens separately)
	if len(users) != 2 {
		t.Errorf("ParseUsers() got %d users, want 2", len(users))
	}

	// First user should have empty ID
	if len(users) > 0 && users[0].ID != "" {
		t.Errorf("First user ID = %s, want empty string", users[0].ID)
	}
}

func TestCSVParser_CaseInsensitiveHeaders(t *testing.T) {
	// Test that headers are case-insensitive
	csvData := `ID,EMAIL,NAME,ROLE,ACTIVE
123,test@test.com,Test,admin,true`

	reader := strings.NewReader(csvData)
	parser, err := NewCSVParser(reader)
	if err != nil {
		t.Fatalf("NewCSVParser() error: %v", err)
	}

	var users []*models.UserImport
	err = parser.ParseUsers(func(row int, user *models.UserImport) error {
		users = append(users, user)
		return nil
	})

	if err != nil {
		t.Errorf("ParseUsers() error: %v", err)
	}

	if len(users) != 1 {
		t.Fatalf("ParseUsers() got %d users, want 1", len(users))
	}

	if users[0].Email != "test@test.com" {
		t.Errorf("User email = %s, want test@test.com", users[0].Email)
	}
}

func TestCSVParser_TotalLines(t *testing.T) {
	csvData := `id,email,name,role,active
1,a@test.com,A,admin,true
2,b@test.com,B,reader,true
3,c@test.com,C,author,true`

	reader := strings.NewReader(csvData)
	parser, err := NewCSVParser(reader)
	if err != nil {
		t.Fatalf("NewCSVParser() error: %v", err)
	}

	// After reading headers, line number should be 1
	if parser.TotalLines() != 1 {
		t.Errorf("TotalLines() after header = %d, want 1", parser.TotalLines())
	}

	err = parser.ParseUsers(func(row int, user *models.UserImport) error {
		return nil
	})

	if err != nil {
		t.Errorf("ParseUsers() error: %v", err)
	}

	// After parsing all rows, line number should be 4 (header + 3 data rows)
	if parser.TotalLines() != 4 {
		t.Errorf("TotalLines() after parsing = %d, want 4", parser.TotalLines())
	}
}

func TestCSVParser_ParseArticles(t *testing.T) {
	csvData := `id,slug,title,body,author_id,tags,status,published_at
de9f2098-3528-42a8-bc6a-1f13ee5f6247,test-article,Test Article,Article body,16b0c588-6f4b-4812-8fea-a39692850695,"tech,golang",published,2024-01-15T10:30:00Z
ab123456-1234-5678-90ab-cdef12345678,draft-article,Draft Article,Draft body,27c1d699-7f5c-5823-9feb-b40793961706,testing,draft,`

	reader := strings.NewReader(csvData)
	parser, err := NewCSVParser(reader)
	if err != nil {
		t.Fatalf("NewCSVParser() error: %v", err)
	}

	var articles []*models.ArticleImport
	err = parser.ParseArticles(func(row int, article *models.ArticleImport) error {
		articles = append(articles, article)
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
	if len(articles[0].Tags) != 2 || articles[0].Tags[0] != "tech" || articles[0].Tags[1] != "golang" {
		t.Errorf("First article tags = %v, want [tech golang]", articles[0].Tags)
	}

	// Verify second article
	if articles[1].Slug != "draft-article" {
		t.Errorf("Second article slug = %s, want draft-article", articles[1].Slug)
	}
	if articles[1].Status != "draft" {
		t.Errorf("Second article status = %s, want draft", articles[1].Status)
	}
}

func TestCSVParser_ParseComments(t *testing.T) {
	csvData := `id,article_id,user_id,body,created_at
cm_123,de9f2098-3528-42a8-bc6a-1f13ee5f6247,16b0c588-6f4b-4812-8fea-a39692850695,This is a comment,2024-01-15T10:30:00Z
cm_456,ab123456-1234-5678-90ab-cdef12345678,27c1d699-7f5c-5823-9feb-b40793961706,Another comment,2024-02-20T14:45:00Z`

	reader := strings.NewReader(csvData)
	parser, err := NewCSVParser(reader)
	if err != nil {
		t.Fatalf("NewCSVParser() error: %v", err)
	}

	var comments []*models.CommentImport
	err = parser.ParseComments(func(row int, comment *models.CommentImport) error {
		comments = append(comments, comment)
		return nil
	})

	if err != nil {
		t.Errorf("ParseComments() error: %v", err)
	}

	if len(comments) != 2 {
		t.Fatalf("ParseComments() got %d comments, want 2", len(comments))
	}

	// Verify first comment
	if comments[0].ID != "cm_123" {
		t.Errorf("First comment ID = %s, want cm_123", comments[0].ID)
	}
	if comments[0].Body != "This is a comment" {
		t.Errorf("First comment body = %s, want 'This is a comment'", comments[0].Body)
	}

	// Verify second comment
	if comments[1].ID != "cm_456" {
		t.Errorf("Second comment ID = %s, want cm_456", comments[1].ID)
	}
	if comments[1].UserID != "27c1d699-7f5c-5823-9feb-b40793961706" {
		t.Errorf("Second comment user_id = %s, want 27c1d699-7f5c-5823-9feb-b40793961706", comments[1].UserID)
	}
}

package integration_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/rohit/bulk-import-export/internal/domain/models"
	"github.com/rohit/bulk-import-export/internal/service/import/parsers"
	"github.com/rohit/bulk-import-export/internal/service/validation"
)

// getTestDataDir returns the path to the test data directory
func getTestDataDir() string {
	// Get the path relative to this test file
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	dir := filepath.Dir(filename)
	// Go up two directories to reach the project root
	return filepath.Join(dir, "..", "..", "import_testdata_all_in_one")
}

// TestIntegration_UserImport_WithRealData tests user import with the actual test data
func TestIntegration_UserImport_WithRealData(t *testing.T) {
	testDataDir := getTestDataDir()
	// Check if test data exists
	csvPath := filepath.Join(testDataDir, "users_huge.csv")
	if _, err := os.Stat(csvPath); os.IsNotExist(err) {
		t.Skipf("Test data file not found: %s", csvPath)
	}

	file, err := os.Open(csvPath)
	if err != nil {
		t.Fatalf("Failed to open test data: %v", err)
	}
	defer file.Close()

	parser, err := parsers.NewCSVParser(file)
	if err != nil {
		t.Fatalf("Failed to create CSV parser: %v", err)
	}

	validator := validation.NewUserValidator()

	stats := struct {
		totalRecords    int
		validRecords    int
		invalidRecords  int
		invalidEmails   int
		invalidRoles    int
		missingFields   int
		invalidUUIDs    int
		duplicateEmails int
	}{}

	seenEmails := make(map[string]int)

	err = parser.ParseUsers(func(row int, user *models.UserImport) error {
		stats.totalRecords++

		// Track duplicates
		if user.Email != "" {
			seenEmails[strings.ToLower(user.Email)]++
		}

		errs := validator.ValidateUserImport(row, user)
		if len(errs) > 0 {
			stats.invalidRecords++
			for _, e := range errs {
				switch e.Code {
				case "INVALID_EMAIL":
					stats.invalidEmails++
				case "INVALID_ROLE":
					stats.invalidRoles++
				case "MISSING_FIELD":
					stats.missingFields++
				case "INVALID_UUID":
					stats.invalidUUIDs++
				}
			}
		} else {
			stats.validRecords++
		}
		return nil
	})

	if err != nil {
		t.Errorf("ParseUsers() error: %v", err)
	}

	// Count duplicates
	for _, count := range seenEmails {
		if count > 1 {
			stats.duplicateEmails += count - 1
		}
	}

	t.Logf("User Import Statistics:")
	t.Logf("  Total records: %d", stats.totalRecords)
	t.Logf("  Valid records: %d", stats.validRecords)
	t.Logf("  Invalid records: %d", stats.invalidRecords)
	t.Logf("  Invalid emails: %d", stats.invalidEmails)
	t.Logf("  Invalid roles: %d", stats.invalidRoles)
	t.Logf("  Missing fields: %d", stats.missingFields)
	t.Logf("  Invalid UUIDs: %d", stats.invalidUUIDs)
	t.Logf("  Duplicate emails: %d", stats.duplicateEmails)

	// Test expectations based on test data description
	if stats.totalRecords == 0 {
		t.Error("Expected to parse some records from test data")
	}

	// Test data should have some invalid records (manager role, foo@bar emails, etc.)
	if stats.invalidRecords == 0 {
		t.Error("Expected some invalid records in test data (invalid emails, roles, etc.)")
	}
}

// TestIntegration_ArticleImport_WithRealData tests article import with the actual test data
func TestIntegration_ArticleImport_WithRealData(t *testing.T) {
	testDataDir := getTestDataDir()
	// Check if test data exists
	ndjsonPath := filepath.Join(testDataDir, "articles_huge.ndjson")
	if _, err := os.Stat(ndjsonPath); os.IsNotExist(err) {
		t.Skipf("Test data file not found: %s", ndjsonPath)
	}

	file, err := os.Open(ndjsonPath)
	if err != nil {
		t.Fatalf("Failed to open test data: %v", err)
	}
	defer file.Close()

	parser := parsers.NewNDJSONParser(file)
	validator := validation.NewArticleValidator()

	stats := struct {
		totalRecords     int
		validRecords     int
		invalidRecords   int
		parseErrors      int
		invalidSlugs     int
		invalidAuthorIDs int
		missingFields    int
	}{}

	err = parser.ParseArticles(func(row int, article *models.ArticleImport, rawJSON string) error {
		stats.totalRecords++

		if article == nil {
			stats.parseErrors++
			return nil
		}

		errs := validator.ValidateArticleImport(row, article)
		if len(errs) > 0 {
			stats.invalidRecords++
			for _, e := range errs {
				switch e.Code {
				case "INVALID_SLUG":
					stats.invalidSlugs++
				case "INVALID_AUTHOR":
					stats.invalidAuthorIDs++
				case "MISSING_FIELD":
					stats.missingFields++
				}
			}
		} else {
			stats.validRecords++
		}
		return nil
	})

	if err != nil {
		t.Errorf("ParseArticles() error: %v", err)
	}

	t.Logf("Article Import Statistics:")
	t.Logf("  Total records: %d", stats.totalRecords)
	t.Logf("  Valid records: %d", stats.validRecords)
	t.Logf("  Invalid records: %d", stats.invalidRecords)
	t.Logf("  Parse errors: %d", stats.parseErrors)
	t.Logf("  Invalid slugs: %d", stats.invalidSlugs)
	t.Logf("  Invalid author IDs: %d", stats.invalidAuthorIDs)
	t.Logf("  Missing fields: %d", stats.missingFields)

	// Test expectations
	if stats.totalRecords == 0 {
		t.Error("Expected to parse some records from test data")
	}

	// Test data should have invalid slugs with spaces like "Draft Fast"
	if stats.invalidSlugs == 0 {
		t.Error("Expected some invalid slugs in test data (slugs with spaces)")
	}
}

// TestIntegration_CommentImport_WithRealData tests comment import with the actual test data
func TestIntegration_CommentImport_WithRealData(t *testing.T) {
	testDataDir := getTestDataDir()
	// Check if test data exists
	ndjsonPath := filepath.Join(testDataDir, "comments_huge.ndjson")
	if _, err := os.Stat(ndjsonPath); os.IsNotExist(err) {
		t.Skipf("Test data file not found: %s", ndjsonPath)
	}

	file, err := os.Open(ndjsonPath)
	if err != nil {
		t.Fatalf("Failed to open test data: %v", err)
	}
	defer file.Close()

	parser := parsers.NewNDJSONParser(file)
	validator := validation.NewCommentValidator()

	stats := struct {
		totalRecords      int
		validRecords      int
		invalidRecords    int
		parseErrors       int
		emptyBodies       int
		invalidArticleIDs int
		invalidUserIDs    int
		bodyTooLong       int
	}{}

	err = parser.ParseComments(func(row int, comment *models.CommentImport, rawJSON string) error {
		stats.totalRecords++

		if comment == nil {
			stats.parseErrors++
			return nil
		}

		errs := validator.ValidateCommentImport(row, comment)
		if len(errs) > 0 {
			stats.invalidRecords++
			for _, e := range errs {
				switch e.Code {
				case "BODY_EMPTY":
					stats.emptyBodies++
				case "INVALID_ARTICLE":
					stats.invalidArticleIDs++
				case "INVALID_USER":
					stats.invalidUserIDs++
				case "BODY_TOO_LONG":
					stats.bodyTooLong++
				}
			}
		} else {
			stats.validRecords++
		}
		return nil
	})

	if err != nil {
		t.Errorf("ParseComments() error: %v", err)
	}

	t.Logf("Comment Import Statistics:")
	t.Logf("  Total records: %d", stats.totalRecords)
	t.Logf("  Valid records: %d", stats.validRecords)
	t.Logf("  Invalid records: %d", stats.invalidRecords)
	t.Logf("  Parse errors: %d", stats.parseErrors)
	t.Logf("  Empty bodies: %d", stats.emptyBodies)
	t.Logf("  Invalid article IDs: %d", stats.invalidArticleIDs)
	t.Logf("  Invalid user IDs: %d", stats.invalidUserIDs)
	t.Logf("  Body too long: %d", stats.bodyTooLong)

	// Test expectations
	if stats.totalRecords == 0 {
		t.Error("Expected to parse some records from test data")
	}

	// Test data should have comments with missing body
	if stats.emptyBodies == 0 {
		t.Error("Expected some empty bodies in test data")
	}
}

// TestIntegration_ValidationErrorCodes verifies error code consistency
func TestIntegration_ValidationErrorCodes(t *testing.T) {
	// Test that error codes follow the expected pattern
	userValidator := validation.NewUserValidator()
	articleValidator := validation.NewArticleValidator()
	commentValidator := validation.NewCommentValidator()

	// Test user validation errors
	t.Run("User validation error codes", func(t *testing.T) {
		testCases := []struct {
			name string
			user *models.UserImport
			code string
		}{
			{
				name: "invalid email",
				user: &models.UserImport{Name: "Test", Email: "foo@bar", Role: "admin", Active: "true"},
				code: "INVALID_EMAIL",
			},
			{
				name: "invalid role",
				user: &models.UserImport{Name: "Test", Email: "test@example.com", Role: "manager", Active: "true"},
				code: "INVALID_ROLE",
			},
		}

		for _, tc := range testCases {
			errs := userValidator.ValidateUserImport(1, tc.user)
			if len(errs) == 0 {
				t.Errorf("%s: expected validation error", tc.name)
				continue
			}
			found := false
			for _, e := range errs {
				if e.Code == tc.code {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("%s: expected error code %s", tc.name, tc.code)
			}
		}
	})

	// Test article validation errors
	t.Run("Article validation error codes", func(t *testing.T) {
		testCases := []struct {
			name    string
			article *models.ArticleImport
			code    string
		}{
			{
				name:    "invalid slug with space",
				article: &models.ArticleImport{Title: "Test", Slug: "Draft Fast", AuthorID: "16b0c588-6f4b-4812-8fea-a39692850695", Status: "draft"},
				code:    "INVALID_SLUG",
			},
		}

		for _, tc := range testCases {
			errs := articleValidator.ValidateArticleImport(1, tc.article)
			if len(errs) == 0 {
				t.Errorf("%s: expected validation error", tc.name)
				continue
			}
			found := false
			for _, e := range errs {
				if e.Code == tc.code {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("%s: expected error code %s, got errors: %v", tc.name, tc.code, errs)
			}
		}
	})

	// Test comment validation errors
	t.Run("Comment validation error codes", func(t *testing.T) {
		testCases := []struct {
			name    string
			comment *models.CommentImport
			code    string
		}{
			{
				name:    "empty body",
				comment: &models.CommentImport{ArticleID: "de9f2098-3528-42a8-bc6a-1f13ee5f6247", UserID: "16b0c588-6f4b-4812-8fea-a39692850695", Body: ""},
				code:    "BODY_EMPTY",
			},
		}

		for _, tc := range testCases {
			errs := commentValidator.ValidateCommentImport(1, tc.comment)
			if len(errs) == 0 {
				t.Errorf("%s: expected validation error", tc.name)
				continue
			}
			found := false
			for _, e := range errs {
				if e.Code == tc.code {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("%s: expected error code %s", tc.name, tc.code)
			}
		}
	})
}

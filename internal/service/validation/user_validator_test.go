package validation

import (
	"testing"

	"github.com/rohit/bulk-import-export/internal/domain/models"
)

func TestUserValidator_ValidateUserImport(t *testing.T) {
	validator := NewUserValidator()

	tests := []struct {
		name        string
		user        *models.UserImport
		wantValid   bool
		wantErrCode string
	}{
		{
			name: "valid user with all fields",
			user: &models.UserImport{
				ID:     "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Email:  "user@example.com",
				Name:   "Test User",
				Role:   "admin",
				Active: "true",
			},
			wantValid: true,
		},
		{
			name: "valid user with author role",
			user: &models.UserImport{
				ID:     "6f304cd1-8a43-4417-aec7-55f419572494",
				Email:  "author@test.org",
				Name:   "Author User",
				Role:   "author",
				Active: "false",
			},
			wantValid: true,
		},
		{
			name: "valid user with reader role",
			user: &models.UserImport{
				ID:     "48d86a11-65e7-4e96-a7a9-fb9787a53df9",
				Email:  "reader@mail.dev",
				Name:   "Reader User",
				Role:   "reader",
				Active: "true",
			},
			wantValid: true,
		},
		{
			name: "invalid email format",
			user: &models.UserImport{
				ID:     "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Email:  "foo@bar",
				Name:   "Test User",
				Role:   "admin",
				Active: "true",
			},
			wantValid:   false,
			wantErrCode: "INVALID_EMAIL",
		},
		{
			name: "empty email",
			user: &models.UserImport{
				ID:     "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Email:  "",
				Name:   "Test User",
				Role:   "admin",
				Active: "true",
			},
			wantValid:   false,
			wantErrCode: "MISSING_FIELD",
		},
		{
			name: "invalid role - manager",
			user: &models.UserImport{
				ID:     "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Email:  "user@example.com",
				Name:   "Test User",
				Role:   "manager",
				Active: "true",
			},
			wantValid:   false,
			wantErrCode: "INVALID_ROLE",
		},
		{
			name: "invalid role - unknown",
			user: &models.UserImport{
				ID:     "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Email:  "user@example.com",
				Name:   "Test User",
				Role:   "superuser",
				Active: "true",
			},
			wantValid:   false,
			wantErrCode: "INVALID_ROLE",
		},
		{
			name: "empty name",
			user: &models.UserImport{
				ID:     "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Email:  "user@example.com",
				Name:   "",
				Role:   "admin",
				Active: "true",
			},
			wantValid:   false,
			wantErrCode: "MISSING_FIELD",
		},
		{
			name: "invalid active value",
			user: &models.UserImport{
				ID:     "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Email:  "user@example.com",
				Name:   "Test User",
				Role:   "admin",
				Active: "yes",
			},
			wantValid:   false,
			wantErrCode: "INVALID_BOOLEAN",
		},
		{
			name: "invalid UUID format",
			user: &models.UserImport{
				ID:     "invalid-uuid",
				Email:  "user@example.com",
				Name:   "Test User",
				Role:   "admin",
				Active: "true",
			},
			wantValid:   false,
			wantErrCode: "INVALID_UUID",
		},
		{
			name: "email without domain extension",
			user: &models.UserImport{
				ID:     "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Email:  "user@localhost",
				Name:   "Test User",
				Role:   "admin",
				Active: "true",
			},
			wantValid:   false,
			wantErrCode: "INVALID_EMAIL",
		},
		{
			name: "valid email with subdomain",
			user: &models.UserImport{
				ID:     "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Email:  "user@mail.example.com",
				Name:   "Test User",
				Role:   "admin",
				Active: "true",
			},
			wantValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validator.ValidateUserImport(1, tt.user)

			if tt.wantValid && len(errs) > 0 {
				t.Errorf("ValidateUserImport() expected valid, got errors: %v", errs)
			}

			if !tt.wantValid {
				if len(errs) == 0 {
					t.Errorf("ValidateUserImport() expected errors, got none")
				} else {
					found := false
					for _, err := range errs {
						if err.Code == tt.wantErrCode {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("ValidateUserImport() expected error code %s, got %v", tt.wantErrCode, errs)
					}
				}
			}
		})
	}
}

func TestUserValidator_ValidateEmail(t *testing.T) {
	validEmails := []string{
		"user@example.com",
		"user@example.org",
		"user@mail.dev",
		"user.name@example.com",
		"user+tag@example.com",
		"user123@test.net",
	}

	invalidEmails := []string{
		"",
		"user",
		"user@",
		"@example.com",
		"user@bar",
		"foo@bar",
		"user@localhost",
	}

	for _, email := range validEmails {
		if !emailRegex.MatchString(email) {
			t.Errorf("Email regex rejected valid email: %q", email)
		}
	}

	for _, email := range invalidEmails {
		if emailRegex.MatchString(email) {
			t.Errorf("Email regex accepted invalid email: %q", email)
		}
	}
}

func TestUserValidator_ConvertToUser(t *testing.T) {
	validator := NewUserValidator()

	tests := []struct {
		name    string
		input   *models.UserImport
		wantErr bool
	}{
		{
			name: "valid conversion with ID",
			input: &models.UserImport{
				ID:     "5864905b-ec8c-4fa6-8ba7-545d13f29b4e",
				Email:  "user@example.com",
				Name:   "Test User",
				Role:   "admin",
				Active: "true",
			},
			wantErr: false,
		},
		{
			name: "valid conversion without ID",
			input: &models.UserImport{
				Email:  "user2@example.com",
				Name:   "Test User 2",
				Role:   "reader",
				Active: "false",
			},
			wantErr: false,
		},
		{
			name: "conversion with invalid ID",
			input: &models.UserImport{
				ID:     "invalid",
				Email:  "user@example.com",
				Name:   "Test User",
				Role:   "admin",
				Active: "true",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			user, err := validator.ConvertToUser(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertToUser() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && user == nil {
				t.Error("ConvertToUser() returned nil user without error")
			}
		})
	}
}

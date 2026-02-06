package validation

import (
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rohit/bulk-import-export/internal/domain/errors"
	"github.com/rohit/bulk-import-export/internal/domain/models"
)

// UserValidator validates user data during import
type UserValidator struct{}

// NewUserValidator creates a new UserValidator
func NewUserValidator() *UserValidator {
	return &UserValidator{}
}

// Email regex pattern
var emailRegex = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)

// ValidateUserImport validates a user import record
func (v *UserValidator) ValidateUserImport(row int, user *models.UserImport) []*errors.ValidationError {
	var errs []*errors.ValidationError
	identifier := user.Email
	if identifier == "" && user.ID != "" {
		identifier = user.ID
	}

	// Validate ID (optional but must be valid UUID if provided)
	if user.ID != "" {
		if _, err := uuid.Parse(user.ID); err != nil {
			errs = append(errs, errors.NewValidationError(row, identifier, "id", errors.ErrCodeInvalidUUID, "Invalid UUID format"))
		}
	}

	// Validate email (required, valid format)
	if user.Email == "" {
		errs = append(errs, errors.NewValidationError(row, identifier, "email", errors.ErrCodeMissingField, "Email is required"))
	} else if !emailRegex.MatchString(user.Email) {
		errs = append(errs, errors.NewValidationError(row, identifier, "email", errors.ErrCodeInvalidEmail, "Invalid email format"))
	}

	// Validate name (required, max 255 chars)
	if user.Name == "" {
		errs = append(errs, errors.NewValidationError(row, identifier, "name", errors.ErrCodeMissingField, "Name is required"))
	} else if len(user.Name) > 255 {
		errs = append(errs, errors.NewValidationError(row, identifier, "name", errors.ErrCodeInvalidName, "Name must be at most 255 characters"))
	}

	// Validate role (must be one of allowed roles)
	if user.Role == "" {
		errs = append(errs, errors.NewValidationError(row, identifier, "role", errors.ErrCodeMissingField, "Role is required"))
	} else if !models.AllowedUserRoles[strings.ToLower(user.Role)] {
		errs = append(errs, errors.NewValidationError(row, identifier, "role", errors.ErrCodeInvalidRole, "Role must be one of: admin, reader, author"))
	}

	// Validate active (must be boolean)
	if user.Active != "" {
		active := strings.ToLower(user.Active)
		if active != "true" && active != "false" {
			errs = append(errs, errors.NewValidationError(row, identifier, "active", errors.ErrCodeInvalidBoolean, "Active must be 'true' or 'false'"))
		}
	}

	// Validate created_at (optional, must be valid ISO8601 if provided)
	if user.CreatedAt != "" {
		if _, err := time.Parse(time.RFC3339, user.CreatedAt); err != nil {
			errs = append(errs, errors.NewValidationError(row, identifier, "created_at", errors.ErrCodeInvalidTimestamp, "Invalid timestamp format (expected ISO8601/RFC3339)"))
		}
	}

	// Validate updated_at (optional, must be valid ISO8601 if provided)
	if user.UpdatedAt != "" {
		if _, err := time.Parse(time.RFC3339, user.UpdatedAt); err != nil {
			errs = append(errs, errors.NewValidationError(row, identifier, "updated_at", errors.ErrCodeInvalidTimestamp, "Invalid timestamp format (expected ISO8601/RFC3339)"))
		}
	}

	return errs
}

// ConvertToUser converts a validated UserImport to a User model
func (v *UserValidator) ConvertToUser(ui *models.UserImport) (*models.User, error) {
	user := &models.User{
		Email: strings.ToLower(strings.TrimSpace(ui.Email)),
		Name:  strings.TrimSpace(ui.Name),
		Role:  strings.ToLower(strings.TrimSpace(ui.Role)),
	}

	// Parse ID
	if ui.ID != "" {
		id, err := uuid.Parse(ui.ID)
		if err != nil {
			return nil, err
		}
		user.ID = id
	} else {
		user.ID = uuid.New()
	}

	// Parse active
	if ui.Active != "" {
		user.Active = strings.ToLower(ui.Active) == "true"
	} else {
		user.Active = true // default
	}

	// Parse timestamps
	if ui.CreatedAt != "" {
		t, err := time.Parse(time.RFC3339, ui.CreatedAt)
		if err != nil {
			return nil, err
		}
		user.CreatedAt = t
	} else {
		user.CreatedAt = time.Now().UTC()
	}

	if ui.UpdatedAt != "" {
		t, err := time.Parse(time.RFC3339, ui.UpdatedAt)
		if err != nil {
			return nil, err
		}
		user.UpdatedAt = t
	} else {
		user.UpdatedAt = time.Now().UTC()
	}

	return user, nil
}

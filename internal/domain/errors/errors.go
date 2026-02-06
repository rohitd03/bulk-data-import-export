package errors

import (
	"fmt"
)

// Error codes for validation and processing errors
const (
	// General errors
	ErrCodeInternalError       = "INTERNAL_ERROR"
	ErrCodeInvalidRequest      = "INVALID_REQUEST"
	ErrCodeNotFound            = "NOT_FOUND"
	ErrCodeConflict            = "CONFLICT"
	ErrCodeIdempotencyConflict = "IDEMPOTENCY_CONFLICT"

	// Validation errors - User
	ErrCodeInvalidUUID      = "INVALID_UUID"
	ErrCodeInvalidEmail     = "INVALID_EMAIL"
	ErrCodeDuplicateEmail   = "DUPLICATE_EMAIL"
	ErrCodeInvalidName      = "INVALID_NAME"
	ErrCodeInvalidRole      = "INVALID_ROLE"
	ErrCodeInvalidBoolean   = "INVALID_BOOLEAN"
	ErrCodeInvalidTimestamp = "INVALID_TIMESTAMP"
	ErrCodeMissingField     = "MISSING_FIELD"

	// Validation errors - Article
	ErrCodeInvalidSlug        = "INVALID_SLUG"
	ErrCodeDuplicateSlug      = "DUPLICATE_SLUG"
	ErrCodeInvalidTitle       = "INVALID_TITLE"
	ErrCodeInvalidBody        = "INVALID_BODY"
	ErrCodeInvalidAuthor      = "INVALID_AUTHOR"
	ErrCodeInvalidTags        = "INVALID_TAGS"
	ErrCodeInvalidStatus      = "INVALID_STATUS"
	ErrCodeDraftWithPublished = "INVALID_PUBLISHED_AT"
	ErrCodeMissingPublishedAt = "MISSING_PUBLISHED_AT"

	// Validation errors - Comment
	ErrCodeInvalidArticle = "INVALID_ARTICLE"
	ErrCodeInvalidUser    = "INVALID_USER"
	ErrCodeBodyTooLong    = "BODY_TOO_LONG"
	ErrCodeBodyEmpty      = "BODY_EMPTY"

	// Foreign key errors
	ErrCodeFKViolation     = "FK_VIOLATION"
	ErrCodeAuthorNotFound  = "AUTHOR_NOT_FOUND"
	ErrCodeArticleNotFound = "ARTICLE_NOT_FOUND"
	ErrCodeUserNotFound    = "USER_NOT_FOUND"

	// File errors
	ErrCodeInvalidFileType = "INVALID_FILE_TYPE"
	ErrCodeFileTooLarge    = "FILE_TOO_LARGE"
	ErrCodeFileReadError   = "FILE_READ_ERROR"
	ErrCodeFileParseError  = "FILE_PARSE_ERROR"

	// Job errors
	ErrCodeJobNotFound      = "JOB_NOT_FOUND"
	ErrCodeJobAlreadyExists = "JOB_ALREADY_EXISTS"
	ErrCodeJobFailed        = "JOB_FAILED"
)

// AppError represents an application error
type AppError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	Field      string `json:"field,omitempty"`
	StatusCode int    `json:"-"`
}

func (e *AppError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("[%s] %s (field: %s)", e.Code, e.Message, e.Field)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// ValidationError represents a validation error for a specific record
type ValidationError struct {
	RowNumber        int    `json:"row_number"`
	RecordIdentifier string `json:"record_identifier,omitempty"`
	FieldName        string `json:"field_name,omitempty"`
	Code             string `json:"code"`
	Message          string `json:"message"`
	RawData          string `json:"raw_data,omitempty"`
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("row %d: [%s] %s", e.RowNumber, e.Code, e.Message)
}

// NewValidationError creates a new validation error
func NewValidationError(rowNumber int, recordID, field, code, message string) *ValidationError {
	return &ValidationError{
		RowNumber:        rowNumber,
		RecordIdentifier: recordID,
		FieldName:        field,
		Code:             code,
		Message:          message,
	}
}

// NewAppError creates a new application error
func NewAppError(code, message string, statusCode int) *AppError {
	return &AppError{
		Code:       code,
		Message:    message,
		StatusCode: statusCode,
	}
}

// NewAppErrorWithField creates a new application error with a field
func NewAppErrorWithField(code, message, field string, statusCode int) *AppError {
	return &AppError{
		Code:       code,
		Message:    message,
		Field:      field,
		StatusCode: statusCode,
	}
}

// Error factory functions
func ErrInternalError(message string) *AppError {
	return NewAppError(ErrCodeInternalError, message, 500)
}

func ErrNotFound(resource string) *AppError {
	return NewAppError(ErrCodeNotFound, fmt.Sprintf("%s not found", resource), 404)
}

func ErrInvalidRequest(message string) *AppError {
	return NewAppError(ErrCodeInvalidRequest, message, 400)
}

func ErrConflict(message string) *AppError {
	return NewAppError(ErrCodeConflict, message, 409)
}

func ErrIdempotencyConflict(existingJobID string) *AppError {
	return NewAppError(ErrCodeIdempotencyConflict,
		fmt.Sprintf("Request with this idempotency key already exists (job_id: %s)", existingJobID), 409)
}

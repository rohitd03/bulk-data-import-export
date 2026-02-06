package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// User represents a user entity
type User struct {
	ID        uuid.UUID `json:"id" db:"id"`
	Email     string    `json:"email" db:"email"`
	Name      string    `json:"name" db:"name"`
	Role      string    `json:"role" db:"role"`
	Active    bool      `json:"active" db:"active"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// UserImport represents user data during import (before validation)
type UserImport struct {
	ID        string `json:"id" csv:"id"`
	Email     string `json:"email" csv:"email"`
	Name      string `json:"name" csv:"name"`
	Role      string `json:"role" csv:"role"`
	Active    string `json:"active" csv:"active"`
	CreatedAt string `json:"created_at" csv:"created_at"`
	UpdatedAt string `json:"updated_at" csv:"updated_at"`
}

// AllowedUserRoles defines valid user roles
var AllowedUserRoles = map[string]bool{
	"admin":  true,
	"reader": true,
	"author": true,
}

// Article represents an article entity
type Article struct {
	ID          uuid.UUID       `json:"id" db:"id"`
	Slug        string          `json:"slug" db:"slug"`
	Title       string          `json:"title" db:"title"`
	Body        string          `json:"body" db:"body"`
	AuthorID    uuid.UUID       `json:"author_id" db:"author_id"`
	Tags        json.RawMessage `json:"tags" db:"tags"`
	PublishedAt *time.Time      `json:"published_at,omitempty" db:"published_at"`
	Status      string          `json:"status" db:"status"`
	CreatedAt   time.Time       `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at" db:"updated_at"`
}

// ArticleImport represents article data during import
type ArticleImport struct {
	ID          string   `json:"id" csv:"id"`
	Slug        string   `json:"slug" csv:"slug"`
	Title       string   `json:"title" csv:"title"`
	Body        string   `json:"body" csv:"body"`
	AuthorID    string   `json:"author_id" csv:"author_id"`
	Tags        []string `json:"tags" csv:"tags"`
	PublishedAt string   `json:"published_at,omitempty" csv:"published_at"`
	Status      string   `json:"status" csv:"status"`
}

// AllowedArticleStatuses defines valid article statuses
var AllowedArticleStatuses = map[string]bool{
	"draft":     true,
	"published": true,
	"archived":  true,
}

// Comment represents a comment entity
type Comment struct {
	ID        uuid.UUID `json:"id" db:"id"`
	ArticleID uuid.UUID `json:"article_id" db:"article_id"`
	UserID    uuid.UUID `json:"user_id" db:"user_id"`
	Body      string    `json:"body" db:"body"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// CommentImport represents comment data during import
type CommentImport struct {
	ID        string `json:"id" csv:"id"`
	ArticleID string `json:"article_id" csv:"article_id"`
	UserID    string `json:"user_id" csv:"user_id"`
	Body      string `json:"body" csv:"body"`
	CreatedAt string `json:"created_at" csv:"created_at"`
}

// MaxCommentWords defines the maximum word count for comments
const MaxCommentWords = 500

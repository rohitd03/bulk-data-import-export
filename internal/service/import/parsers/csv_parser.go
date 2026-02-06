package parsers

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/rohit/bulk-import-export/internal/domain/models"
)

// CSVParser parses CSV files for user imports
type CSVParser struct {
	reader     *csv.Reader
	headers    []string
	headerMap  map[string]int
	lineNumber int
}

// NewCSVParser creates a new CSV parser from a reader
func NewCSVParser(r io.Reader) (*CSVParser, error) {
	// Wrap in buffered reader for efficiency
	br := bufio.NewReaderSize(r, 64*1024) // 64KB buffer
	csvReader := csv.NewReader(br)
	csvReader.FieldsPerRecord = -1 // Allow variable number of fields
	csvReader.LazyQuotes = true
	csvReader.TrimLeadingSpace = true

	// Read header row
	headers, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV headers: %w", err)
	}

	// Build header map
	headerMap := make(map[string]int)
	for i, h := range headers {
		headerMap[strings.ToLower(strings.TrimSpace(h))] = i
	}

	return &CSVParser{
		reader:     csvReader,
		headers:    headers,
		headerMap:  headerMap,
		lineNumber: 1, // Header is line 1
	}, nil
}

// ParseUsers streams user records from the CSV file
func (p *CSVParser) ParseUsers(callback func(row int, user *models.UserImport) error) error {
	for {
		record, err := p.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Skip malformed rows but continue processing
			p.lineNumber++
			continue
		}

		p.lineNumber++
		user := p.parseUserRecord(record)

		if err := callback(p.lineNumber, user); err != nil {
			return err
		}
	}
	return nil
}

// parseUserRecord converts a CSV record to a UserImport struct
func (p *CSVParser) parseUserRecord(record []string) *models.UserImport {
	user := &models.UserImport{}

	if idx, ok := p.headerMap["id"]; ok && idx < len(record) {
		user.ID = strings.TrimSpace(record[idx])
	}
	if idx, ok := p.headerMap["email"]; ok && idx < len(record) {
		user.Email = strings.TrimSpace(record[idx])
	}
	if idx, ok := p.headerMap["name"]; ok && idx < len(record) {
		user.Name = strings.TrimSpace(record[idx])
	}
	if idx, ok := p.headerMap["role"]; ok && idx < len(record) {
		user.Role = strings.TrimSpace(record[idx])
	}
	if idx, ok := p.headerMap["active"]; ok && idx < len(record) {
		user.Active = strings.TrimSpace(record[idx])
	}
	if idx, ok := p.headerMap["created_at"]; ok && idx < len(record) {
		user.CreatedAt = strings.TrimSpace(record[idx])
	}
	if idx, ok := p.headerMap["updated_at"]; ok && idx < len(record) {
		user.UpdatedAt = strings.TrimSpace(record[idx])
	}

	return user
}

// TotalLines returns an estimated total line count (read so far)
func (p *CSVParser) TotalLines() int {
	return p.lineNumber
}

// ParseArticles streams article records from the CSV file
func (p *CSVParser) ParseArticles(callback func(row int, article *models.ArticleImport) error) error {
	for {
		record, err := p.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Skip malformed rows but continue processing
			p.lineNumber++
			continue
		}

		p.lineNumber++
		article := p.parseArticleRecord(record)

		if err := callback(p.lineNumber, article); err != nil {
			return err
		}
	}
	return nil
}

// parseArticleRecord converts a CSV record to an ArticleImport struct
func (p *CSVParser) parseArticleRecord(record []string) *models.ArticleImport {
	article := &models.ArticleImport{}

	if idx, ok := p.headerMap["id"]; ok && idx < len(record) {
		article.ID = strings.TrimSpace(record[idx])
	}
	if idx, ok := p.headerMap["slug"]; ok && idx < len(record) {
		article.Slug = strings.TrimSpace(record[idx])
	}
	if idx, ok := p.headerMap["title"]; ok && idx < len(record) {
		article.Title = strings.TrimSpace(record[idx])
	}
	if idx, ok := p.headerMap["body"]; ok && idx < len(record) {
		article.Body = strings.TrimSpace(record[idx])
	}
	if idx, ok := p.headerMap["author_id"]; ok && idx < len(record) {
		article.AuthorID = strings.TrimSpace(record[idx])
	}
	if idx, ok := p.headerMap["tags"]; ok && idx < len(record) {
		// Parse tags as comma-separated values
		tagsStr := strings.TrimSpace(record[idx])
		if tagsStr != "" {
			article.Tags = strings.Split(tagsStr, ",")
			for i := range article.Tags {
				article.Tags[i] = strings.TrimSpace(article.Tags[i])
			}
		}
	}
	if idx, ok := p.headerMap["published_at"]; ok && idx < len(record) {
		article.PublishedAt = strings.TrimSpace(record[idx])
	}
	if idx, ok := p.headerMap["status"]; ok && idx < len(record) {
		article.Status = strings.TrimSpace(record[idx])
	}

	return article
}

// ParseComments streams comment records from the CSV file
func (p *CSVParser) ParseComments(callback func(row int, comment *models.CommentImport) error) error {
	for {
		record, err := p.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Skip malformed rows but continue processing
			p.lineNumber++
			continue
		}

		p.lineNumber++
		comment := p.parseCommentRecord(record)

		if err := callback(p.lineNumber, comment); err != nil {
			return err
		}
	}
	return nil
}

// parseCommentRecord converts a CSV record to a CommentImport struct
func (p *CSVParser) parseCommentRecord(record []string) *models.CommentImport {
	comment := &models.CommentImport{}

	if idx, ok := p.headerMap["id"]; ok && idx < len(record) {
		comment.ID = strings.TrimSpace(record[idx])
	}
	if idx, ok := p.headerMap["article_id"]; ok && idx < len(record) {
		comment.ArticleID = strings.TrimSpace(record[idx])
	}
	if idx, ok := p.headerMap["user_id"]; ok && idx < len(record) {
		comment.UserID = strings.TrimSpace(record[idx])
	}
	if idx, ok := p.headerMap["body"]; ok && idx < len(record) {
		comment.Body = strings.TrimSpace(record[idx])
	}
	if idx, ok := p.headerMap["created_at"]; ok && idx < len(record) {
		comment.CreatedAt = strings.TrimSpace(record[idx])
	}

	return comment
}

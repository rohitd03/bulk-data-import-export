package parsers

import (
	"bufio"
	"encoding/json"
	"io"

	"github.com/rohit/bulk-import-export/internal/domain/models"
)

// NDJSONParser parses NDJSON (newline-delimited JSON) files
type NDJSONParser struct {
	scanner    *bufio.Scanner
	lineNumber int
}

// NewNDJSONParser creates a new NDJSON parser from a reader
func NewNDJSONParser(r io.Reader) *NDJSONParser {
	scanner := bufio.NewScanner(r)
	// Increase buffer size for large JSON objects
	const maxBufferSize = 10 * 1024 * 1024 // 10MB per line max
	buf := make([]byte, 64*1024)           // 64KB initial
	scanner.Buffer(buf, maxBufferSize)

	return &NDJSONParser{
		scanner:    scanner,
		lineNumber: 0,
	}
}

// ParseArticles streams article records from the NDJSON file
func (p *NDJSONParser) ParseArticles(callback func(row int, article *models.ArticleImport, rawJSON string) error) error {
	for p.scanner.Scan() {
		p.lineNumber++
		line := p.scanner.Text()

		if line == "" {
			continue // Skip empty lines
		}

		var article models.ArticleImport
		if err := json.Unmarshal([]byte(line), &article); err != nil {
			// Pass nil article with error - the callback should handle parse errors
			if err := callback(p.lineNumber, nil, line); err != nil {
				return err
			}
			continue
		}

		if err := callback(p.lineNumber, &article, line); err != nil {
			return err
		}
	}

	return p.scanner.Err()
}

// ParseUsers streams user records from the NDJSON file
func (p *NDJSONParser) ParseUsers(callback func(row int, user *models.UserImport, rawJSON string) error) error {
	for p.scanner.Scan() {
		p.lineNumber++
		line := p.scanner.Text()

		if line == "" {
			continue // Skip empty lines
		}

		var user models.UserImport
		if err := json.Unmarshal([]byte(line), &user); err != nil {
			// Pass nil user with error - the callback should handle parse errors
			if err := callback(p.lineNumber, nil, line); err != nil {
				return err
			}
			continue
		}

		if err := callback(p.lineNumber, &user, line); err != nil {
			return err
		}
	}

	return p.scanner.Err()
}

// ParseComments streams comment records from the NDJSON file
func (p *NDJSONParser) ParseComments(callback func(row int, comment *models.CommentImport, rawJSON string) error) error {
	for p.scanner.Scan() {
		p.lineNumber++
		line := p.scanner.Text()

		if line == "" {
			continue // Skip empty lines
		}

		var comment models.CommentImport
		if err := json.Unmarshal([]byte(line), &comment); err != nil {
			// Pass nil comment with error - the callback should handle parse errors
			if err := callback(p.lineNumber, nil, line); err != nil {
				return err
			}
			continue
		}

		if err := callback(p.lineNumber, &comment, line); err != nil {
			return err
		}
	}

	return p.scanner.Err()
}

// TotalLines returns the total lines read so far
func (p *NDJSONParser) TotalLines() int {
	return p.lineNumber
}

// ParseGeneric parses NDJSON into a generic map (for mixed content)
func (p *NDJSONParser) ParseGeneric(callback func(row int, data map[string]interface{}, rawJSON string) error) error {
	for p.scanner.Scan() {
		p.lineNumber++
		line := p.scanner.Text()

		if line == "" {
			continue
		}

		var data map[string]interface{}
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			if err := callback(p.lineNumber, nil, line); err != nil {
				return err
			}
			continue
		}

		if err := callback(p.lineNumber, data, line); err != nil {
			return err
		}
	}

	return p.scanner.Err()
}

package parsers

import (
	"testing"
)

func TestDetectFormat(t *testing.T) {
	tests := []struct {
		filename string
		expected FileFormat
	}{
		{"users.csv", FormatCSV},
		{"users.CSV", FormatCSV},
		{"data/users.csv", FormatCSV},
		{"articles.ndjson", FormatNDJSON},
		{"articles.NDJSON", FormatNDJSON},
		{"comments.jsonl", FormatNDJSON},
		{"data.json", FormatJSON},
		{"noextension", FormatCSV}, // defaults to CSV
		{"", FormatCSV},            // defaults to CSV
		{"file.txt", FormatCSV},    // unknown defaults to CSV
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			result := DetectFormat(tt.filename)
			if result != tt.expected {
				t.Errorf("DetectFormat(%q) = %q, want %q", tt.filename, result, tt.expected)
			}
		})
	}
}

func TestFileFormat_IsCSV(t *testing.T) {
	tests := []struct {
		format   FileFormat
		expected bool
	}{
		{FormatCSV, true},
		{FormatNDJSON, false},
		{FormatJSON, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.format), func(t *testing.T) {
			if result := tt.format.IsCSV(); result != tt.expected {
				t.Errorf("FileFormat(%q).IsCSV() = %v, want %v", tt.format, result, tt.expected)
			}
		})
	}
}

func TestFileFormat_IsNDJSON(t *testing.T) {
	tests := []struct {
		format   FileFormat
		expected bool
	}{
		{FormatCSV, false},
		{FormatNDJSON, true},
		{FormatJSON, true}, // JSON is treated as NDJSON
	}

	for _, tt := range tests {
		t.Run(string(tt.format), func(t *testing.T) {
			if result := tt.format.IsNDJSON(); result != tt.expected {
				t.Errorf("FileFormat(%q).IsNDJSON() = %v, want %v", tt.format, result, tt.expected)
			}
		})
	}
}

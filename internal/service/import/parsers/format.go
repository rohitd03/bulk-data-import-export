package parsers

import (
	"path/filepath"
	"strings"
)

// FileFormat represents the format of an import file
type FileFormat string

const (
	FormatCSV    FileFormat = "csv"
	FormatNDJSON FileFormat = "ndjson"
	FormatJSON   FileFormat = "json"
)

// DetectFormat determines the file format from the filename extension
func DetectFormat(filename string) FileFormat {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".csv":
		return FormatCSV
	case ".ndjson", ".jsonl":
		return FormatNDJSON
	case ".json":
		return FormatJSON
	default:
		// Default to CSV for backwards compatibility
		return FormatCSV
	}
}

// IsCSV returns true if the format is CSV
func (f FileFormat) IsCSV() bool {
	return f == FormatCSV
}

// IsNDJSON returns true if the format is NDJSON
func (f FileFormat) IsNDJSON() bool {
	return f == FormatNDJSON || f == FormatJSON
}

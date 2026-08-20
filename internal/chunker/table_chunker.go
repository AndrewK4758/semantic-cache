package chunker

import (
	"strings"
)

// TableChunker handles row-atomic splitting of Markdown tables with repeated headers.
type TableChunker struct{}

// NewTableChunker creates a new TableChunker instance.
func NewTableChunker() *TableChunker {
	return &TableChunker{}
}

// EstimateTokens calculates an approximate token count (~4 characters per token).
func EstimateTokens(text string) int {
	if len(text) == 0 {
		return 0
	}
	words := len(strings.Fields(text))
	charEstimate := len(text) / 4
	if words > charEstimate {
		return words
	}
	return charEstimate
}

// IsTable checks if a block of text is formatted as a Markdown table.
func (tc *TableChunker) IsTable(text string) bool {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	if len(lines) < 2 {
		return false
	}
	firstLine := strings.TrimSpace(lines[0])
	secondLine := strings.TrimSpace(lines[1])
	return strings.HasPrefix(firstLine, "|") && strings.HasSuffix(firstLine, "|") &&
		strings.HasPrefix(secondLine, "|") && strings.Contains(secondLine, "-")
}

// ChunkTable splits a Markdown table into row-atomic segments, repeating headers on each continuation chunk.
func (tc *TableChunker) ChunkTable(tableMarkdown string, maxTokens int) []string {
	trimmed := strings.TrimSpace(tableMarkdown)
	if len(trimmed) == 0 {
		return nil
	}

	lines := strings.Split(trimmed, "\n")
	if len(lines) < 3 {
		return []string{trimmed}
	}

	headerRow := strings.TrimSpace(lines[0])
	delimiterRow := strings.TrimSpace(lines[1])
	headerPrefix := headerRow + "\n" + delimiterRow + "\n"
	headerTokens := EstimateTokens(headerPrefix)

	if maxTokens <= headerTokens+5 {
		maxTokens = headerTokens + 20
	}

	var chunks []string
	var currentRows []string
	currentTokens := headerTokens

	for i := 2; i < len(lines); i++ {
		row := strings.TrimSpace(lines[i])
		if len(row) == 0 {
			continue
		}

		rowTokens := EstimateTokens(row + "\n")

		if len(currentRows) > 0 && (currentTokens+rowTokens > maxTokens) {
			chunkContent := headerPrefix + strings.Join(currentRows, "\n")
			chunks = append(chunks, chunkContent)
			currentRows = nil
			currentTokens = headerTokens
		}

		currentRows = append(currentRows, row)
		currentTokens += rowTokens
	}

	if len(currentRows) > 0 {
		chunkContent := headerPrefix + strings.Join(currentRows, "\n")
		chunks = append(chunks, chunkContent)
	}

	return chunks
}

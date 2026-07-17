package domain

// CacheRecord represents a stored semantic embedding and its associated metadata/payload.
type CacheRecord struct {
	ID          string
	Metadata    map[string]any
	Vector      []float32
	JSONPayload string
}

// SearchResult wraps a CacheRecord with its similarity score.
type SearchResult struct {
	Record CacheRecord
	Score  float32
}

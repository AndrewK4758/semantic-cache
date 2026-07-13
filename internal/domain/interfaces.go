package domain

import "context"

// EmbeddingService generates mathematical vectors from text.
type EmbeddingService interface {
	Generate(ctx context.Context, text string) ([]float32, error)
}

// VectorStore persists and searches semantic embeddings.
type VectorStore interface {
	Search(ctx context.Context, vector []float32, metadata map[string]string, limit int) ([]SearchResult, error)
	Upsert(ctx context.Context, record CacheRecord) error
}

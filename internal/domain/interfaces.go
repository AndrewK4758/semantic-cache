package domain

import "context"

// EmbeddingService generates mathematical vectors from text.
type EmbeddingService interface {
	Generate(ctx context.Context, text string) ([]float32, error)
}

// VectorStore persists and searches semantic embeddings.
type VectorStore interface {
	Search(ctx context.Context, collectionName string, vector []float32, metadata map[string]interface{}, limit int) ([]SearchResult, error)
	Upsert(ctx context.Context, collectionName string, record CacheRecord) error
	CheckMetadata(ctx context.Context, collectionName string, metadata map[string]interface{}) (bool, error)
}

package domain

import (
	"context"

	pb "github.com/AndrewK4758/shared_protos"
)

// EmbeddingService generates mathematical vectors from text.
type EmbeddingService interface {
	Generate(ctx context.Context, text string) ([]float32, error)
}

// VectorStore persists and searches semantic embeddings.
type VectorStore interface {
	Search(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]SearchResult, error)
	Upsert(ctx context.Context, collectionName string, record CacheRecord) error
	CheckMetadata(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) (bool, error)
	GetByMetadata(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) ([]SearchResult, error)
}

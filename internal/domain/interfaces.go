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

    // EnsureCollection Hybrid vector store methods
	EnsureCollection(ctx context.Context, collectionName string, denseDim uint64, pqCompression uint32) error
	UpsertHybrid(ctx context.Context, collectionName string, record CacheRecord) error
	HybridSearch(ctx context.Context, collectionName string, denseVector []float32, sparseVector *SparseVector, identity *pb.InfrastructureIdentity, metadata map[string]any, alpha float32, limit int) ([]SearchResult, error)
}

package domain

import (
	pb "github.com/AndrewK4758/shared_protos"
)

// SparseVector holds sparse term indices and weights.
type SparseVector struct {
	Indices []uint32  `json:"indices"`
	Values  []float32 `json:"values"`
}

// CacheRecord represents a stored semantic embedding and its associated metadata/payload.
type CacheRecord struct {
	ID          string
	Identity    *pb.InfrastructureIdentity
	Metadata    map[string]any
	Vector      []float32     // Dense vector (3072d)
	SparseVec   *SparseVector // Sparse vector (BM25/SPLADE)
	JSONPayload string
}

// SearchResult wraps a CacheRecord with its similarity score.
type SearchResult struct {
	Record      CacheRecord
	Score       float32
	DenseScore  float32
	SparseScore float32
}

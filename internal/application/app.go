package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/doc_processor/semantic_cache_service/internal/domain"
)

// SemanticCacheApp coordinates the semantic cache use cases.
type SemanticCacheApp struct {
	embedder domain.EmbeddingService
	store    domain.VectorStore
}

// NewSemanticCacheApp creates a new application instance.
func NewSemanticCacheApp(embedder domain.EmbeddingService, store domain.VectorStore) *SemanticCacheApp {
	return &SemanticCacheApp{
		embedder: embedder,
		store:    store,
	}
}

// CheckCache processes the Check Cache use case.
func (a *SemanticCacheApp) CheckCache(ctx context.Context, text string, metadata map[string]string, threshold float32) (hit bool, extractedPayload string, confidence float32, err error) {
	vector, err := a.embedder.Generate(ctx, text)
	if err != nil {
		return false, "", 0, fmt.Errorf("action failed for job CheckCache: embedding generation error: %w", err)
	}

	results, err := a.store.Search(ctx, vector, metadata, 1)
	if err != nil {
		return false, "", 0, fmt.Errorf("action failed for job CheckCache: vector search error: %w", err)
	}

	if len(results) > 0 {
		topMatch := results[0]
		if topMatch.Score >= threshold {
			return true, topMatch.Record.JSONPayload, topMatch.Score, nil
		}
	}

	return false, "", 0, nil
}

// StoreExtraction processes the Store Extraction use case.
func (a *SemanticCacheApp) StoreExtraction(ctx context.Context, text string, metadata map[string]string, extractedPayload string) error {
	vector, err := a.embedder.Generate(ctx, text)
	if err != nil {
		return fmt.Errorf("action failed for job StoreExtraction: embedding generation error: %w", err)
	}

	// Generate a deterministic ID based on text and metadata
	hashStr := text
	for k, v := range metadata {
		hashStr += k + ":" + v + "|"
	}
	hash := sha256.Sum256([]byte(hashStr))
	recordID := hex.EncodeToString(hash[:])

	record := domain.CacheRecord{
		ID:          recordID,
		Metadata:    metadata,
		Vector:      vector,
		JSONPayload: extractedPayload,
	}

	if err := a.store.Upsert(ctx, record); err != nil {
		return fmt.Errorf("action failed for job StoreExtraction: vector upsert error: %w", err)
	}

	return nil
}

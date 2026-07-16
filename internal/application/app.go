package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"

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
func (a *SemanticCacheApp) CheckCache(ctx context.Context, collectionName string, text string, metadata map[string]interface{}, threshold float32) (hit bool, extractedPayload string, confidence float32, err error) {
	log.Printf("INFO: [SemanticCacheApp] CheckCache invoked. Collection: %s, Input Text Length: %d, Threshold: %.2f", collectionName, len(text), threshold)

	log.Printf("INFO: [SemanticCacheApp] Generating embedding for input text...")
	vector, err := a.embedder.Generate(ctx, text)
	if err != nil {
		log.Printf("ERROR: [SemanticCacheApp] Failed to generate embedding: %v", err)
		return false, "", 0, fmt.Errorf("action failed for job CheckCache: embedding generation error: %w", err)
	}
	log.Printf("INFO: [SemanticCacheApp] Successfully generated embedding vector of length %d", len(vector))

	log.Printf("INFO: [SemanticCacheApp] Searching Qdrant Vector Store for top 1 match...")
	results, err := a.store.Search(ctx, collectionName, vector, metadata, 1)
	if err != nil {
		log.Printf("ERROR: [SemanticCacheApp] Failed to search vector store: %v", err)
		return false, "", 0, fmt.Errorf("action failed for job CheckCache: vector search error: %w", err)
	}

	if len(results) > 0 {
		topMatch := results[0]
		log.Printf("INFO: [SemanticCacheApp] Vector search returned top match with Cosine Similarity Score: %.4f (Threshold: %.2f)", topMatch.Score, threshold)
		if topMatch.Score >= threshold {
			log.Printf("INFO: [SemanticCacheApp] CACHE HIT! Score %.4f exceeds threshold. Returning cached payload.", topMatch.Score)
			return true, topMatch.Record.JSONPayload, topMatch.Score, nil
		}
		log.Printf("INFO: [SemanticCacheApp] CACHE MISS. Score %.4f is below threshold %.2f.", topMatch.Score, threshold)
	} else {
		log.Printf("INFO: [SemanticCacheApp] CACHE MISS. Vector search returned 0 results.")
	}

	return false, "", 0, nil
}

// StoreExtraction processes the Store Extraction use case.
func (a *SemanticCacheApp) StoreExtraction(ctx context.Context, collectionName string, text string, metadata map[string]interface{}, extractedPayload string) error {
	log.Printf("INFO: [SemanticCacheApp] StoreExtraction invoked. Collection: %s, Input Text Length: %d, Payload Length: %d", collectionName, len(text), len(extractedPayload))

	log.Printf("INFO: [SemanticCacheApp] Generating embedding for input text to store...")
	vector, err := a.embedder.Generate(ctx, text)
	if err != nil {
		log.Printf("ERROR: [SemanticCacheApp] Failed to generate embedding: %v", err)
		return fmt.Errorf("action failed for job StoreExtraction: embedding generation error: %w", err)
	}

	// Generate a deterministic ID based on text and metadata
	hashStr := text
	for k, v := range metadata {
		hashStr += fmt.Sprintf("%s:%v|", k, v)
	}
	hash := sha256.Sum256([]byte(hashStr))
	recordID := hex.EncodeToString(hash[:])
	log.Printf("DEBUG: [SemanticCacheApp] Generated Record ID: %s", recordID)

	record := domain.CacheRecord{
		ID:          recordID,
		Metadata:    metadata,
		Vector:      vector,
		JSONPayload: extractedPayload,
	}

	log.Printf("INFO: [SemanticCacheApp] Upserting record into Qdrant Vector Store...")
	if err := a.store.Upsert(ctx, collectionName, record); err != nil {
		log.Printf("ERROR: [SemanticCacheApp] Failed to upsert record: %v", err)
		return fmt.Errorf("action failed for job StoreExtraction: vector upsert error: %w", err)
	}

	log.Printf("INFO: [SemanticCacheApp] Successfully stored cache record.")
	return nil
}

// CheckMetadata processes the pure metadata existence check use case.
func (a *SemanticCacheApp) CheckMetadata(ctx context.Context, collectionName string, metadata map[string]interface{}) (bool, error) {
	log.Printf("INFO: [SemanticCacheApp] CheckMetadata invoked. Collection: %s, Metadata fields: %d", collectionName, len(metadata))

	exists, err := a.store.CheckMetadata(ctx, collectionName, metadata)
	if err != nil {
		log.Printf("ERROR: [SemanticCacheApp] Failed to check metadata: %v", err)
		return false, fmt.Errorf("action failed for job CheckMetadata: vector store check error: %w", err)
	}

	log.Printf("INFO: [SemanticCacheApp] CheckMetadata successful. Exists: %v", exists)
	return exists, nil
}

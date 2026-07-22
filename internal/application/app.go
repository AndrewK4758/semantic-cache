package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/AndrewK4758/shared_utils/logger"
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
func (a *SemanticCacheApp) CheckCache(ctx context.Context, collectionName string, text string, metadata map[string]any, threshold float32) (hit bool, extractedPayload string, confidence float32, err error) {
	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] CheckCache invoked. Collection: %s, Input Text Length: %d, Threshold: %.2f", collectionName, len(text), threshold)

	// TIER 1: EXACT METADATA MATCH (Bypass LLM)
	if len(metadata) > 0 {
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] TIER 1: Attempting Exact Metadata Match...")
		metaResults, err := a.store.GetByMetadata(ctx, collectionName, metadata)
		if err == nil && len(metaResults) > 0 {
			topMatch := metaResults[0]
			logger.Info("SemanticCache", "INFO: [SemanticCacheApp] CACHE HIT [METADATA_EXACT_MATCH]! Bypassing embedding generation.")
			return true, topMatch.Record.JSONPayload, topMatch.Score, nil
		}
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] TIER 1 Miss: No exact metadata match found. Proceeding to Semantic Fallback.")
	}

	// TIER 2: SEMANTIC FALLBACK
	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] TIER 2: Generating embedding for input text...")
	vector, err := a.embedder.Generate(ctx, text)
	if err != nil {
		logger.Info("SemanticCache", "ERROR: [SemanticCacheApp] Failed to generate embedding: %v", err)
		return false, "", 0, fmt.Errorf("action failed for job CheckCache: embedding generation error: %w", err)
	}
	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Successfully generated embedding vector of length %d", len(vector))

	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Searching Qdrant Vector Store for top 1 match...")
	results, err := a.store.Search(ctx, collectionName, vector, nil, 1) // Do not filter semantic search by metadata, allow broad semantic matching
	if err != nil {
		logger.Info("SemanticCache", "ERROR: [SemanticCacheApp] Failed to search vector store: %v", err)
		return false, "", 0, fmt.Errorf("action failed for job CheckCache: vector search error: %w", err)
	}

	if len(results) > 0 {
		topMatch := results[0]
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Vector search returned top match with Cosine Similarity Score: %.4f (Threshold: %.2f)", topMatch.Score, threshold)
		if topMatch.Score >= threshold {
			logger.Info("SemanticCache", "INFO: [SemanticCacheApp] CACHE HIT [SEMANTIC_SIMILARITY]! Score %.4f exceeds threshold. Returning cached payload.", topMatch.Score)
			return true, topMatch.Record.JSONPayload, topMatch.Score, nil
		}
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] CACHE MISS. Score %.4f is below threshold %.2f.", topMatch.Score, threshold)
	} else {
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] CACHE MISS. Vector search returned 0 results.")
	}

	return false, "", 0, nil
}

// StoreExtraction processes the Store Extraction use case.
func (a *SemanticCacheApp) StoreExtraction(ctx context.Context, collectionName string, text string, metadata map[string]any, extractedPayload string) error {
	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] StoreExtraction invoked. Collection: %s, Input Text Length: %d, Payload Length: %d", collectionName, len(text), len(extractedPayload))

	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Generating embedding for input text to store...")
	vector, err := a.embedder.Generate(ctx, text)
	if err != nil {
		logger.Info("SemanticCache", "ERROR: [SemanticCacheApp] Failed to generate embedding: %v", err)
		return fmt.Errorf("action failed for job StoreExtraction: embedding generation error: %w", err)
	}

	// Generate a deterministic ID based on text and metadata
	var hashStr strings.Builder
	hashStr.WriteString(text)
	for k, v := range metadata {
		hashStr.WriteString(fmt.Sprintf("%s:%v|", k, v))
	}
	hash := sha256.Sum256([]byte(hashStr.String()))
	recordID := hex.EncodeToString(hash[:])
	logger.Info("SemanticCache", "DEBUG: [SemanticCacheApp] Generated Record ID: %s", recordID)

	record := domain.CacheRecord{
		ID:          recordID,
		Metadata:    metadata,
		Vector:      vector,
		JSONPayload: extractedPayload,
	}

	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Upserting record into Qdrant Vector Store...")
	if err := a.store.Upsert(ctx, collectionName, record); err != nil {
		logger.Info("SemanticCache", "ERROR: [SemanticCacheApp] Failed to upsert record: %v", err)
		return fmt.Errorf("action failed for job StoreExtraction: vector upsert error: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Successfully stored cache record.")
	return nil
}

// CheckMetadata processes the pure metadata existence check use case.
func (a *SemanticCacheApp) CheckMetadata(ctx context.Context, collectionName string, metadata map[string]any) (bool, error) {
	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] CheckMetadata invoked. Collection: %s, Metadata fields: %d", collectionName, len(metadata))

	exists, err := a.store.CheckMetadata(ctx, collectionName, metadata)
	if err != nil {
		logger.Info("SemanticCache", "ERROR: [SemanticCacheApp] Failed to check metadata: %v", err)
		return false, fmt.Errorf("action failed for job CheckMetadata: vector store check error: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] CheckMetadata successful. Exists: %v", exists)
	return exists, nil
}

package application

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
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

func purgeIgnoredMetadata(metaMap map[string]any) {
	if metaMap == nil {
		return
	}
	delete(metaMap, "json_payload")
	delete(metaMap, "email_text")
	delete(metaMap, "raw_email")
	delete(metaMap, "inference_input")
	delete(metaMap, "inbound_email_text")
	delete(metaMap, "EmailBody")
}

func extractVectorFromAny(anyVal *anypb.Any) ([]float32, error) {
	if anyVal == nil {
		return nil, nil
	}
	var strVal wrapperspb.StringValue
	if err := anyVal.UnmarshalTo(&strVal); err == nil {
		var vec []float32
		if errJson := json.Unmarshal([]byte(strVal.GetValue()), &vec); errJson == nil {
			return vec, nil
		}
	}

	var list structpb.ListValue
	if err := anyVal.UnmarshalTo(&list); err != nil {
		return nil, fmt.Errorf("failed to unmarshal vector: %w", err)
	}

	if len(list.Values) > 0 {
		vec := make([]float32, 0, len(list.Values))
		for _, v := range list.Values {
			vec = append(vec, float32(v.GetNumberValue()))
		}
		return vec, nil
	}
	return nil, nil
}

// CheckCache processes the two-stage Check Cache use case using pb.CacheContract.
func (a *SemanticCacheApp) CheckCache(ctx context.Context, contract *pb.CacheContract, identity *pb.InfrastructureIdentity, fallbackMetadata map[string]any) (hit bool, extractedPayload string, confidence float32, err error) {
	if contract == nil {
		panic(fmt.Errorf("action failed for job CheckCache: cache contract is required"))
	}
	if identity == nil {
		panic("missing required property: identity cannot be nil")
	}
	if strings.TrimSpace(identity.TenantId) == "" {
		panic("missing required property: TenantId is required")
	}
	if strings.TrimSpace(identity.AppId) == "" {
		panic("missing required property: AppId is required")
	}

	collectionName := contract.GetCollectionName()
	threshold := contract.GetConfidence()

	var text string
	if contract.EmbeddingLookup != nil {
		var strVal wrapperspb.StringValue
		if err := contract.EmbeddingLookup.UnmarshalTo(&strVal); err == nil {
			text = strVal.GetValue()
		} else {
			var val structpb.Value
			if err2 := contract.EmbeddingLookup.UnmarshalTo(&val); err2 == nil {
				text = val.GetStringValue()
			} else {
				logger.Error("SemanticCache", "ERROR: [SemanticCacheApp] Failed to unmarshal EmbeddingLookup as StringValue (%v) or StructValue (%v)", err, err2)
			}
		}
	}

	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] CheckCache invoked. Collection: %s, Input Text Length: %d, Threshold: %.2f", collectionName, len(text), threshold)
	if text != "" {
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Query Input:\n%s", text)
	}

	// Extract metadata from CacheContract
	var metaMap map[string]any
	if contract.MetadataLookup != nil {
		metaMap = contract.MetadataLookup.AsMap()
	}

	// STAGE 1: METADATA LOOKUP (Bypass LLM embedding generation)
	strictMetaMap := make(map[string]any)
	if metaMap != nil {
		if val, ok := metaMap["subject_class"]; ok {
			strictMetaMap["subject_class"] = val
		}
	}

	if len(strictMetaMap) > 0 {
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Stage 1: Attempting Exact Metadata Match with Metadata: %+v", strictMetaMap)
		metaResults, metaErr := a.store.GetByMetadata(ctx, collectionName, identity, strictMetaMap)
		if metaErr == nil && len(metaResults) > 0 {
			topMatch := metaResults[0]
			logger.Info("SemanticCache", "INFO: [SemanticCacheApp] CACHE HIT [METADATA_EXACT_MATCH]! Bypassing embedding generation.")
			logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Cached Payload Output:\n%s", topMatch.Record.JSONPayload)
			return true, topMatch.Record.JSONPayload, 1.0, nil
		}
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Stage 1 Miss: No exact metadata match found. Proceeding to Stage 2 Embedding Fallback.")
	} else {
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Stage 1 Skipped: No subject_class provided. Falling back to Stage 2.")
	}

	// STAGE 2: EMBEDDING FALLBACK
	var vector []float32
	var embedErr error
	if contract.EmbeddingLookup != nil {
		vector, _ = extractVectorFromAny(contract.EmbeddingLookup)
	}

	if len(vector) == 0 {
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Stage 2: Generating embedding for input text: %s", text)
		vector, embedErr = a.embedder.Generate(ctx, text)
		if embedErr != nil {
			logger.Info("SemanticCache", "ERROR: [SemanticCacheApp] Failed to generate embedding: %v", embedErr)
			return false, "", 0, fmt.Errorf("action failed for job CheckCache: embedding generation error: %w", embedErr)
		}
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Successfully generated embedding vector of length %d", len(vector))
	} else {
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Stage 2: Using pre-computed vector from EmbeddingLookup of length %d", len(vector))
	}

	// Do NOT pass subject_class into Search filter, leave empty map
	threshold = 0.975 // Ensure threshold is set to 0.975

	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Searching Qdrant Vector Store for top 1 match with nil Metadata Filter")
	results, searchErr := a.store.Search(ctx, collectionName, vector, identity, nil, 1)
	if searchErr != nil {
		logger.Info("SemanticCache", "ERROR: [SemanticCacheApp] Failed to search vector store: %v", searchErr)
		return false, "", 0, fmt.Errorf("action failed for job CheckCache: vector search error: %w", searchErr)
	}

	if len(results) > 0 {
		topMatch := results[0]
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Vector search returned top match with Cosine Similarity Score: %.4f (Threshold: %.2f)", topMatch.Score, threshold)
		if topMatch.Score >= threshold {
			logger.Info("SemanticCache", "INFO: [SemanticCacheApp] CACHE HIT [SEMANTIC_SIMILARITY]! Score %.4f exceeds threshold. Returning cached payload.", topMatch.Score)
			logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Cached Payload Output:\n%s", topMatch.Record.JSONPayload)
			return true, topMatch.Record.JSONPayload, topMatch.Score, nil
		}
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] CACHE MISS. Score %.4f is below threshold %.2f.", topMatch.Score, threshold)
	} else {
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] CACHE MISS. Vector search returned 0 results.")
	}

	return false, "", 0, nil
}

// StoreExtraction processes the Store Extraction use case.
func (a *SemanticCacheApp) StoreExtraction(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any, extractedPayload string, embeddingPayload *anypb.Any) error {
	purgeIgnoredMetadata(metadata)

	if _, ok := metadata["subject_class"]; !ok {
		logger.Error("SemanticCache", "FAIL-FAST ASSERTION FAILED [StoreExtraction]: subject_class is missing from metadata")
		panic(fmt.Errorf("action failed for job StoreExtraction: missing required metadata key 'subject_class'"))
	}

	if identity == nil {
		panic("missing required property: identity cannot be nil")
	}
	if strings.TrimSpace(identity.TenantId) == "" {
		panic("missing required property: TenantId is required")
	}
	if strings.TrimSpace(identity.AppId) == "" {
		panic("missing required property: AppId is required")
	}
	if title, ok := metadata["document_title"].(string); !ok || strings.TrimSpace(title) == "" {
		panic("missing required property document_title in metadata")
	}

	var text string
	if embeddingPayload != nil {
		var strVal wrapperspb.StringValue
		if err := embeddingPayload.UnmarshalTo(&strVal); err == nil {
			text = strVal.GetValue()
		} else {
			var val structpb.Value
			if err2 := embeddingPayload.UnmarshalTo(&val); err2 == nil {
				text = val.GetStringValue()
			}
		}
	}
	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] StoreExtraction invoked. Collection: %s, Input Text Length: %d, Payload Length: %d", collectionName, len(text), len(extractedPayload))

	var vector []float32
	var embErr error
	if embeddingPayload != nil {
		vector, _ = extractVectorFromAny(embeddingPayload)
	}

	if len(vector) == 0 {
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Generating embedding for input text to store...")
		vector, embErr = a.embedder.Generate(ctx, text)
		if embErr != nil {
			logger.Info("SemanticCache", "ERROR: [SemanticCacheApp] Failed to generate embedding: %v", embErr)
			return fmt.Errorf("action failed for job StoreExtraction: embedding generation embError: %w", embErr)
		}
	} else {
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Using pre-computed vector for StoreExtraction of length %d", len(vector))
	}

	recordID := uuid.New().String()
	logger.Info("SemanticCache", "DEBUG: [SemanticCacheApp] Generated Record ID: %s", recordID)

	record := domain.CacheRecord{
		ID:          recordID,
		Identity:    identity,
		Metadata:    metadata,
		Vector:      vector,
		JSONPayload: extractedPayload,
	}

	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Upserting record into Qdrant Vector Store...")
	if upsertErr := a.store.Upsert(ctx, collectionName, record); upsertErr != nil {
		logger.Info("SemanticCache", "ERROR: [SemanticCacheApp] Failed to upsert record: %v", upsertErr)
		return fmt.Errorf("action failed for job StoreExtraction: vector upsert upsertError: %w", upsertErr)
	}

	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Successfully stored cache record.")
	return nil
}

// CheckMetadata processes the pure metadata existence check use case.
func (a *SemanticCacheApp) CheckMetadata(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) (bool, error) {
	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] CheckMetadata invoked. Collection: %s, Metadata fields: %d", collectionName, len(metadata))

	exists, metaErr := a.store.CheckMetadata(ctx, collectionName, identity, metadata)
	if metaErr != nil {
		logger.Info("SemanticCache", "ERROR: [SemanticCacheApp] Failed to check metadata: %v", metaErr)
		return false, fmt.Errorf("action failed for job CheckMetadata: vector store check metaError: %w", metaErr)
	}

	logger.Info("SemanticCache", "INFO: [SemanticCacheApp] CheckMetadata successful. Exists: %v", exists)
	return exists, nil
}

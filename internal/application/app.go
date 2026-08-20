package application

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/doc_processor/semantic_cache_service/internal/chunker"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
	"github.com/doc_processor/semantic_cache_service/internal/masking"
	"github.com/doc_processor/semantic_cache_service/internal/sparse"
	"google.golang.org/protobuf/types/known/anypb"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// SemanticCacheApp coordinates the semantic cache use cases.
type SemanticCacheApp struct {
	embedder  domain.EmbeddingService
	store     domain.VectorStore
	sparseGen *sparse.SparseGenerator
}

// NewSemanticCacheApp creates a new application instance.
func NewSemanticCacheApp(embedder domain.EmbeddingService, store domain.VectorStore) *SemanticCacheApp {
	return &SemanticCacheApp{
		embedder:  embedder,
		store:     store,
		sparseGen: sparse.NewSparseGenerator(),
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
	
	var val structpb.Value
	if err := anyVal.UnmarshalTo(&val); err == nil {
		if val.GetListValue() != nil {
			list := val.GetListValue()
			vec := make([]float32, 0, len(list.Values))
			for _, v := range list.Values {
				vec = append(vec, float32(v.GetNumberValue()))
			}
			return vec, nil
		} else if val.GetStringValue() != "" {
			var vec []float32
			if errJson := json.Unmarshal([]byte(val.GetStringValue()), &vec); errJson == nil {
				return vec, nil
			}
		}
	}

	var list structpb.ListValue
	if err := anyVal.UnmarshalTo(&list); err == nil {
		if len(list.Values) > 0 {
			vec := make([]float32, 0, len(list.Values))
			for _, v := range list.Values {
				vec = append(vec, float32(v.GetNumberValue()))
			}
			return vec, nil
		}
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
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Query Input received (length: %d)", len(text))
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
		vector, err = extractVectorFromAny(contract.EmbeddingLookup)
		if err != nil {
			logger.Error("SemanticCache", "Failed to extract vector: %v", err)
			return false, "", 0, fmt.Errorf("failed to extract vector: %w", err)
		}
	}

	if len(vector) == 0 {
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Stage 2: Generating embedding for input text (length: %d)", len(text))
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
		vector, embErr = extractVectorFromAny(embeddingPayload)
		if embErr != nil {
			logger.Error("SemanticCache", "Failed to extract vector from payload: %v", embErr)
			return fmt.Errorf("failed to extract vector from payload: %w", embErr)
		}
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

// MaskEntities processes natural entity surrogate masking for a document.
func (a *SemanticCacheApp) MaskEntities(ctx context.Context, req *pb.MaskEntitiesRequest) (*pb.MaskEntitiesResponse, error) {
	if req == nil {
		panic(fmt.Errorf("action failed for job MaskEntities: request cannot be nil"))
	}
	identity := req.GetIdentity()
	if identity == nil {
		panic("missing required property: identity cannot be nil")
	}
	if strings.TrimSpace(identity.TenantId) == "" {
		panic("missing required property: TenantId is required")
	}
	if strings.TrimSpace(identity.AppId) == "" {
		panic("missing required property: AppId is required")
	}
	if strings.TrimSpace(identity.JobId) == "" {
		panic("missing required property: JobId is required")
	}

	masker := masking.NewEntityMasker()
	maskedText, surrogateMap, totalCount := masker.MaskText(req.GetDocumentText(), req.GetEntities())

	return &pb.MaskEntitiesResponse{
		MaskedText:        maskedText,
		SurrogateMap:      surrogateMap,
		TotalReplacements: int32(totalCount),
	}, nil
}

// SemanticChunkDocument processes section-isolated document chunking with metadata header injection and optional masking.
func (a *SemanticCacheApp) SemanticChunkDocument(ctx context.Context, req *pb.SemanticChunkDocumentRequest) (*pb.SemanticChunkDocumentResponse, error) {
	if req == nil {
		panic(fmt.Errorf("action failed for job SemanticChunkDocument: request cannot be nil"))
	}
	identity := req.GetIdentity()
	if identity == nil {
		panic("missing required property: identity cannot be nil")
	}
	if strings.TrimSpace(identity.TenantId) == "" {
		panic("missing required property: TenantId is required")
	}
	if strings.TrimSpace(identity.AppId) == "" {
		panic("missing required property: AppId is required")
	}
	if strings.TrimSpace(identity.JobId) == "" {
		panic("missing required property: JobId is required")
	}

	textToChunk := req.GetDocumentText()
	var surrogateMap map[string]string

	if req.GetApplyMasking() && len(req.GetEntities()) > 0 {
		masker := masking.NewEntityMasker()
		var count int
		textToChunk, surrogateMap, count = masker.MaskText(textToChunk, req.GetEntities())
		logger.Info("SemanticCache", "INFO: [SemanticCacheApp] Masked %d entity occurrences before chunking", count)
	}

	maxTokens := int(req.GetMaxChunkTokens())
	if maxTokens <= 0 {
		maxTokens = 512
	}
	overlapTokens := int(req.GetOverlapTokens())
	if overlapTokens < 0 {
		overlapTokens = 50
	}

	sc := chunker.NewSectionChunker(maxTokens, overlapTokens)
	chunks := sc.ChunkDocument(textToChunk, req.GetDocumentType(), identity)

	totalTokens := 0
	for _, chunk := range chunks {
		totalTokens += int(chunk.GetTokenCount())
	}

	return &pb.SemanticChunkDocumentResponse{
		Chunks:       chunks,
		SurrogateMap: surrogateMap,
		TotalChunks:  int32(len(chunks)),
		TotalTokens:  int32(totalTokens),
	}, nil
}

// GenerateSparseEmbedding generates deterministic term-frequency sparse vectors.
func (a *SemanticCacheApp) GenerateSparseEmbedding(ctx context.Context, req *pb.GenerateSparseEmbeddingRequest) (*pb.GenerateSparseEmbeddingResponse, error) {
	if req == nil {
		panic(fmt.Errorf("action failed for job GenerateSparseEmbedding: request cannot be nil"))
	}
	identity := req.GetIdentity()
	if identity == nil {
		panic("missing required property: identity cannot be nil")
	}
	if strings.TrimSpace(identity.TenantId) == "" {
		panic("missing required property: TenantId is required")
	}
	if strings.TrimSpace(identity.AppId) == "" {
		panic("missing required property: AppId is required")
	}
	if strings.TrimSpace(identity.JobId) == "" {
		panic("missing required property: JobId is required")
	}

	sVec := a.sparseGen.Generate(req.GetText())
	return &pb.GenerateSparseEmbeddingResponse{
		SparseVector: &pb.SparseVectorProto{
			Indices: sVec.Indices,
			Values:  sVec.Values,
		},
		UniqueTerms: int32(len(sVec.Indices)),
	}, nil
}

// HybridCheckCache performs convex linear hybrid search over dense and sparse vectors.
func (a *SemanticCacheApp) HybridCheckCache(ctx context.Context, req *pb.HybridCheckCacheRequest) (*pb.HybridCheckCacheResponse, error) {
	if req == nil {
		panic(fmt.Errorf("action failed for job HybridCheckCache: request cannot be nil"))
	}
	identity := req.GetIdentity()
	if identity == nil {
		panic("missing required property: identity cannot be nil")
	}
	if strings.TrimSpace(identity.TenantId) == "" {
		panic("missing required property: TenantId is required")
	}
	if strings.TrimSpace(identity.AppId) == "" {
		panic("missing required property: AppId is required")
	}
	if strings.TrimSpace(identity.JobId) == "" {
		panic("missing required property: JobId is required")
	}

	denseVector := req.GetDenseVector()
	if len(denseVector) == 0 && req.GetQueryText() != "" {
		var err error
		denseVector, err = a.embedder.Generate(ctx, req.GetQueryText())
		if err != nil {
			return nil, fmt.Errorf("failed to generate dense embedding: %w", err)
		}
	}

	var sparseVec domain.SparseVector
	if req.GetSparseVector() != nil && len(req.GetSparseVector().GetIndices()) > 0 {
		sparseVec.Indices = req.GetSparseVector().GetIndices()
		sparseVec.Values = req.GetSparseVector().GetValues()
	} else if req.GetQueryText() != "" {
		genVec := a.sparseGen.Generate(req.GetQueryText())
		sparseVec.Indices = genVec.Indices
		sparseVec.Values = genVec.Values
	}

	var metaMap map[string]any
	if req.GetMetadataLookup() != nil {
		metaMap = req.GetMetadataLookup().AsMap()
	}

	alpha := req.GetAlpha()
	if alpha <= 0 {
		alpha = 0.7
	}

	threshold := req.GetConfidenceThreshold()
	if threshold <= 0 {
		threshold = 0.85
	}

	results, err := a.store.HybridSearch(ctx, req.GetCollectionName(), denseVector, &sparseVec, identity, metaMap, alpha, 1)
	if err != nil {
		return nil, fmt.Errorf("hybrid search failed: %w", err)
	}

	if len(results) > 0 && results[0].Score >= threshold {
		top := results[0]
		return &pb.HybridCheckCacheResponse{
			Hit:              true,
			ExtractedPayload: top.Record.JSONPayload,
			CombinedScore:    top.Score,
			DenseScore:       top.DenseScore,
			SparseScore:      top.SparseScore,
		}, nil
	}

	return &pb.HybridCheckCacheResponse{
		Hit: false,
	}, nil
}

// HybridStoreExtraction stores dual dense and sparse vectors into the vector store.
func (a *SemanticCacheApp) HybridStoreExtraction(ctx context.Context, req *pb.HybridStoreExtractionRequest) (*pb.HybridStoreExtractionResponse, error) {
	if req == nil {
		panic(fmt.Errorf("action failed for job HybridStoreExtraction: request cannot be nil"))
	}
	identity := req.GetIdentity()
	if identity == nil {
		panic("missing required property: identity cannot be nil")
	}
	if strings.TrimSpace(identity.TenantId) == "" {
		panic("missing required property: TenantId is required")
	}
	if strings.TrimSpace(identity.AppId) == "" {
		panic("missing required property: AppId is required")
	}
	if strings.TrimSpace(identity.JobId) == "" {
		panic("missing required property: JobId is required")
	}

	var metaMap map[string]any
	if req.GetMetadata() != nil {
		metaMap = req.GetMetadata().AsMap()
	} else {
		metaMap = make(map[string]any)
	}

	if req.GetDocumentType() != "" {
		metaMap["doc_type"] = req.GetDocumentType()
	}
	if req.GetSectionTitle() != "" {
		metaMap["section_title"] = req.GetSectionTitle()
	}

	denseVector := req.GetDenseVector()
	if len(denseVector) == 0 && req.GetExtractedPayload() != "" {
		var err error
		denseVector, err = a.embedder.Generate(ctx, req.GetExtractedPayload())
		if err != nil {
			return nil, fmt.Errorf("failed to generate dense vector for store: %w", err)
		}
	}

	var sparseVec *domain.SparseVector
	if req.GetSparseVector() != nil && len(req.GetSparseVector().GetIndices()) > 0 {
		sparseVec = &domain.SparseVector{
			Indices: req.GetSparseVector().GetIndices(),
			Values:  req.GetSparseVector().GetValues(),
		}
	} else if req.GetExtractedPayload() != "" {
		genVec := a.sparseGen.Generate(req.GetExtractedPayload())
		sparseVec = &domain.SparseVector{
			Indices: genVec.Indices,
			Values:  genVec.Values,
		}
	}

	record := domain.CacheRecord{
		ID:          uuid.New().String(),
		Identity:    identity,
		Metadata:    metaMap,
		Vector:      denseVector,
		SparseVec:   sparseVec,
		JSONPayload: req.GetExtractedPayload(),
	}

	err := a.store.UpsertHybrid(ctx, req.GetCollectionName(), record)
	if err != nil {
		return &pb.HybridStoreExtractionResponse{Success: false}, fmt.Errorf("upsert hybrid failed: %w", err)
	}

	return &pb.HybridStoreExtractionResponse{Success: true}, nil
}



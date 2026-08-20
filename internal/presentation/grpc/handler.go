package grpc

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/identityparser"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// SemanticCacheHandler implements the SemanticCacheService gRPC interface.
type SemanticCacheHandler struct {
	pb.UnimplementedSemanticCacheServiceServer
	app *application.SemanticCacheApp
}

// NewSemanticCacheHandler creates a new gRPC handler.
func NewSemanticCacheHandler(app *application.SemanticCacheApp) *SemanticCacheHandler {
	return &SemanticCacheHandler{
		app: app,
	}
}

// CheckCache handles the CheckCache gRPC request.
func (h *SemanticCacheHandler) CheckCache(ctx context.Context, req *pb.CheckCacheRequest) (*pb.CheckCacheResponse, error) {
	if err := identityparser.NewIdentityParser(req.GetIdentity()).Validate(true); err != nil {
		logger.Error("SemanticCache", "FAIL-FAST ASSERTION FAILED [gRPC CheckCache]: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
	}
	if strings.TrimSpace(req.CollectionName) == "" {
		return nil, status.Error(codes.InvalidArgument, "CollectionName is required")
	}

	logger.Info("SemanticCache", "INFO: [gRPC] Received CheckCache request for Collection: %s, JobId: %s", req.CollectionName, req.Identity.JobId)

	contract := &pb.CacheContract{
		Enabled:         req.Enabled,
		CollectionName:  req.CollectionName,
		CacheHit:        req.CacheHit,
		Confidence:      req.Confidence,
		EmbeddingLookup: req.EmbeddingLookup,
		MetadataLookup:  req.MetadataLookup,
	}

	hit, payload, confidence, err := h.app.CheckCache(ctx, contract, req.Identity, nil)
	if err != nil {
		logger.Info("SemanticCache", "ERROR: [gRPC] CheckCache failed: %v", err)
		return nil, fmt.Errorf("application layer CheckCache failed: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] CheckCache successful. Returning Hit=%v, Confidence=%.4f", hit, confidence)
	return &pb.CheckCacheResponse{
		Hit:              hit,
		ExtractedPayload: payload,
		Confidence:       confidence,
	}, nil
}

// StoreExtraction handles the StoreExtraction gRPC request.
func (h *SemanticCacheHandler) StoreExtraction(ctx context.Context, req *pb.StoreExtractionRequest) (*pb.StoreExtractionResponse, error) {
	if err := identityparser.NewIdentityParser(req.GetIdentity()).Validate(true); err != nil {
		logger.Error("SemanticCache", "FAIL-FAST ASSERTION FAILED [gRPC StoreExtraction]: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
	}
	if strings.TrimSpace(req.CollectionName) == "" {
		return nil, status.Error(codes.InvalidArgument, "CollectionName is required")
	}

	logger.Info("SemanticCache", "INFO: [gRPC] Received StoreExtraction request for Collection: %s, JobId: %s", req.CollectionName, req.Identity.JobId)

	var metaMap map[string]any
	if req.Metadata != nil {
		metaMap = req.Metadata.AsMap()
	}
	err := h.app.StoreExtraction(ctx, req.CollectionName, req.Identity, metaMap, req.ExtractedPayload, req.EmbeddingPayload)
	if err != nil {
		logger.Info("SemanticCache", "ERROR: [gRPC] StoreExtraction failed: %v", err)
		return &pb.StoreExtractionResponse{Success: false}, fmt.Errorf("application layer StoreExtraction failed: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] StoreExtraction successful.")
	return &pb.StoreExtractionResponse{Success: true}, nil
}

// SeedCache handles the SeedCache gRPC request.
func (h *SemanticCacheHandler) SeedCache(ctx context.Context, req *pb.SeedCacheRequest) (*pb.SeedCacheResponse, error) {
	if err := identityparser.NewIdentityParser(req.GetIdentity()).Validate(true); err != nil {
		logger.Error("SemanticCache", "FAIL-FAST ASSERTION FAILED [gRPC SeedCache]: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
	}
	if strings.TrimSpace(req.CollectionName) == "" {
		return nil, status.Error(codes.InvalidArgument, "CollectionName is required")
	}

	logger.Info("SemanticCache", "INFO: [gRPC] Received SeedCache request for Collection: %s, JobId: %s", req.CollectionName, req.Identity.JobId)

	// Seeding uses the exact same underlying logic as storing an extraction
	var metaMap map[string]any
	if req.Metadata != nil {
		metaMap = req.Metadata.AsMap()
	}
	err := h.app.StoreExtraction(ctx, req.CollectionName, req.Identity, metaMap, req.ExtractedPayload, req.EmbeddingPayload)
	if err != nil {
		logger.Info("SemanticCache", "ERROR: [gRPC] SeedCache failed: %v", err)
		return &pb.SeedCacheResponse{Success: false, Message: err.Error()}, fmt.Errorf("application layer StoreExtraction for seeding failed: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] SeedCache successful.")
	return &pb.SeedCacheResponse{Success: true, Message: "Successfully seeded cache"}, nil
}

// CheckMetadataExists handles the CheckMetadataExists gRPC request.
func (h *SemanticCacheHandler) CheckMetadataExists(ctx context.Context, req *pb.CheckMetadataRequest) (*pb.CheckMetadataResponse, error) {
	if err := identityparser.NewIdentityParser(req.GetIdentity()).Validate(true); err != nil {
		logger.Error("SemanticCache", "FAIL-FAST ASSERTION FAILED [gRPC CheckMetadataExists]: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
	}
	if strings.TrimSpace(req.CollectionName) == "" {
		return nil, status.Error(codes.InvalidArgument, "CollectionName is required")
	}

	logger.Info("SemanticCache", "INFO: [gRPC] Received CheckMetadataExists request for Collection: %s, JobId: %s", req.CollectionName, req.Identity.JobId)

	var metaMap map[string]any
	if req.Metadata != nil {
		metaMap = req.Metadata.AsMap()
	}
	exists, err := h.app.CheckMetadata(ctx, req.CollectionName, req.Identity, metaMap)
	if err != nil {
		logger.Info("SemanticCache", "ERROR: [gRPC] CheckMetadataExists failed: %v", err)
		return nil, fmt.Errorf("application layer CheckMetadata failed: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] CheckMetadataExists successful.")
	return &pb.CheckMetadataResponse{Exists: exists}, nil
}

// QueryBlankDocument handles the QueryBlankDocument gRPC request.
func (h *SemanticCacheHandler) QueryBlankDocument(ctx context.Context, req *pb.QueryBlankDocumentRequest) (*pb.QueryBlankDocumentResponse, error) {
	if err := identityparser.NewIdentityParser(req.GetIdentity()).Validate(true); err != nil {
		logger.Error("SemanticCache", "FAIL-FAST ASSERTION FAILED [gRPC QueryBlankDocument]: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
	}
	if strings.TrimSpace(req.CollectionName) == "" {
		return nil, status.Error(codes.InvalidArgument, "CollectionName is required")
	}

	reqJson, _ := protojson.Marshal(req)
	logger.Info("SemanticCache", "INFO: [gRPC] Received QueryBlankDocument request: %s", string(reqJson))

	contract := &pb.CacheContract{
		CollectionName:  req.CollectionName,
		EmbeddingLookup: req.EmbeddingLookup,
		Confidence:      req.ConfidenceThreshold,
	}

	hit, payload, confidence, err := h.app.CheckCache(ctx, contract, req.Identity, nil)
	if err != nil {
		logger.Error("SemanticCache", "ERROR: [gRPC] QueryBlankDocument failed: %v", err)
		return nil, fmt.Errorf("application layer CheckCache failed: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] QueryBlankDocument successful. Hit=%v, Confidence=%.4f", hit, confidence)
	return &pb.QueryBlankDocumentResponse{
		IsBlankMatch:          hit,
		Confidence:            confidence,
		MatchedClassification: payload,
	}, nil
}

// RegisterBlankDocument handles the RegisterBlankDocument gRPC request.
func (h *SemanticCacheHandler) RegisterBlankDocument(ctx context.Context, req *pb.RegisterBlankDocumentRequest) (*pb.RegisterBlankDocumentResponse, error) {
	if err := identityparser.NewIdentityParser(req.GetIdentity()).Validate(true); err != nil {
		logger.Error("SemanticCache", "FAIL-FAST ASSERTION FAILED [gRPC RegisterBlankDocument]: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
	}
	if strings.TrimSpace(req.CollectionName) == "" {
		return nil, status.Error(codes.InvalidArgument, "CollectionName is required")
	}

	reqJson, _ := protojson.Marshal(req)
	logger.Info("SemanticCache", "INFO: [gRPC] Received RegisterBlankDocument request: %s", string(reqJson))

	if req.EmbeddingPayload == nil {
		return nil, status.Error(codes.InvalidArgument, "EmbeddingPayload cannot be empty")
	}
	// For blank document registration, we can just use a constant or extract string if needed for payload.
	// We'll store a placeholder payload for blank documents
	blankMeta := map[string]any{
		"subject_class":  "BLANK_DOCUMENT",
		"document_title": "BLANK_DOCUMENT",
		"TemplateHash":   "BLANK_HASH",
	}
	err := h.app.StoreExtraction(ctx, req.CollectionName, req.Identity, blankMeta, "BLANK_DOCUMENT", req.EmbeddingPayload)
	if err != nil {
		logger.Error("SemanticCache", "ERROR: [gRPC] RegisterBlankDocument failed: %v", err)
		return &pb.RegisterBlankDocumentResponse{Success: false}, fmt.Errorf("application layer StoreExtraction failed: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] RegisterBlankDocument successful.")
	return &pb.RegisterBlankDocumentResponse{Success: true}, nil
}

// MaskEntities handles the MaskEntities gRPC request.
func (h *SemanticCacheHandler) MaskEntities(ctx context.Context, req *pb.MaskEntitiesRequest) (*pb.MaskEntitiesResponse, error) {
	if err := identityparser.NewIdentityParser(req.GetIdentity()).Validate(true); err != nil {
		logger.Error("SemanticCache", "FAIL-FAST ASSERTION FAILED [gRPC MaskEntities]: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] Received MaskEntities request for JobId: %s, EntityCount: %d", req.GetIdentity().GetJobId(), len(req.GetEntities()))

	resp, err := h.app.MaskEntities(ctx, req)
	if err != nil {
		logger.Error("SemanticCache", "ERROR: [gRPC] MaskEntities failed: %v", err)
		return nil, fmt.Errorf("application layer MaskEntities failed: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] MaskEntities successful. TotalReplacements=%d", resp.GetTotalReplacements())
	return resp, nil
}

// SemanticChunkDocument handles the SemanticChunkDocument gRPC request.
func (h *SemanticCacheHandler) SemanticChunkDocument(ctx context.Context, req *pb.SemanticChunkDocumentRequest) (*pb.SemanticChunkDocumentResponse, error) {
	if err := identityparser.NewIdentityParser(req.GetIdentity()).Validate(true); err != nil {
		logger.Error("SemanticCache", "FAIL-FAST ASSERTION FAILED [gRPC SemanticChunkDocument]: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] Received SemanticChunkDocument request for DocType: %s, JobId: %s", req.GetDocumentType(), req.GetIdentity().GetJobId())

	resp, err := h.app.SemanticChunkDocument(ctx, req)
	if err != nil {
		logger.Error("SemanticCache", "ERROR: [gRPC] SemanticChunkDocument failed: %v", err)
		return nil, fmt.Errorf("application layer SemanticChunkDocument failed: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] SemanticChunkDocument successful. TotalChunks=%d, TotalTokens=%d", resp.GetTotalChunks(), resp.GetTotalTokens())
	return resp, nil
}

// GenerateSparseEmbedding handles the GenerateSparseEmbedding gRPC request.
func (h *SemanticCacheHandler) GenerateSparseEmbedding(ctx context.Context, req *pb.GenerateSparseEmbeddingRequest) (*pb.GenerateSparseEmbeddingResponse, error) {
	if err := identityparser.NewIdentityParser(req.GetIdentity()).Validate(true); err != nil {
		logger.Error("SemanticCache", "FAIL-FAST ASSERTION FAILED [gRPC GenerateSparseEmbedding]: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
	}

	resp, err := h.app.GenerateSparseEmbedding(ctx, req)
	if err != nil {
		logger.Error("SemanticCache", "ERROR: [gRPC] GenerateSparseEmbedding failed: %v", err)
		return nil, fmt.Errorf("application layer GenerateSparseEmbedding failed: %w", err)
	}

	return resp, nil
}

// HybridCheckCache handles the HybridCheckCache gRPC request.
func (h *SemanticCacheHandler) HybridCheckCache(ctx context.Context, req *pb.HybridCheckCacheRequest) (*pb.HybridCheckCacheResponse, error) {
	if err := identityparser.NewIdentityParser(req.GetIdentity()).Validate(true); err != nil {
		logger.Error("SemanticCache", "FAIL-FAST ASSERTION FAILED [gRPC HybridCheckCache]: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
	}
	if strings.TrimSpace(req.CollectionName) == "" {
		return nil, status.Error(codes.InvalidArgument, "CollectionName is required")
	}

	resp, err := h.app.HybridCheckCache(ctx, req)
	if err != nil {
		logger.Error("SemanticCache", "ERROR: [gRPC] HybridCheckCache failed: %v", err)
		return nil, fmt.Errorf("application layer HybridCheckCache failed: %w", err)
	}

	return resp, nil
}

// HybridStoreExtraction handles the HybridStoreExtraction gRPC request.
func (h *SemanticCacheHandler) HybridStoreExtraction(ctx context.Context, req *pb.HybridStoreExtractionRequest) (*pb.HybridStoreExtractionResponse, error) {
	if err := identityparser.NewIdentityParser(req.GetIdentity()).Validate(true); err != nil {
		logger.Error("SemanticCache", "FAIL-FAST ASSERTION FAILED [gRPC HybridStoreExtraction]: %v", err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid identity: %v", err)
	}
	if strings.TrimSpace(req.CollectionName) == "" {
		return nil, status.Error(codes.InvalidArgument, "CollectionName is required")
	}

	resp, err := h.app.HybridStoreExtraction(ctx, req)
	if err != nil {
		logger.Error("SemanticCache", "ERROR: [gRPC] HybridStoreExtraction failed: %v", err)
		return nil, fmt.Errorf("application layer HybridStoreExtraction failed: %w", err)
	}

	return resp, nil
}



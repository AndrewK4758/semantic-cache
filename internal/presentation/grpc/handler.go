package grpc

import (
	"context"
	"fmt"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/doc_processor/semantic_cache_service/internal/application"
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

func buildMetadata(reqMetadata map[string]string, identity *pb.InfrastructureIdentity) map[string]any {
	metadata := make(map[string]any)
	for k, v := range reqMetadata {
		metadata[k] = v
	}
	if identity != nil {
		metadata["tenant_id"] = identity.TenantId
		metadata["app_id"] = identity.AppId
	}
	return metadata
}

// CheckCache handles the CheckCache gRPC request.
func (h *SemanticCacheHandler) CheckCache(ctx context.Context, req *pb.CheckCacheRequest) (*pb.CheckCacheResponse, error) {
	logger.Info("SemanticCache", "INFO: [gRPC] Received CheckCache request. Collection: %s", req.CollectionName)

	metadata := buildMetadata(req.Metadata, req.Identity)

	hit, payload, confidence, err := h.app.CheckCache(ctx, req.CollectionName, req.Text, metadata, req.Threshold)
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
	logger.Info("SemanticCache", "INFO: [gRPC] Received StoreExtraction request. Collection: %s", req.CollectionName)

	metadata := buildMetadata(req.Metadata, req.Identity)

	err := h.app.StoreExtraction(ctx, req.CollectionName, req.Text, metadata, req.ExtractedPayload)
	if err != nil {
		logger.Info("SemanticCache", "ERROR: [gRPC] StoreExtraction failed: %v", err)
		return &pb.StoreExtractionResponse{Success: false}, fmt.Errorf("application layer StoreExtraction failed: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] StoreExtraction successful.")
	return &pb.StoreExtractionResponse{Success: true}, nil
}

// SeedCache handles the SeedCache gRPC request.
func (h *SemanticCacheHandler) SeedCache(ctx context.Context, req *pb.SeedCacheRequest) (*pb.SeedCacheResponse, error) {
	logger.Info("SemanticCache", "INFO: [gRPC] Received SeedCache request. Collection: %s", req.CollectionName)

	metadata := buildMetadata(req.Metadata, req.Identity)

	// Seeding uses the exact same underlying logic as storing an extraction
	err := h.app.StoreExtraction(ctx, req.CollectionName, req.TemplateText, metadata, req.ExtractedPayload)
	if err != nil {
		logger.Info("SemanticCache", "ERROR: [gRPC] SeedCache failed: %v", err)
		return &pb.SeedCacheResponse{Success: false, Message: err.Error()}, fmt.Errorf("application layer StoreExtraction for seeding failed: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] SeedCache successful.")
	return &pb.SeedCacheResponse{Success: true, Message: "Successfully seeded cache"}, nil
}

// CheckMetadataExists handles the CheckMetadataExists gRPC request.
func (h *SemanticCacheHandler) CheckMetadataExists(ctx context.Context, req *pb.CheckMetadataRequest) (*pb.CheckMetadataResponse, error) {
	logger.Info("SemanticCache", "INFO: [gRPC] Received CheckMetadataExists request. Collection: %s", req.CollectionName)

	metadata := buildMetadata(req.Metadata, req.Identity)

	exists, err := h.app.CheckMetadata(ctx, req.CollectionName, metadata)
	if err != nil {
		logger.Info("SemanticCache", "ERROR: [gRPC] CheckMetadataExists failed: %v", err)
		return nil, fmt.Errorf("application layer CheckMetadata failed: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] CheckMetadataExists successful.")
	return &pb.CheckMetadataResponse{Exists: exists}, nil
}

// QueryBlankDocument handles the QueryBlankDocument gRPC request.
func (h *SemanticCacheHandler) QueryBlankDocument(ctx context.Context, req *pb.QueryBlankDocumentRequest) (*pb.QueryBlankDocumentResponse, error) {
	logger.Info("SemanticCache", "INFO: [gRPC] Received QueryBlankDocument request. Collection: %s", req.CollectionName)

	metadata := buildMetadata(nil, req.Identity)
	hit, payload, confidence, err := h.app.CheckCache(ctx, req.CollectionName, req.TextRepresentation, metadata, req.ConfidenceThreshold)
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
	logger.Info("SemanticCache", "INFO: [gRPC] Received RegisterBlankDocument request. Collection: %s", req.CollectionName)

	metadata := buildMetadata(nil, req.Identity)
	
	err := h.app.StoreExtraction(ctx, req.CollectionName, req.TextRepresentation, metadata, "blank_document")
	if err != nil {
		logger.Error("SemanticCache", "ERROR: [gRPC] RegisterBlankDocument failed: %v", err)
		return &pb.RegisterBlankDocumentResponse{Success: false}, fmt.Errorf("application layer StoreExtraction failed: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] RegisterBlankDocument successful.")
	return &pb.RegisterBlankDocumentResponse{Success: true}, nil
}

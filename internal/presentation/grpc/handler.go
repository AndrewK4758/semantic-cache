package grpc

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func toMetadataMap(m map[string]string) map[string]any {
	if len(m) == 0 {
		return nil
	}
	res := make(map[string]any, len(m))
	for k, v := range m {
		res[k] = v
	}
	return res
}

// CheckCache handles the CheckCache gRPC request.
func (h *SemanticCacheHandler) CheckCache(ctx context.Context, req *pb.CheckCacheRequest) (*pb.CheckCacheResponse, error) {
	if req.Identity == nil {
		return nil, status.Error(codes.InvalidArgument, "Identity is required")
	}
	if strings.TrimSpace(req.CollectionName) == "" {
		return nil, status.Error(codes.InvalidArgument, "CollectionName is required")
	}

	logger.Info("SemanticCache", "INFO: [gRPC] Received CheckCache request. Collection: %s", req.CollectionName)

	hit, payload, confidence, err := h.app.CheckCache(ctx, req.CollectionName, req.Text, req.Identity, toMetadataMap(req.Metadata), req.Threshold)
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
	if req.Identity == nil {
		return nil, status.Error(codes.InvalidArgument, "Identity is required")
	}
	if strings.TrimSpace(req.CollectionName) == "" {
		return nil, status.Error(codes.InvalidArgument, "CollectionName is required")
	}

	logger.Info("SemanticCache", "INFO: [gRPC] Received StoreExtraction request. Collection: %s", req.CollectionName)

	err := h.app.StoreExtraction(ctx, req.CollectionName, req.Text, req.Identity, toMetadataMap(req.Metadata), req.ExtractedPayload)
	if err != nil {
		logger.Info("SemanticCache", "ERROR: [gRPC] StoreExtraction failed: %v", err)
		return &pb.StoreExtractionResponse{Success: false}, fmt.Errorf("application layer StoreExtraction failed: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] StoreExtraction successful.")
	return &pb.StoreExtractionResponse{Success: true}, nil
}

// SeedCache handles the SeedCache gRPC request.
func (h *SemanticCacheHandler) SeedCache(ctx context.Context, req *pb.SeedCacheRequest) (*pb.SeedCacheResponse, error) {
	if req.Identity == nil {
		return nil, status.Error(codes.InvalidArgument, "Identity is required")
	}
	if strings.TrimSpace(req.CollectionName) == "" {
		return nil, status.Error(codes.InvalidArgument, "CollectionName is required")
	}

	logger.Info("SemanticCache", "INFO: [gRPC] Received SeedCache request. Collection: %s", req.CollectionName)

	// Seeding uses the exact same underlying logic as storing an extraction
	err := h.app.StoreExtraction(ctx, req.CollectionName, req.TemplateText, req.Identity, toMetadataMap(req.Metadata), req.ExtractedPayload)
	if err != nil {
		logger.Info("SemanticCache", "ERROR: [gRPC] SeedCache failed: %v", err)
		return &pb.SeedCacheResponse{Success: false, Message: err.Error()}, fmt.Errorf("application layer StoreExtraction for seeding failed: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] SeedCache successful.")
	return &pb.SeedCacheResponse{Success: true, Message: "Successfully seeded cache"}, nil
}

// CheckMetadataExists handles the CheckMetadataExists gRPC request.
func (h *SemanticCacheHandler) CheckMetadataExists(ctx context.Context, req *pb.CheckMetadataRequest) (*pb.CheckMetadataResponse, error) {
	if req.Identity == nil {
		return nil, status.Error(codes.InvalidArgument, "Identity is required")
	}
	if strings.TrimSpace(req.CollectionName) == "" {
		return nil, status.Error(codes.InvalidArgument, "CollectionName is required")
	}

	logger.Info("SemanticCache", "INFO: [gRPC] Received CheckMetadataExists request. Collection: %s", req.CollectionName)

	exists, err := h.app.CheckMetadata(ctx, req.CollectionName, req.Identity, toMetadataMap(req.Metadata))
	if err != nil {
		logger.Info("SemanticCache", "ERROR: [gRPC] CheckMetadataExists failed: %v", err)
		return nil, fmt.Errorf("application layer CheckMetadata failed: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] CheckMetadataExists successful.")
	return &pb.CheckMetadataResponse{Exists: exists}, nil
}

// QueryBlankDocument handles the QueryBlankDocument gRPC request.
func (h *SemanticCacheHandler) QueryBlankDocument(ctx context.Context, req *pb.QueryBlankDocumentRequest) (*pb.QueryBlankDocumentResponse, error) {
	if req.Identity == nil {
		return nil, status.Error(codes.InvalidArgument, "Identity is required")
	}
	if strings.TrimSpace(req.CollectionName) == "" {
		return nil, status.Error(codes.InvalidArgument, "CollectionName is required")
	}

	logger.Info("SemanticCache", "INFO: [gRPC] Received QueryBlankDocument request. Collection: %s", req.CollectionName)

	hit, payload, confidence, err := h.app.CheckCache(ctx, req.CollectionName, req.TextRepresentation, req.Identity, nil, req.ConfidenceThreshold)
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
	if req.Identity == nil {
		return nil, status.Error(codes.InvalidArgument, "Identity is required")
	}
	if strings.TrimSpace(req.CollectionName) == "" {
		return nil, status.Error(codes.InvalidArgument, "CollectionName is required")
	}

	logger.Info("SemanticCache", "INFO: [gRPC] Received RegisterBlankDocument request. Collection: %s", req.CollectionName)

	payload := req.TextRepresentation
	if strings.TrimSpace(payload) == "" {
		return nil, status.Error(codes.InvalidArgument, "TextRepresentation cannot be empty")
	}
	err := h.app.StoreExtraction(ctx, req.CollectionName, req.TextRepresentation, req.Identity, nil, payload)
	if err != nil {
		logger.Error("SemanticCache", "ERROR: [gRPC] RegisterBlankDocument failed: %v", err)
		return &pb.RegisterBlankDocumentResponse{Success: false}, fmt.Errorf("application layer StoreExtraction failed: %w", err)
	}

	logger.Info("SemanticCache", "INFO: [gRPC] RegisterBlankDocument successful.")
	return &pb.RegisterBlankDocumentResponse{Success: true}, nil
}

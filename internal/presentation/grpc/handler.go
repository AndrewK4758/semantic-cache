package grpc

import (
	"context"

	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/doc_processor/semantic_cache_service/pkg/pb"
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
	hit, payload, confidence, err := h.app.CheckCache(ctx, req.Text, req.Metadata, req.Threshold)
	if err != nil {
		return nil, err
	}

	return &pb.CheckCacheResponse{
		Hit:              hit,
		ExtractedPayload: payload,
		Confidence:       confidence,
	}, nil
}

// StoreExtraction handles the StoreExtraction gRPC request.
func (h *SemanticCacheHandler) StoreExtraction(ctx context.Context, req *pb.StoreExtractionRequest) (*pb.StoreExtractionResponse, error) {
	err := h.app.StoreExtraction(ctx, req.Text, req.Metadata, req.ExtractedPayload)
	if err != nil {
		return &pb.StoreExtractionResponse{Success: false}, err
	}

	return &pb.StoreExtractionResponse{Success: true}, nil
}

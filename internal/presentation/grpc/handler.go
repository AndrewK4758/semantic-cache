package grpc

import (
	"context"
	"log"

	pb "github.com/AndrewK4758/shared_protos"
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

// CheckCache handles the CheckCache gRPC request.
func (h *SemanticCacheHandler) CheckCache(ctx context.Context, req *pb.CheckCacheRequest) (*pb.CheckCacheResponse, error) {
	log.Printf("INFO: [gRPC] Received CheckCache request. Metadata count: %d", len(req.Metadata))
	
	hit, payload, confidence, err := h.app.CheckCache(ctx, req.Text, req.Metadata, req.Threshold)
	if err != nil {
		log.Printf("ERROR: [gRPC] CheckCache failed: %v", err)
		return nil, err
	}

	log.Printf("INFO: [gRPC] CheckCache successful. Returning Hit=%v, Confidence=%.4f", hit, confidence)
	return &pb.CheckCacheResponse{
		Hit:              hit,
		ExtractedPayload: payload,
		Confidence:       confidence,
	}, nil
}

// StoreExtraction handles the StoreExtraction gRPC request.
func (h *SemanticCacheHandler) StoreExtraction(ctx context.Context, req *pb.StoreExtractionRequest) (*pb.StoreExtractionResponse, error) {
	log.Printf("INFO: [gRPC] Received StoreExtraction request. Metadata count: %d", len(req.Metadata))

	err := h.app.StoreExtraction(ctx, req.Text, req.Metadata, req.ExtractedPayload)
	if err != nil {
		log.Printf("ERROR: [gRPC] StoreExtraction failed: %v", err)
		return &pb.StoreExtractionResponse{Success: false}, err
	}

	log.Printf("INFO: [gRPC] StoreExtraction successful.")
	return &pb.StoreExtractionResponse{Success: true}, nil
}

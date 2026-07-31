package grpc_test

import (
	"context"
	"errors"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
	grpchandler "github.com/doc_processor/semantic_cache_service/internal/presentation/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

type mockEmbedder struct{}

func (m *mockEmbedder) Generate(ctx context.Context, text string) ([]float32, error) {
	return []float32{0.1, 0.2, 0.3}, nil
}

type mockStore struct {
	records map[string]domain.CacheRecord
}

func newMockStore() *mockStore {
	return &mockStore{records: make(map[string]domain.CacheRecord)}
}

func (m *mockStore) Search(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
	if len(m.records) == 0 {
		return nil, nil
	}
	for _, r := range m.records {
		return []domain.SearchResult{
			{Record: r, Score: 0.95},
		}, nil
	}
	return nil, nil
}

func (m *mockStore) Upsert(ctx context.Context, collectionName string, record domain.CacheRecord) error {
	m.records[record.ID] = record
	return nil
}

func (m *mockStore) CheckMetadata(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) (bool, error) {
	return len(m.records) > 0, nil
}

func (m *mockStore) GetByMetadata(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) ([]domain.SearchResult, error) {
	if len(m.records) > 0 {
		for _, r := range m.records {
			return []domain.SearchResult{
				{Record: r, Score: 1.0},
			}, nil
		}
	}
	return nil, nil
}

func TestGRPCHandler_NilIdentityEdgeCases(t *testing.T) {
	store := newMockStore()
	app := application.NewSemanticCacheApp(&mockEmbedder{}, store)
	h := grpchandler.NewSemanticCacheHandler(app)

	ctx := context.Background()

	t.Run("CheckCache with Nil Identity", func(t *testing.T) {
		_, err := h.CheckCache(ctx, &pb.CheckCacheRequest{
			CollectionName: "test",
			Confidence:     0.8,
			Identity:       nil,
		})
		if err == nil {
			t.Fatalf("expected error with nil identity, got nil")
		}
	})

	t.Run("StoreExtraction with Nil Identity", func(t *testing.T) {
		_, err := h.StoreExtraction(ctx, &pb.StoreExtractionRequest{
			CollectionName:   "test",
			ExtractedPayload: `{"test":"val"}`,
			Identity:         nil,
		})
		if err == nil {
			t.Fatalf("expected error with nil identity, got nil")
		}
	})

	t.Run("SeedCache with Nil Identity", func(t *testing.T) {
		_, err := h.SeedCache(ctx, &pb.SeedCacheRequest{
			CollectionName:   "test",
			ExtractedPayload: `{"template":true}`,
			Identity:         nil,
		})
		if err == nil {
			t.Fatalf("expected error with nil identity, got nil")
		}
	})

	t.Run("CheckMetadataExists with Nil Identity", func(t *testing.T) {
		meta, _ := structpb.NewStruct(map[string]any{"foo": "bar"})
		_, err := h.CheckMetadataExists(ctx, &pb.CheckMetadataRequest{
			CollectionName: "test",
			Metadata:       meta,
			Identity:       nil,
		})
		if err == nil {
			t.Fatalf("expected error with nil identity, got nil")
		}
	})

	t.Run("QueryBlankDocument with Nil Identity", func(t *testing.T) {
		_, err := h.QueryBlankDocument(ctx, &pb.QueryBlankDocumentRequest{
			CollectionName:     "test",
			Identity:           nil,
		})
		if err == nil {
			t.Fatalf("expected error with nil identity, got nil")
		}
	})

	t.Run("RegisterBlankDocument with Nil Identity and Empty Text", func(t *testing.T) {
		_, err := h.RegisterBlankDocument(ctx, &pb.RegisterBlankDocumentRequest{
			CollectionName:     "test",
			Identity:           nil,
		})
		if err == nil {
			t.Fatalf("expected error with nil identity and empty text, got nil")
		}
	})
}

type errEmbedder struct{}

func (e *errEmbedder) Generate(ctx context.Context, text string) ([]float32, error) {
	return nil, errors.New("embedder failure")
}

func TestGRPCHandler_ErrorHandling(t *testing.T) {
	store := newMockStore()
	app := application.NewSemanticCacheApp(&errEmbedder{}, store)
	h := grpchandler.NewSemanticCacheHandler(app)

	ctx := context.Background()

	t.Run("CheckCache error propagates", func(t *testing.T) {
		_, err := h.CheckCache(ctx, &pb.CheckCacheRequest{
			CollectionName: "test",
			Confidence:     0.8,
			Identity:       &pb.InfrastructureIdentity{JobId: "j", TenantId: "t", AppId: "a"},
		})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("StoreExtraction error returns success false and error", func(t *testing.T) {
		_, err := h.StoreExtraction(ctx, &pb.StoreExtractionRequest{
			CollectionName:   "test",
			ExtractedPayload: "payload",
			Identity:         &pb.InfrastructureIdentity{JobId: "j", TenantId: "t", AppId: "a"},
		})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

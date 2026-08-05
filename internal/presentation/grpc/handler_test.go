package grpc_test

import (
	"context"
	"errors"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
	grpchandler "github.com/doc_processor/semantic_cache_service/internal/presentation/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
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
			CollectionName: "test",
			Identity:       nil,
		})
		if err == nil {
			t.Fatalf("expected error with nil identity, got nil")
		}
	})

	t.Run("RegisterBlankDocument with Nil Identity and Empty Text", func(t *testing.T) {
		_, err := h.RegisterBlankDocument(ctx, &pb.RegisterBlankDocumentRequest{
			CollectionName: "test",
			Identity:       nil,
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
		st, _ := structpb.NewStruct(map[string]interface{}{
			"subject_class":  "test",
			"TemplateHash":   "hash",
			"document_title": "title",
		})
		_, err := h.StoreExtraction(ctx, &pb.StoreExtractionRequest{
			CollectionName:   "test",
			ExtractedPayload: "payload",
			Identity:         &pb.InfrastructureIdentity{JobId: "j", TenantId: "t", AppId: "a"},
			Metadata:         st,
		})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestGRPCHandler_HappyPath(t *testing.T) {
	store := newMockStore()
	app := application.NewSemanticCacheApp(&mockEmbedder{}, store)
	h := grpchandler.NewSemanticCacheHandler(app)
	ctx := context.Background()

	identity := &pb.InfrastructureIdentity{
		TenantId: "t-grpc-1",
		AppId:    "a-grpc-1",
		JobId:    "j-grpc-1",
	}

	t.Run("StoreExtraction Happy Path", func(t *testing.T) {
		resp, err := h.StoreExtraction(ctx, &pb.StoreExtractionRequest{
			CollectionName:   "coll_happy",
			ExtractedPayload: `{"extracted":true}`,
			Identity:         identity,
			Metadata:         &structpb.Struct{Fields: map[string]*structpb.Value{"subject_class": structpb.NewStringValue("CLASS"), "document_title": structpb.NewStringValue("Title"), "TemplateHash": structpb.NewStringValue("hash")}},
		})
		if err != nil {
			t.Fatalf("unexpected error in StoreExtraction: %v", err)
		}
		if !resp.Success {
			t.Fatalf("expected StoreExtraction success=true")
		}
	})

	t.Run("CheckCache Happy Path", func(t *testing.T) {
		resp, err := h.CheckCache(ctx, &pb.CheckCacheRequest{
			CollectionName: "coll_happy",
			Confidence:     0.8,
			Identity:       identity,
		})
		if err != nil {
			t.Fatalf("unexpected error in CheckCache: %v", err)
		}
		if !resp.Hit {
			t.Fatalf("expected CheckCache hit=true after StoreExtraction")
		}
	})

	t.Run("SeedCache Happy Path", func(t *testing.T) {
		resp, err := h.SeedCache(ctx, &pb.SeedCacheRequest{
			CollectionName:   "coll_seed",
			ExtractedPayload: `{"seeded":true}`,
			Identity:         identity,
			Metadata:         &structpb.Struct{Fields: map[string]*structpb.Value{"subject_class": structpb.NewStringValue("CLASS"), "document_title": structpb.NewStringValue("Title"), "TemplateHash": structpb.NewStringValue("hash")}},
		})
		if err != nil {
			t.Fatalf("unexpected error in SeedCache: %v", err)
		}
		if !resp.Success {
			t.Fatalf("expected SeedCache success=true")
		}
	})

	t.Run("CheckMetadataExists Happy Path", func(t *testing.T) {
		meta, _ := structpb.NewStruct(map[string]any{"env": "test"})
		resp, err := h.CheckMetadataExists(ctx, &pb.CheckMetadataRequest{
			CollectionName: "coll_happy",
			Metadata:       meta,
			Identity:       identity,
		})
		if err != nil {
			t.Fatalf("unexpected error in CheckMetadataExists: %v", err)
		}
		if !resp.Exists {
			t.Fatalf("expected CheckMetadataExists exists=true")
		}
	})

	t.Run("QueryBlankDocument Happy Path", func(t *testing.T) {
		resp, err := h.QueryBlankDocument(ctx, &pb.QueryBlankDocumentRequest{
			CollectionName:      "coll_happy",
			ConfidenceThreshold: 0.8,
			Identity:            identity,
		})
		if err != nil {
			t.Fatalf("unexpected error in QueryBlankDocument: %v", err)
		}
		if !resp.IsBlankMatch {
			t.Fatalf("expected QueryBlankDocument match=true")
		}
	})

	t.Run("RegisterBlankDocument Happy Path", func(t *testing.T) {
		val, _ := structpb.NewValue("blank document text sample")
		anyPayload, _ := anypb.New(val)
		resp, err := h.RegisterBlankDocument(ctx, &pb.RegisterBlankDocumentRequest{
			CollectionName:   "coll_blank",
			EmbeddingPayload: anyPayload,
			Identity:         identity,
		})
		if err != nil {
			t.Fatalf("unexpected error in RegisterBlankDocument: %v", err)
		}
		if !resp.Success {
			t.Fatalf("expected RegisterBlankDocument success=true")
		}
	})
}

func TestGRPCHandler_EmptyCollectionNameValidation(t *testing.T) {
	store := newMockStore()
	app := application.NewSemanticCacheApp(&mockEmbedder{}, store)
	h := grpchandler.NewSemanticCacheHandler(app)
	ctx := context.Background()

	identity := &pb.InfrastructureIdentity{
		TenantId: "t-val",
		AppId:    "a-val",
		JobId:    "j-val",
	}

	assertInvalidArgument := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: expected error for empty collection name, got nil", name)
		}
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.InvalidArgument {
			t.Fatalf("%s: expected InvalidArgument status code, got %v", name, err)
		}
	}

	t.Run("CheckCache empty collection", func(t *testing.T) {
		_, err := h.CheckCache(ctx, &pb.CheckCacheRequest{CollectionName: "", Identity: identity})
		assertInvalidArgument("CheckCache", err)
	})

	t.Run("StoreExtraction empty collection", func(t *testing.T) {
		_, err := h.StoreExtraction(ctx, &pb.StoreExtractionRequest{CollectionName: "", Identity: identity})
		assertInvalidArgument("StoreExtraction", err)
	})

	t.Run("SeedCache empty collection", func(t *testing.T) {
		_, err := h.SeedCache(ctx, &pb.SeedCacheRequest{CollectionName: "", Identity: identity})
		assertInvalidArgument("SeedCache", err)
	})

	t.Run("CheckMetadataExists empty collection", func(t *testing.T) {
		_, err := h.CheckMetadataExists(ctx, &pb.CheckMetadataRequest{CollectionName: "", Identity: identity})
		assertInvalidArgument("CheckMetadataExists", err)
	})

	t.Run("QueryBlankDocument empty collection", func(t *testing.T) {
		_, err := h.QueryBlankDocument(ctx, &pb.QueryBlankDocumentRequest{CollectionName: "", Identity: identity})
		assertInvalidArgument("QueryBlankDocument", err)
	})

	t.Run("RegisterBlankDocument empty collection", func(t *testing.T) {
		val, _ := structpb.NewValue("text")
		anyPayload, _ := anypb.New(val)
		_, err := h.RegisterBlankDocument(ctx, &pb.RegisterBlankDocumentRequest{CollectionName: "", EmbeddingPayload: anyPayload, Identity: identity})
		assertInvalidArgument("RegisterBlankDocument", err)
	})
}

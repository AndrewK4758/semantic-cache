package application_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
	"github.com/doc_processor/semantic_cache_service/internal/infrastructure/qdrant"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// Empirical Challenger 1 Verification Suite for M3 R2

func assertPanicMessageContains(t *testing.T, expectedSubstring string, fn func()) {
	t.Helper()
	var panicVal any
	func() {
		defer func() {
			panicVal = recover()
		}()
		fn()
	}()
	if panicVal == nil {
		t.Fatalf("expected panic containing %q, but code executed without panic", expectedSubstring)
	}
	panicStr := fmt.Sprint(panicVal)
	if expectedSubstring != "" && !strings.Contains(panicStr, expectedSubstring) {
		t.Fatalf("expected panic message containing %q, got %q", expectedSubstring, panicStr)
	}
}

// TestChallenger1_Empirical_StoreExtraction_FailFastPanics verifies panic behaviors on invalid identity and document_title
func TestChallenger1_Empirical_StoreExtraction_FailFastPanics(t *testing.T) {
	app := application.NewSemanticCacheApp(&mockEmbeddingService{}, &mockVectorStore{})
	validId := &pb.InfrastructureIdentity{TenantId: "t-challenger", AppId: "a-challenger", JobId: "j-challenger"}
	val, _ := structpb.NewValue("test text")
	anyPayload, _ := anypb.New(val)

	// 1. Missing/Invalid Identity Panics
	invalidIdentities := []*pb.InfrastructureIdentity{
		nil,
		{TenantId: "", AppId: "app1"},
		{TenantId: "tenant1", AppId: ""},
		{TenantId: "   ", AppId: "app1"},
		{TenantId: "tenant1", AppId: "   "},
	}

	for _, id := range invalidIdentities {
		assertPanicMessageContains(t, "missing required property", func() {
			_ = app.StoreExtraction(context.Background(), "testing_coll", id, map[string]any{"document_title": "Valid Title", "subject_class": "CLASS", "TemplateHash": "hash"}, "Valid Title", anyPayload)
		})
	}

	// 2. Missing/Empty Document Title Panics
	invalidTitles := []string{"", "   ", "\t\n"}
	for _, title := range invalidTitles {
		assertPanicMessageContains(t, "missing required property document_title", func() {
			_ = app.StoreExtraction(context.Background(), "testing_coll", validId, map[string]any{"document_title": title, "subject_class": "CLASS", "TemplateHash": "hash"}, title, anyPayload)
		})
	}
}

// TestChallenger1_Empirical_CheckCache_VectorSearch_NilMetadata_Succeeds verifies vector search with nil metadata works without panic
func TestChallenger1_Empirical_CheckCache_VectorSearch_NilMetadata_Succeeds(t *testing.T) {
	searchCalled := false
	var passedMetadata map[string]any

	embedMock := &mockEmbeddingService{
		generateFn: func(ctx context.Context, text string) ([]float32, error) {
			return []float32{0.1, 0.2, 0.3}, nil
		},
	}

	storeMock := &mockVectorStore{
		searchFn: func(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
			searchCalled = true
			passedMetadata = metadata
			return []domain.SearchResult{
				{
					Score: 0.98,
					Record: domain.CacheRecord{
						JSONPayload: "Closing Disclosure",
						Metadata: map[string]any{
							"document_title": "Closing Disclosure",
							"subject_class":  "CLOSING",
						},
					},
				},
			}, nil
		},
	}

	app := application.NewSemanticCacheApp(embedMock, storeMock)

	contract := &pb.CacheContract{
		CollectionName: "test_coll",
		Confidence:     0.90,
		MetadataLookup: nil, // Metadata is nil
	}
	identity := &pb.InfrastructureIdentity{
		TenantId: "t-challenger",
		AppId:    "a-challenger",
		JobId:    "j-challenger",
	}

	hit, docTitle, conf, err := app.CheckCache(context.Background(), contract, identity)
	if err != nil {
		t.Fatalf("unexpected error during CheckCache with nil metadata: %v", err)
	}

	if !searchCalled {
		t.Fatalf("expected vector store Search to be called during Stage 2")
	}

	if passedMetadata != nil {
		t.Fatalf("expected passed metadata to Search to be nil, got: %v", passedMetadata)
	}

	if !hit {
		t.Fatalf("expected hit to be true, got false")
	}

	if docTitle != "Closing Disclosure" || conf != 0.98 {
		t.Fatalf("unexpected CheckCache result: hit=%v, title=%s, conf=%f", hit, docTitle, conf)
	}
}

// TestChallenger1_Empirical_QdrantClient_EmptyCollection_Panics verifies Qdrant client panics on empty collectionName
func TestChallenger1_Empirical_QdrantClient_EmptyCollection_Panics(t *testing.T) {
	client := &qdrant.Client{}
	validId := &pb.InfrastructureIdentity{TenantId: "t-challenger", AppId: "a-challenger"}

	assertPanicMessageContains(t, "collectionName is required", func() {
		_, _ = client.Search(context.Background(), "", []float32{0.1}, validId, nil, 1)
	})

	assertPanicMessageContains(t, "collectionName is required", func() {
		_ = client.Upsert(context.Background(), "", domain.CacheRecord{Identity: validId})
	})

	assertPanicMessageContains(t, "collectionName is required", func() {
		_, _ = client.CheckMetadata(context.Background(), "", validId, map[string]any{"k": "v"})
	})

	assertPanicMessageContains(t, "collectionName is required", func() {
		_, _ = client.GetByMetadata(context.Background(), "", validId, map[string]any{"k": "v"})
	})
}

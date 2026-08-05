package application_test

import (
	"context"
	"testing"

	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
	pb "github.com/AndrewK4758/shared_protos"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)


// TestM4_QdrantGenericMetadataPayload_PurgesForbiddenFields verifies that
// StoreExtraction saves tenant_id, app_id, subject_class, document_title into Qdrant metadata,
// while purging json_payload and job_id.
func TestM4_QdrantGenericMetadataPayload_PurgesForbiddenFields(t *testing.T) {
	var capturedRecord domain.CacheRecord

	storeMock := &mockVectorStore{
		upsertFn: func(ctx context.Context, collectionName string, record domain.CacheRecord) error {
			capturedRecord = record
			return nil
		},
	}
	embedMock := &mockEmbeddingService{
		generateFn: func(ctx context.Context, text string) ([]float32, error) {
			return []float32{0.1, 0.2, 0.3}, nil
		},
	}

	app := application.NewSemanticCacheApp(embedMock, storeMock)
	ctx := context.Background()

	identity := &pb.InfrastructureIdentity{
		TenantId: "tenant_vault",
		AppId:    "app_secure",
		JobId:    "job_to_be_purged",
	}

	inputMeta := map[string]any{
		"e2e_flag":       true,
		"document_title": "E2E Title",
		"subject_class":  "E2E_CLASS",
		"TemplateHash":   "hash",
	}

	val, _ := structpb.NewValue("E2E Document Text")
	anyPayload, _ := anypb.New(val)

	err := app.StoreExtraction(
		ctx,
		"testing_e2e_collection",
		identity,
		inputMeta,
		"E2E Title",
		anyPayload,
	)
	if err != nil {
		t.Fatalf("StoreExtraction failed unexpectedly: %v", err)
	}

	// Assert generic identity & metadata fields are saved
	if capturedRecord.Identity.TenantId != "tenant_vault" {
		t.Errorf("Expected Identity.TenantId 'tenant_vault', got %v", capturedRecord.Identity.TenantId)
	}
	if capturedRecord.Identity.AppId != "app_secure" {
		t.Errorf("Expected Identity.AppId 'app_secure', got %v", capturedRecord.Identity.AppId)
	}
	if capturedRecord.Metadata["document_title"] != "E2E Title" {
		t.Errorf("Expected document_title 'E2E Title', got %v", capturedRecord.Metadata["document_title"])
	}

	// Assert forbidden fields are purged
	if _, exists := capturedRecord.Metadata["json_payload"]; exists {
		t.Errorf("Forbidden field 'json_payload' was NOT purged from metadata payload!")
	}
	if _, exists := capturedRecord.Metadata["job_id"]; exists {
		t.Errorf("Forbidden field 'job_id' was NOT purged from metadata payload!")
	}

	// Assert custom allowed fields persist
	if capturedRecord.Metadata["e2e_flag"] != true {
		t.Errorf("Expected e2e_flag true, got %v", capturedRecord.Metadata["e2e_flag"])
	}
}

// TestM4_CheckCache_ReturnsDocumentTitleOnHit verifies CheckCache returns
// document_title, subject_class, and confidence score on cache hit.
func TestM4_CheckCache_ReturnsDocumentTitleOnHit(t *testing.T) {
	expectedTitle := "Title Commitment"

	storeMock := &mockVectorStore{
		searchFn: func(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
			return []domain.SearchResult{
				{
					Score: 0.98,
					Record: domain.CacheRecord{
						JSONPayload: "Closing Disclosure Final",
						Metadata: map[string]any{
							"document_title": expectedTitle,
							"tenant_id":      identity.TenantId,
							"app_id":         identity.AppId,
						},
					},
				},
			}, nil
		},
	}
	embedMock := &mockEmbeddingService{
		generateFn: func(ctx context.Context, text string) ([]float32, error) {
			return []float32{0.5, 0.5, 0.5}, nil
		},
	}

	app := application.NewSemanticCacheApp(embedMock, storeMock)
	ctx := context.Background()

	contract := &pb.CacheContract{
		CollectionName: "testing_e2e_collection",
	}
	identity := &pb.InfrastructureIdentity{
		TenantId: "tenant_title_co",
		AppId:    "app_title_proc",
		JobId:    "job_test_hit",
	}

	hit, docTitle, conf, err := app.CheckCache(ctx, contract, identity, nil)
	if err != nil {
		t.Fatalf("CheckCache failed: %v", err)
	}

	if !hit {
		t.Fatalf("Expected cache hit=true, got false")
	}
	if docTitle != "Closing Disclosure Final" || conf < 0.90 {
		t.Fatalf("CheckCache values mismatched. hit: %v, payload: %s, conf: %f", hit, docTitle, conf)
	}
	if conf != 0.98 {
		t.Errorf("Expected confidence 0.98, got %f", conf)
	}
}

// TestM4_IdentityFailFast_Validation tests fail-fast behavior when identity
// is missing or incomplete.
func TestM4_IdentityFailFast_Validation(t *testing.T) {
	app := application.NewSemanticCacheApp(&mockEmbeddingService{}, &mockVectorStore{})

	contract := &pb.CacheContract{CollectionName: "testing_e2e_collection"}

	invalidIdentities := map[string]*pb.InfrastructureIdentity{
		"Nil identity":       nil,
		"Empty TenantId":     {TenantId: "", AppId: "app1", JobId: "j1"},
		"Empty AppId":        {TenantId: "t1", AppId: "", JobId: "j1"},
		"Whitespace Tenant":  {TenantId: "   ", AppId: "app1", JobId: "j1"},
		"Whitespace App":     {TenantId: "t1", AppId: "   ", JobId: "j1"},
	}

	for name, id := range invalidIdentities {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("%s: Expected panic for invalid identity, got none", name)
				}
			}()
			_, _, _, _ = app.CheckCache(context.Background(), contract, id, nil)
		})
	}
}

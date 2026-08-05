package application_test

import (
	"context"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestChallenger2_EmpiricalPayloadPurity tests requirement 2:
// Empirically verify that StoreExtraction stores tenant_id, app_id, subject_class, and document_title
// and NEVER stores "json_payload".
func TestChallenger2_EmpiricalPayloadPurity(t *testing.T) {
	t.Run("StoreExtraction stores required metadata fields in CacheRecord", func(t *testing.T) {
		var capturedRecord domain.CacheRecord
		var capturedCollection string

		storeMock := &mockVectorStore{
			upsertFn: func(ctx context.Context, collectionName string, record domain.CacheRecord) error {
				capturedCollection = collectionName
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

		identity := &pb.InfrastructureIdentity{
			TenantId: "tenant_emp_123",
			AppId:    "app_emp_456",
			JobId:    "job_emp_789",
		}
		inputMeta := map[string]any{
			"custom_meta": "custom_val",
		}

		val, _ := structpb.NewValue("Sample extracted document content")
		anyPayload, _ := anypb.New(val)

		err := app.StoreExtraction(
			context.Background(),
			"test_emp_collection",
			identity,
			inputMeta,
			"Closing Disclosure Empirical",
			"CLOSING_EMPIRICAL",
			anyPayload,
		)

		if err != nil {
			t.Fatalf("StoreExtraction returned unexpected error: %v", err)
		}

		if capturedCollection != "test_emp_collection" {
			t.Errorf("Expected collection 'test_emp_collection', got '%s'", capturedCollection)
		}

		if capturedRecord.Identity == nil {
			t.Fatalf("Expected non-nil Identity in CacheRecord")
		}
		if capturedRecord.Identity.TenantId != "tenant_emp_123" {
			t.Errorf("Expected TenantId 'tenant_emp_123', got '%s'", capturedRecord.Identity.TenantId)
		}
		if capturedRecord.Identity.AppId != "app_emp_456" {
			t.Errorf("Expected AppId 'app_emp_456', got '%s'", capturedRecord.Identity.AppId)
		}

		if capturedRecord.Metadata["document_title"] != "Closing Disclosure Empirical" {
			t.Errorf("Expected document_title 'Closing Disclosure Empirical', got '%v'", capturedRecord.Metadata["document_title"])
		}
		if capturedRecord.Metadata["subject_class"] != "CLOSING_EMPIRICAL" {
			t.Errorf("Expected subject_class 'CLOSING_EMPIRICAL', got '%v'", capturedRecord.Metadata["subject_class"])
		}

		if _, exists := capturedRecord.Metadata["json_payload"]; exists {
			t.Errorf("VIOLATION: CacheRecord.Metadata contains 'json_payload'")
		}
	})

	t.Run("StoreExtraction with explicit json_payload in input metadata", func(t *testing.T) {
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

		identity := &pb.InfrastructureIdentity{
			TenantId: "tenant_emp_123",
			AppId:    "app_emp_456",
			JobId:    "job_emp_789",
		}
		inputMetaWithJsonPayload := map[string]any{
			"json_payload": `{"large_nested_data": "must_not_be_stored"}`,
			"custom_meta":  "custom_val",
		}

		val, _ := structpb.NewValue("Sample text")
		anyPayload, _ := anypb.New(val)

		err := app.StoreExtraction(
			context.Background(),
			"test_emp_collection",
			identity,
			inputMetaWithJsonPayload,
			"Title Policy Empirical",
			"TITLE_POLICY",
			anyPayload,
		)

		if err != nil {
			t.Fatalf("StoreExtraction returned unexpected error: %v", err)
		}

		if val, exists := capturedRecord.Metadata["json_payload"]; exists {
			t.Errorf("SECURITY/PURITY VIOLATION: StoreExtraction stored 'json_payload' in metadata when passed in input metadata! Value: %v", val)
		}
	})
}

// TestChallenger2_EmpiricalCheckCacheReturnMetadata tests requirement 3:
// Empirically verify that CheckCache returns document_title and subject_class upon hit.
func TestChallenger2_EmpiricalCheckCacheReturnMetadata(t *testing.T) {
	identity := &pb.InfrastructureIdentity{
		TenantId: "tenant_check_123",
		AppId:    "app_check_456",
		JobId:    "job_check_789",
	}

	t.Run("Stage 1 Hit (Exact Metadata Match) returns document_title and subject_class", func(t *testing.T) {
		storeMock := &mockVectorStore{
			getByMetadataFn: func(ctx context.Context, collectionName string, id *pb.InfrastructureIdentity, metadata map[string]any) ([]domain.SearchResult, error) {
				return []domain.SearchResult{
					{
						Score: 1.0,
						Record: domain.CacheRecord{
							ID:       "uuid-1234",
							Identity: id,
							Metadata: map[string]any{
								"document_title": "Closing Disclosure Final",
								"subject_class":  "CLOSING_CLASS",
							},
						},
					},
				}, nil
			},
		}
		embedMock := &mockEmbeddingService{}
		app := application.NewSemanticCacheApp(embedMock, storeMock)

		metaStruct, _ := structpb.NewStruct(map[string]any{"filename": "closing.pdf"})
		contract := &pb.CacheContract{
			CollectionName: "coll_check",
			Confidence:     0.85,
			MetadataLookup: metaStruct,
		}

		hit, docTitle, subjClass, conf, err := app.CheckCache(context.Background(), contract, identity, nil)
		if err != nil {
			t.Fatalf("CheckCache returned error: %v", err)
		}

		if !hit {
			t.Errorf("Expected hit=true")
		}
		if docTitle != "Closing Disclosure Final" {
			t.Errorf("Expected docTitle='Closing Disclosure Final', got '%s'", docTitle)
		}
		if subjClass != "CLOSING_CLASS" {
			t.Errorf("Expected subjClass='CLOSING_CLASS', got '%s'", subjClass)
		}
		if conf != 1.0 {
			t.Errorf("Expected confidence=1.0, got %f", conf)
		}
	})

	t.Run("Stage 2 Hit (Semantic Embedding Search) returns document_title and subject_class", func(t *testing.T) {
		storeMock := &mockVectorStore{
			searchFn: func(ctx context.Context, collectionName string, vector []float32, id *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
				return []domain.SearchResult{
					{
						Score: 0.92,
						Record: domain.CacheRecord{
							ID:       "uuid-5678",
							Identity: id,
							Metadata: map[string]any{
								"document_title": "Appraisal Report",
								"subject_class":  "APPRAISAL_CLASS",
							},
						},
					},
				}, nil
			},
		}
		embedMock := &mockEmbeddingService{
			generateFn: func(ctx context.Context, text string) ([]float32, error) {
				return []float32{0.1, 0.2, 0.3}, nil
			},
		}
		app := application.NewSemanticCacheApp(embedMock, storeMock)

		val, _ := structpb.NewValue("Search text query")
		anyVec, _ := anypb.New(val)

		contract := &pb.CacheContract{
			CollectionName:  "coll_check",
			Confidence:      0.85,
			EmbeddingLookup: anyVec,
		}

		hit, docTitle, subjClass, conf, err := app.CheckCache(context.Background(), contract, identity, nil)
		if err != nil {
			t.Fatalf("CheckCache returned error: %v", err)
		}

		if !hit {
			t.Errorf("Expected hit=true")
		}
		if docTitle != "Appraisal Report" {
			t.Errorf("Expected docTitle='Appraisal Report', got '%s'", docTitle)
		}
		if subjClass != "APPRAISAL_CLASS" {
			t.Errorf("Expected subjClass='APPRAISAL_CLASS', got '%s'", subjClass)
		}
		if conf != 0.92 {
			t.Errorf("Expected confidence=0.92, got %f", conf)
		}
	})
}

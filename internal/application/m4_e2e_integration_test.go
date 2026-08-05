package application_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
)

// TestM4_SHA256RecordID_Calculation verifies the deterministic SHA256 ID formula:
// SHA256(cleanedText + tenantId + appId) without job_id dependency.
func TestM4_SHA256RecordID_Calculation(t *testing.T) {
	tenantID := "tenant_acme"
	appID := "app_artemis"
	cleanedText := "Closing Disclosure template text without PII"

	identityJob1 := &pb.InfrastructureIdentity{
		TenantId: tenantID,
		AppId:    appID,
		JobId:    "job_001_alpha",
	}

	identityJob2 := &pb.InfrastructureIdentity{
		TenantId: tenantID,
		AppId:    appID,
		JobId:    "job_999_omega",
	}

	id1, err1 := application.GenerateRecordID(cleanedText, identityJob1)
	if err1 != nil {
		t.Fatalf("GenerateRecordID failed for job1: %v", err1)
	}

	id2, err2 := application.GenerateRecordID(cleanedText, identityJob2)
	if err2 != nil {
		t.Fatalf("GenerateRecordID failed for job2: %v", err2)
	}

	// 1. Verify JobId variation does not affect generated ID
	if id1 != id2 {
		t.Errorf("Expected identical record IDs across different jobs, got %s vs %s", id1, id2)
	}

	// 2. Authoritative derivation: SHA256(len:val format)
	expectedInput := fmt.Sprintf("%d:%s|%d:%s|%d:%s", len(tenantID), tenantID, len(appID), appID, len(cleanedText), cleanedText)
	expectedSum := sha256.Sum256([]byte(expectedInput))
	expectedHex := hex.EncodeToString(expectedSum[:])

	if id1 != expectedHex {
		t.Errorf("Record ID derivation mismatch: expected %s, got %s", expectedHex, id1)
	}

	// 3. Changing tenantId MUST produce a distinct ID
	identityDiffTenant := &pb.InfrastructureIdentity{
		TenantId: "tenant_stark",
		AppId:    appID,
		JobId:    "job_001_alpha",
	}
	idDiffTenant, _ := application.GenerateRecordID(cleanedText, identityDiffTenant)
	if id1 == idDiffTenant {
		t.Errorf("Expected different record IDs for different tenants, got match %s", id1)
	}
}

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

	identity := &pb.InfrastructureIdentity{
		TenantId: "tenant_vault",
		AppId:    "app_secure",
		JobId:    "job_to_be_purged",
	}

	inputMetadata := map[string]any{
		"json_payload":   `{"pii_name":"John Doe","sensitive":"12345"}`,
		"job_id":         "job_to_be_purged",
		"custom_setting": "client_opt_in",
	}

	docTitle := "Closing Disclosure"
	subjClass := "CLOSING"

	err := app.StoreExtraction(
		context.Background(),
		"generic_cache_coll",
		identity,
		inputMetadata,
		docTitle,
		subjClass,
		nil,
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
	if capturedRecord.Metadata["document_title"] != docTitle {
		t.Errorf("Expected document_title '%s', got %v", docTitle, capturedRecord.Metadata["document_title"])
	}
	if capturedRecord.Metadata["subject_class"] != subjClass {
		t.Errorf("Expected subject_class '%s', got %v", subjClass, capturedRecord.Metadata["subject_class"])
	}

	// Assert forbidden fields are purged
	if _, exists := capturedRecord.Metadata["json_payload"]; exists {
		t.Errorf("Forbidden field 'json_payload' was NOT purged from metadata payload!")
	}
	if _, exists := capturedRecord.Metadata["job_id"]; exists {
		t.Errorf("Forbidden field 'job_id' was NOT purged from metadata payload!")
	}

	// Assert custom allowed fields persist
	if capturedRecord.Metadata["custom_setting"] != "client_opt_in" {
		t.Errorf("Expected custom_setting 'client_opt_in', got %v", capturedRecord.Metadata["custom_setting"])
	}
}

// TestM4_CheckCache_ReturnsDocumentTitleOnHit verifies CheckCache returns
// document_title, subject_class, and confidence score on cache hit.
func TestM4_CheckCache_ReturnsDocumentTitleOnHit(t *testing.T) {
	expectedTitle := "Title Commitment"
	expectedSubject := "TITLE_OPINION"

	storeMock := &mockVectorStore{
		searchFn: func(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
			return []domain.SearchResult{
				{
					Score: 0.98,
					Record: domain.CacheRecord{
						Metadata: map[string]any{
							"document_title": expectedTitle,
							"subject_class":  expectedSubject,
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

	contract := &pb.CacheContract{
		CollectionName: "title_opinion_templates",
		Confidence:     0.90,
	}
	identity := &pb.InfrastructureIdentity{
		TenantId: "tenant_title_co",
		AppId:    "app_title_proc",
		JobId:    "job_test_hit",
	}

	hit, docTitle, subjClass, conf, err := app.CheckCache(context.Background(), contract, identity, nil)
	if err != nil {
		t.Fatalf("CheckCache failed: %v", err)
	}

	if !hit {
		t.Fatalf("Expected cache hit=true, got false")
	}
	if docTitle != expectedTitle {
		t.Errorf("Expected docTitle '%s', got '%s'", expectedTitle, docTitle)
	}
	if subjClass != expectedSubject {
		t.Errorf("Expected subjClass '%s', got '%s'", expectedSubject, subjClass)
	}
	if conf != 0.98 {
		t.Errorf("Expected confidence 0.98, got %f", conf)
	}
}

// TestM4_IdentityFailFast_Validation tests fail-fast behavior when identity
// is missing or incomplete.
func TestM4_IdentityFailFast_Validation(t *testing.T) {
	app := application.NewSemanticCacheApp(&mockEmbeddingService{}, &mockVectorStore{})

	contract := &pb.CacheContract{CollectionName: "test_collection"}

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
			_, _, _, _, _ = app.CheckCache(context.Background(), contract, id, nil)
		})
	}
}

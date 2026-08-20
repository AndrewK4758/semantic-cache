package application_test

import (
	"context"
	"errors"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// --- Mocks ---

type mockEmbeddingService struct {
	generateFn func(ctx context.Context, text string) ([]float32, error)
}

func (m *mockEmbeddingService) Generate(ctx context.Context, text string) ([]float32, error) {
	if m.generateFn != nil {
		return m.generateFn(ctx, text)
	}
	return []float32{0.1, 0.2, 0.3}, nil
}

type mockVectorStore struct {
	searchFn        func(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error)
	upsertFn        func(ctx context.Context, collectionName string, record domain.CacheRecord) error
	checkMetadataFn func(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) (bool, error)
	getByMetadataFn func(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) ([]domain.SearchResult, error)
}

func (m *mockVectorStore) Search(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
	if m.searchFn != nil {
		return m.searchFn(ctx, collectionName, vector, identity, metadata, limit)
	}
	return nil, nil
}

func (m *mockVectorStore) Upsert(ctx context.Context, collectionName string, record domain.CacheRecord) error {
	if m.upsertFn != nil {
		return m.upsertFn(ctx, collectionName, record)
	}
	return nil
}

func (m *mockVectorStore) CheckMetadata(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) (bool, error) {
	if m.checkMetadataFn != nil {
		return m.checkMetadataFn(ctx, collectionName, identity, metadata)
	}
	return false, nil
}

func (m *mockVectorStore) GetByMetadata(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) ([]domain.SearchResult, error) {
	if m.getByMetadataFn != nil {
		return m.getByMetadataFn(ctx, collectionName, identity, metadata)
	}
	return nil, nil
}

func (m *mockVectorStore) EnsureCollection(ctx context.Context, collectionName string, denseDim uint64, pqCompression uint32) error {
	return nil
}

func (m *mockVectorStore) UpsertHybrid(ctx context.Context, collectionName string, record domain.CacheRecord) error {
	return nil
}

func (m *mockVectorStore) HybridSearch(ctx context.Context, collectionName string, denseVector []float32, sparseVector *domain.SparseVector, identity *pb.InfrastructureIdentity, metadata map[string]any, alpha float32, limit int) ([]domain.SearchResult, error) {
	return []domain.SearchResult{
		{
			Record:      domain.CacheRecord{JSONPayload: `{"hybrid_hit": true}`},
			Score:       0.95,
			DenseScore:  0.96,
			SparseScore: 0.92,
		},
	}, nil
}


// --- Tests ---

func TestSemanticCacheApp_CheckCache(t *testing.T) {
	type args struct {
		text      string
		metadata  map[string]any
		threshold float32
	}
	tests := []struct {
		name               string
		embedFn            func(ctx context.Context, text string) ([]float32, error)
		searchFn           func(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error)
		getByMetadataFn    func(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) ([]domain.SearchResult, error)
		args               args
		expectedHit        bool
		expectedPayload    string
		expectedConfidence float32
		expectError        bool
	}{
		{
			name: "Tier 1: Cache Hit (Exact Metadata Match)",
			getByMetadataFn: func(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) ([]domain.SearchResult, error) {
				return []domain.SearchResult{
					{
						Score: 1.0,
						Record: domain.CacheRecord{
							JSONPayload: `{"status":"exact_match"}`,
						},
					},
				}, nil
			},
			embedFn: func(ctx context.Context, text string) ([]float32, error) {
				return nil, errors.New("embedder should NOT be called for exact metadata match")
			},
			args: args{
				text:      "some text",
				metadata:  map[string]any{"filename": "test.pdf", "subject_class": "test_subject"},
				threshold: 0.90,
			},
			expectedHit:        true,
			expectedPayload:    `{"status":"exact_match"}`,
			expectedConfidence: 1.0,
			expectError:        false,
		},
		{
			name: "Tier 1: Resiliency - Qdrant Metadata Error Fallback",
			getByMetadataFn: func(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) ([]domain.SearchResult, error) {
				return nil, errors.New("qdrant scroll offline")
			},
			searchFn: func(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
				return []domain.SearchResult{
					{
						Score: 0.98,
						Record: domain.CacheRecord{
							JSONPayload: `{"status":"fallback_hit"}`,
						},
					},
				}, nil
			},
			args: args{
				text:      "sample invoice text",
				metadata:  map[string]any{"filename": "test.pdf"},
				threshold: 0.90,
			},
			expectedHit:        true,
			expectedPayload:    `{"status":"fallback_hit"}`,
			expectedConfidence: 0.98,
			expectError:        false,
		},
		{
			name: "Cache Hit - Score exceeds threshold",
			searchFn: func(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
				return []domain.SearchResult{
					{
						Score: 0.98,
						Record: domain.CacheRecord{
							JSONPayload: `{"status":"ok"}`,
						},
					},
				}, nil
			},
			args: args{
				text:      "sample invoice text",
				metadata:  map[string]any{"document_type": "invoice", "TemplateHash": "hash"},
				threshold: 0.90,
			},
			expectedHit:        true,
			expectedPayload:    `{"status":"ok"}`,
			expectedConfidence: 0.98,
			expectError:        false,
		},
		{
			name: "Cache Miss - Score below threshold",
			searchFn: func(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
				return []domain.SearchResult{
					{
						Score: 0.85,
						Record: domain.CacheRecord{
							JSONPayload: `{"status":"ok"}`,
						},
					},
				}, nil
			},
			args: args{
				text:      "sample text",
				metadata:  map[string]any{"document_type": "invoice"},
				threshold: 0.90,
			},
			expectedHit:        false,
			expectedPayload:    "",
			expectedConfidence: 0,
			expectError:        false,
		},
		{
			name: "Success - Cache Miss (No Results)",
			searchFn: func(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
				return []domain.SearchResult{}, nil
			},
			args: args{
				text:      "sample text",
				metadata:  map[string]any{"document_type": "invoice"},
				threshold: 0.90,
			},
			expectedHit:        false,
			expectedPayload:    "",
			expectedConfidence: 0,
			expectError:        false,
		},
		{
			name: "Error - Embedding failure",
			embedFn: func(ctx context.Context, text string) ([]float32, error) {
				return nil, errors.New("openai offline")
			},
			args: args{
				text:      "sample invoice text",
				metadata:  map[string]any{"document_type": "invoice"},
				threshold: 0.8,
			},
			expectError: true,
		},
		{
			name: "Error - Store search failure",
			searchFn: func(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
				return nil, errors.New("qdrant offline")
			},
			args: args{
				text:      "sample text",
				metadata:  map[string]any{"document_type": "invoice"},
				threshold: 0.90,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			embedMock := &mockEmbeddingService{generateFn: tt.embedFn}
			storeMock := &mockVectorStore{searchFn: tt.searchFn, getByMetadataFn: tt.getByMetadataFn}
			app := application.NewSemanticCacheApp(embedMock, storeMock)

			var metaStruct *structpb.Struct
			if tt.args.metadata != nil {
				var err error
				metaStruct, err = structpb.NewStruct(tt.args.metadata)
				if err != nil {
					t.Fatalf("failed to create structpb: %v", err)
				}
			}

			contract := &pb.CacheContract{
				CollectionName: "test_collection",
				Confidence:     tt.args.threshold,
				MetadataLookup: metaStruct,
			}
			identity := &pb.InfrastructureIdentity{
				TenantId: "tenant_123",
				AppId:    "app_456",
				JobId:    "job_789",
			}

			hit, payload, conf, err := app.CheckCache(context.Background(), contract, identity, nil)

			if (err != nil) != tt.expectError {
				t.Fatalf("expected error: %v, got: %v", tt.expectError, err)
			}

			if hit != tt.expectedHit {
				t.Errorf("expected hit: %v, got: %v", tt.expectedHit, hit)
			}

			if payload != tt.expectedPayload {
				t.Errorf("expected payload: %s, got: %s", tt.expectedPayload, payload)
			}

			if conf != tt.expectedConfidence {
				t.Errorf("expected confidence: %f, got: %f", tt.expectedConfidence, conf)
			}
		})
	}
}

func TestCheckCache_EmptyMetadataWithNonNilIdentity_SkipsStage1(t *testing.T) {
	embedCalled := false
	searchCalled := false

	embedMock := &mockEmbeddingService{
		generateFn: func(ctx context.Context, text string) ([]float32, error) {
			embedCalled = true
			return []float32{0.1, 0.2, 0.3}, nil
		},
	}

	storeMock := &mockVectorStore{
		getByMetadataFn: func(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) ([]domain.SearchResult, error) {
			t.Errorf("getByMetadataFn (Stage 1) should NOT be called when metadata lookup is empty")
			return []domain.SearchResult{
				{
					Score: 1.0,
					Record: domain.CacheRecord{
						JSONPayload: `{"doc": "wrong_unrelated_doc"}`,
					},
				},
			}, nil
		},
		searchFn: func(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
			searchCalled = true
			return []domain.SearchResult{
				{
					Score: 0.98,
					Record: domain.CacheRecord{
						JSONPayload: `{"doc": "correct_semantic_doc"}`,
					},
				},
			}, nil
		},
	}

	app := application.NewSemanticCacheApp(embedMock, storeMock)

	contract := &pb.CacheContract{
		CollectionName: "test_collection",
		Confidence:     0.90,
		MetadataLookup: nil,
	}
	identity := &pb.InfrastructureIdentity{
		TenantId: "tenant_123",
		AppId:    "app_456",
		JobId:    "job_789",
	}

	hit, payload, conf, err := app.CheckCache(context.Background(), contract, identity, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !hit {
		t.Errorf("expected hit to be true, got false")
	}

	expectedPayload := `{"doc": "correct_semantic_doc"}`
	if payload != expectedPayload {
		t.Errorf("expected payload %s, got %s", expectedPayload, payload)
	}

	if conf != 0.98 {
		t.Errorf("expected confidence 0.98, got %f", conf)
	}

	if !embedCalled {
		t.Errorf("expected Stage 2 embedding generation to be called")
	}

	if !searchCalled {
		t.Errorf("expected Stage 2 vector search to be called")
	}
}

func TestSemanticCacheApp_StoreExtraction(t *testing.T) {
	type args struct {
		text             string
		metadata         map[string]any
		extractedPayload string
	}
	tests := []struct {
		name        string
		embedFn     func(ctx context.Context, text string) ([]float32, error)
		upsertFn    func(ctx context.Context, collectionName string, record domain.CacheRecord) error
		args        args
		expectError bool
	}{
		{
			name: "Success",
			args: args{
				text:             "sample invoice text",
				metadata:         map[string]any{"document_type": "invoice", "subject_class": "INVOICE", "document_title": "Invoice 123", "TemplateHash": "hash"},
				extractedPayload: `{"amount": 100}`,
			},
			expectError: false,
		},
		{
			name: "Error - Embedding failure",
			embedFn: func(ctx context.Context, text string) ([]float32, error) {
				return nil, errors.New("openai offline")
			},
			args: args{
				text:             "sample invoice text",
				metadata:         map[string]any{"document_type": "invoice", "subject_class": "INVOICE", "document_title": "Invoice 123", "TemplateHash": "hash"},
				extractedPayload: `{"amount": 100}`,
			},
			expectError: true,
		},
		{
			name: "Error - Upsert failure",
			upsertFn: func(ctx context.Context, collectionName string, record domain.CacheRecord) error {
				return errors.New("qdrant offline")
			},
			args: args{
				text:             "sample invoice text",
				metadata:         map[string]any{"document_type": "invoice", "subject_class": "INVOICE", "document_title": "Invoice 123", "TemplateHash": "hash"},
				extractedPayload: `{"amount": 100}`,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			embedMock := &mockEmbeddingService{generateFn: tt.embedFn}
			storeMock := &mockVectorStore{upsertFn: tt.upsertFn}
			app := application.NewSemanticCacheApp(embedMock, storeMock)

			val, _ := structpb.NewValue(tt.args.text)
			anyPayload, _ := anypb.New(val)

			identity := &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"}
			err := app.StoreExtraction(context.Background(), "test_collection", identity, tt.args.metadata, tt.args.extractedPayload, anyPayload)

			if (err != nil) != tt.expectError {
				t.Fatalf("expected error: %v, got: %v", tt.expectError, err)
			}
		})
	}
}

func TestSemanticCacheApp_CheckMetadata(t *testing.T) {
	type args struct {
		collectionName string
		metadata       map[string]any
	}
	tests := []struct {
		name            string
		checkMetadataFn func(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) (bool, error)
		args            args
		expectedExists  bool
		expectError     bool
	}{
		{
			name: "Success - Metadata Exists",
			checkMetadataFn: func(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) (bool, error) {
				return true, nil
			},
			args: args{
				collectionName: "test_collection",
				metadata:       map[string]any{"TemplateHash": "testhash123"},
			},
			expectedExists: true,
			expectError:    false,
		},
		{
			name: "Success - Metadata Does Not Exist",
			checkMetadataFn: func(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) (bool, error) {
				return false, nil
			},
			args: args{
				collectionName: "test_collection",
				metadata:       map[string]any{"TemplateHash": "unknownhash"},
			},
			expectedExists: false,
			expectError:    false,
		},
		{
			name: "Error - Vector Store Offline",
			checkMetadataFn: func(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) (bool, error) {
				return false, errors.New("qdrant offline")
			},
			args: args{
				collectionName: "test_collection",
				metadata:       map[string]any{"TemplateHash": "testhash123"},
			},
			expectedExists: false,
			expectError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storeMock := &mockVectorStore{checkMetadataFn: tt.checkMetadataFn}
			app := application.NewSemanticCacheApp(&mockEmbeddingService{}, storeMock)

			exists, err := app.CheckMetadata(context.Background(), tt.args.collectionName, nil, tt.args.metadata)

			if (err != nil) != tt.expectError {
				t.Fatalf("expected error: %v, got: %v", tt.expectError, err)
			}

			if exists != tt.expectedExists {
				t.Errorf("expected exists: %v, got: %v", tt.expectedExists, exists)
			}
		})
	}
}

func TestSemanticCacheApp_CheckCache_NilContract(t *testing.T) {
	embedMock := &mockEmbeddingService{}
	storeMock := &mockVectorStore{}
	app := application.NewSemanticCacheApp(embedMock, storeMock)

	identity := &pb.InfrastructureIdentity{
		TenantId: "tenant_1",
		AppId:    "app_1",
		JobId:    "job_1",
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when contract is nil, got none")
		}
	}()
	app.CheckCache(context.Background(), nil, identity, nil)
}

func TestSemanticCacheApp_CheckCache_PrecomputedVector(t *testing.T) {
	embedCalled := false
	embedMock := &mockEmbeddingService{
		generateFn: func(ctx context.Context, text string) ([]float32, error) {
			embedCalled = true
			return nil, errors.New("should not generate embedding when pre-computed vector is provided")
		},
	}

	var searchedVector []float32
	storeMock := &mockVectorStore{
		searchFn: func(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
			searchedVector = vector
			return []domain.SearchResult{
				{
					Score:  0.98,
					Record: domain.CacheRecord{JSONPayload: `{"precomputed":true}`},
				},
			}, nil
		},
	}

	app := application.NewSemanticCacheApp(embedMock, storeMock)

	listVal, _ := structpb.NewList([]any{0.5, 0.6, 0.7})
	anyVec, _ := anypb.New(listVal)

	contract := &pb.CacheContract{
		CollectionName:  "test_coll",
		Confidence:      0.90,
		EmbeddingLookup: anyVec,
	}
	identity := &pb.InfrastructureIdentity{
		TenantId: "t1",
		AppId:    "a1",
		JobId:    "j1",
	}

	hit, payload, conf, err := app.CheckCache(context.Background(), contract, identity, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if embedCalled {
		t.Fatalf("embedder should not have been called when pre-computed vector is passed")
	}
	if !hit || payload != `{"precomputed":true}` || conf != 0.98 {
		t.Fatalf("unexpected result: hit=%v, payload=%s, conf=%f", hit, payload, conf)
	}
	if len(searchedVector) != 3 || searchedVector[0] != 0.5 {
		t.Fatalf("expected searched vector [0.5, 0.6, 0.7], got %v", searchedVector)
	}
}

func TestSemanticCacheApp_StoreExtraction_PrecomputedVector(t *testing.T) {
	embedCalled := false
	embedMock := &mockEmbeddingService{
		generateFn: func(ctx context.Context, text string) ([]float32, error) {
			embedCalled = true
			return nil, errors.New("should not generate embedding")
		},
	}

	var storedVec []float32
	storeMock := &mockVectorStore{
		upsertFn: func(ctx context.Context, collectionName string, record domain.CacheRecord) error {
			storedVec = record.Vector
			return nil
		},
	}

	app := application.NewSemanticCacheApp(embedMock, storeMock)

	listVal, _ := structpb.NewList([]any{0.11, 0.22, 0.33})
	anyVec, _ := anypb.New(listVal)

	identity := &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"}

	err := app.StoreExtraction(context.Background(), "test_coll", identity, map[string]any{"subject_class": "CLASS", "document_title": "Title", "TemplateHash": "hash"}, `{"data":1}`, anyVec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if embedCalled {
		t.Fatalf("embedder should not have been called when pre-computed vector is passed")
	}
	if len(storedVec) != 3 || storedVec[0] != float32(0.11) {
		t.Fatalf("expected stored vector [0.11, 0.22, 0.33], got %v", storedVec)
	}
}

func TestSemanticCacheApp_GenerateSparseEmbedding(t *testing.T) {
	app := application.NewSemanticCacheApp(&mockEmbeddingService{}, &mockVectorStore{})
	identity := &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"}

	resp, err := app.GenerateSparseEmbedding(context.Background(), &pb.GenerateSparseEmbeddingRequest{
		Identity: identity,
		Text:     "Closing disclosure for borrower John Doe",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.UniqueTerms == 0 || len(resp.SparseVector.Indices) == 0 {
		t.Fatalf("expected non-empty sparse vector")
	}
}

func TestSemanticCacheApp_HybridCheckCache_And_Store(t *testing.T) {
	app := application.NewSemanticCacheApp(&mockEmbeddingService{}, &mockVectorStore{})
	identity := &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"}

	storeResp, err := app.HybridStoreExtraction(context.Background(), &pb.HybridStoreExtractionRequest{
		CollectionName:   "hybrid_coll",
		Identity:         identity,
		DocumentType:     "ClosingDisclosure",
		SectionTitle:     "Terms",
		ExtractedPayload: `{"approved": true}`,
	})
	if err != nil {
		t.Fatalf("unexpected error in HybridStoreExtraction: %v", err)
	}
	if !storeResp.Success {
		t.Fatalf("expected success=true")
	}

	checkResp, err := app.HybridCheckCache(context.Background(), &pb.HybridCheckCacheRequest{
		CollectionName:      "hybrid_coll",
		Identity:            identity,
		QueryText:           "Closing terms",
		ConfidenceThreshold: 0.8,
		Alpha:               0.7,
	})
	if err != nil {
		t.Fatalf("unexpected error in HybridCheckCache: %v", err)
	}
	if !checkResp.Hit {
		t.Fatalf("expected hit=true")
	}
	if checkResp.CombinedScore <= 0 {
		t.Fatalf("expected positive CombinedScore, got %f", checkResp.CombinedScore)
	}
}


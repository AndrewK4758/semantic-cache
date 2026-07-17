package application_test

import (
	"context"
	"errors"
	"testing"

	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
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
	searchFn        func(ctx context.Context, collectionName string, vector []float32, metadata map[string]any, limit int) ([]domain.SearchResult, error)
	upsertFn        func(ctx context.Context, collectionName string, record domain.CacheRecord) error
	checkMetadataFn func(ctx context.Context, collectionName string, metadata map[string]any) (bool, error)
	getByMetadataFn func(ctx context.Context, collectionName string, metadata map[string]any) ([]domain.SearchResult, error)
}

func (m *mockVectorStore) Search(ctx context.Context, collectionName string, vector []float32, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
	if m.searchFn != nil {
		return m.searchFn(ctx, collectionName, vector, metadata, limit)
	}
	return nil, nil
}

func (m *mockVectorStore) Upsert(ctx context.Context, collectionName string, record domain.CacheRecord) error {
	if m.upsertFn != nil {
		return m.upsertFn(ctx, collectionName, record)
	}
	return nil
}

func (m *mockVectorStore) CheckMetadata(ctx context.Context, collectionName string, metadata map[string]any) (bool, error) {
	if m.checkMetadataFn != nil {
		return m.checkMetadataFn(ctx, collectionName, metadata)
	}
	return false, nil
}

func (m *mockVectorStore) GetByMetadata(ctx context.Context, collectionName string, metadata map[string]any) ([]domain.SearchResult, error) {
	if m.getByMetadataFn != nil {
		return m.getByMetadataFn(ctx, collectionName, metadata)
	}
	return nil, nil
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
		searchFn           func(ctx context.Context, collectionName string, vector []float32, metadata map[string]any, limit int) ([]domain.SearchResult, error)
		getByMetadataFn    func(ctx context.Context, collectionName string, metadata map[string]any) ([]domain.SearchResult, error)
		args               args
		expectedHit        bool
		expectedPayload    string
		expectedConfidence float32
		expectError        bool
	}{
		{
			name: "Tier 1: Cache Hit (Exact Metadata Match)",
			getByMetadataFn: func(ctx context.Context, collectionName string, metadata map[string]any) ([]domain.SearchResult, error) {
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
				metadata:  map[string]any{"filename": "test.pdf"},
				threshold: 0.90,
			},
			expectedHit:        true,
			expectedPayload:    `{"status":"exact_match"}`,
			expectedConfidence: 1.0,
			expectError:        false,
		},
		{
			name: "Tier 1: Resiliency - Qdrant Metadata Error Fallback",
			getByMetadataFn: func(ctx context.Context, collectionName string, metadata map[string]any) ([]domain.SearchResult, error) {
				return nil, errors.New("qdrant scroll offline")
			},
			searchFn: func(ctx context.Context, collectionName string, vector []float32, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
				return []domain.SearchResult{
					{
						Score: 0.95,
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
			expectedConfidence: 0.95,
			expectError:        false,
		},
		{
			name: "Cache Hit - Score exceeds threshold",
			searchFn: func(ctx context.Context, collectionName string, vector []float32, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
				return []domain.SearchResult{
					{
						Score: 0.95,
						Record: domain.CacheRecord{
							JSONPayload: `{"status":"ok"}`,
						},
					},
				}, nil
			},
			args: args{
				text:      "sample invoice text",
				metadata:  map[string]any{"document_type": "invoice"},
				threshold: 0.90,
			},
			expectedHit:        true,
			expectedPayload:    `{"status":"ok"}`,
			expectedConfidence: 0.95,
			expectError:        false,
		},
		{
			name: "Cache Miss - Score below threshold",
			searchFn: func(ctx context.Context, collectionName string, vector []float32, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
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
			searchFn: func(ctx context.Context, collectionName string, vector []float32, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
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
			searchFn: func(ctx context.Context, collectionName string, vector []float32, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
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

			hit, payload, conf, err := app.CheckCache(context.Background(), "test_collection", tt.args.text, tt.args.metadata, tt.args.threshold)

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
				metadata:         map[string]any{"document_type": "invoice"},
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
				metadata:         map[string]any{"document_type": "invoice"},
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
				metadata:         map[string]any{"document_type": "invoice"},
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
			err := app.StoreExtraction(context.Background(), "test_collection", tt.args.text, tt.args.metadata, tt.args.extractedPayload)

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
		checkMetadataFn func(ctx context.Context, collectionName string, metadata map[string]any) (bool, error)
		args            args
		expectedExists  bool
		expectError     bool
	}{
		{
			name: "Success - Metadata Exists",
			checkMetadataFn: func(ctx context.Context, collectionName string, metadata map[string]any) (bool, error) {
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
			checkMetadataFn: func(ctx context.Context, collectionName string, metadata map[string]any) (bool, error) {
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
			checkMetadataFn: func(ctx context.Context, collectionName string, metadata map[string]any) (bool, error) {
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

			exists, err := app.CheckMetadata(context.Background(), tt.args.collectionName, tt.args.metadata)

			if (err != nil) != tt.expectError {
				t.Fatalf("expected error: %v, got: %v", tt.expectError, err)
			}

			if exists != tt.expectedExists {
				t.Errorf("expected exists: %v, got: %v", tt.expectedExists, exists)
			}
		})
	}
}

package application_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
	"google.golang.org/protobuf/types/known/structpb"
)

// Thread-safe mock vector store for stress testing
type concurrentMockVectorStore struct {
	mu          sync.RWMutex
	recordsByID map[string]domain.CacheRecord
	upsertCount int64
	searchCount int64
}

func newConcurrentMockVectorStore() *concurrentMockVectorStore {
	return &concurrentMockVectorStore{
		recordsByID: make(map[string]domain.CacheRecord),
	}
}

func (m *concurrentMockVectorStore) Search(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
	atomic.AddInt64(&m.searchCount, 1)
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []domain.SearchResult
	for _, record := range m.recordsByID {
		// Strict multi-tenant filtering check
		if record.Identity != nil && identity != nil {
			if record.Identity.TenantId == identity.TenantId && record.Identity.AppId == identity.AppId {
				results = append(results, domain.SearchResult{
					Score:  0.95,
					Record: record,
				})
			}
		}
		if len(results) >= limit {
			break
		}
	}
	return results, nil
}

func (m *concurrentMockVectorStore) Upsert(ctx context.Context, collectionName string, record domain.CacheRecord) error {
	atomic.AddInt64(&m.upsertCount, 1)
	m.mu.Lock()
	defer m.mu.Unlock()

	m.recordsByID[record.ID] = record
	return nil
}

func (m *concurrentMockVectorStore) CheckMetadata(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.recordsByID) > 0, nil
}

func (m *concurrentMockVectorStore) GetByMetadata(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) ([]domain.SearchResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []domain.SearchResult
	for _, record := range m.recordsByID {
		if record.Identity != nil && identity != nil {
			if record.Identity.TenantId == identity.TenantId && record.Identity.AppId == identity.AppId {
				results = append(results, domain.SearchResult{
					Score:  1.0,
					Record: record,
				})
			}
		}
	}
	return results, nil
}

// TestM4Challenger_HighConcurrency_MultiTenant_Isolation stress tests the semantic_cache_service application layer
// under 100 concurrent workers across 10 distinct tenants and 5 apps.
func TestM4Challenger_HighConcurrency_MultiTenant_Isolation(t *testing.T) {
	storeMock := newConcurrentMockVectorStore()
	embedMock := &mockEmbeddingService{
		generateFn: func(ctx context.Context, text string) ([]float32, error) {
			return []float32{0.1, 0.2, 0.3, 0.4}, nil
		},
	}

	app := application.NewSemanticCacheApp(embedMock, storeMock)

	numWorkers := 100
	numTenants := 10
	numApps := 5

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	errChan := make(chan error, numWorkers*2)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Launch 100 concurrent workers storing and searching cache entries
	for i := 0; i < numWorkers; i++ {
		workerID := i
		go func() {
			defer wg.Done()

			tenantID := fmt.Sprintf("tenant_%d", workerID%numTenants)
			appID := fmt.Sprintf("app_%d", workerID%numApps)
			jobID := fmt.Sprintf("job_worker_%d", workerID)
			docTitle := fmt.Sprintf("Document Title for Tenant %s", tenantID)

			identity := &pb.InfrastructureIdentity{
				TenantId: tenantID,
				AppId:    appID,
				JobId:    jobID,
			}

			// Step 1: Store Extraction
			metaMap := map[string]any{
				"custom_ref": fmt.Sprintf("ref_%d", workerID),
			}
			err := app.StoreExtraction(ctx, "stress_collection", identity, metaMap, docTitle, "CLOSING", nil)
			if err != nil {
				errChan <- fmt.Errorf("worker %d StoreExtraction failed: %w", workerID, err)
				return
			}

			// Step 2: Check Cache (Exact Metadata Match or Vector Search)
			metaStruct, _ := structpb.NewStruct(metaMap)
			contract := &pb.CacheContract{
				CollectionName: "stress_collection",
				Confidence:     0.90,
				MetadataLookup: metaStruct,
			}

			hit, returnedTitle, _, _, err := app.CheckCache(ctx, contract, identity, nil)
			if err != nil {
				errChan <- fmt.Errorf("worker %d CheckCache failed: %w", workerID, err)
				return
			}
			if !hit {
				errChan <- fmt.Errorf("worker %d expected cache hit, got miss", workerID)
				return
			}
			if returnedTitle != docTitle {
				errChan <- fmt.Errorf("worker %d multi-tenant leak: expected title %s, got %s", workerID, docTitle, returnedTitle)
				return
			}
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("Stress test concurrency failure: %v", err)
	}

	if storeMock.upsertCount != int64(numWorkers) {
		t.Errorf("Expected %d upserts, got %d", numWorkers, storeMock.upsertCount)
	}
}

// TestM4Challenger_ConcurrentIdenticalText_DistinctRecordIDs verifies SHA256 ID length framing
// prevents collisions across distinct tenant/app combinations with identical text inputs.
func TestM4Challenger_ConcurrentIdenticalText_DistinctRecordIDs(t *testing.T) {
	cleanedText := "Standardized Closing Disclosure Document Template Content"

	numGoroutines := 50
	generatedIDs := sync.Map{}

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		tenantID := fmt.Sprintf("tenant_%d", i)
		appID := fmt.Sprintf("app_%d", i%5)
		identity := &pb.InfrastructureIdentity{
			TenantId: tenantID,
			AppId:    appID,
			JobId:    fmt.Sprintf("job_concurrent_%d", i),
		}

		go func() {
			defer wg.Done()
			id, err := application.GenerateRecordID(cleanedText, identity)
			if err != nil {
				t.Errorf("GenerateRecordID error: %v", err)
				return
			}
			if _, loaded := generatedIDs.LoadOrStore(id, tenantID); loaded {
				t.Errorf("COLLISION DETECTED! Record ID %s generated for multiple tenants!", id)
			}
		}()
	}

	wg.Wait()
}

// TestM4Challenger_PrefixAmbiguityCollisionCheck tests boundary framing (e.g. tenant="a", app="bc" vs tenant="ab", app="c").
func TestM4Challenger_PrefixAmbiguityCollisionCheck(t *testing.T) {
	text := "Same cleaned document text"

	identityA := &pb.InfrastructureIdentity{TenantId: "a", AppId: "bc", JobId: "j1"}
	identityB := &pb.InfrastructureIdentity{TenantId: "ab", AppId: "c", JobId: "j1"}

	idA, errA := application.GenerateRecordID(text, identityA)
	if errA != nil {
		t.Fatalf("GenerateRecordID A failed: %v", errA)
	}

	idB, errB := application.GenerateRecordID(text, identityB)
	if errB != nil {
		t.Fatalf("GenerateRecordID B failed: %v", errB)
	}

	if idA == idB {
		t.Fatalf("CRITICAL SECURITY DEFECT: Length framing failed! Tenant('a') + App('bc') produced same hash as Tenant('ab') + App('c'): %s", idA)
	}
}

// TestM4Challenger_ZeroFallback_IdentityAndInputFailures verifies fail-fast zero-fallback rules.
func TestM4Challenger_ZeroFallback_IdentityAndInputFailures(t *testing.T) {
	storeMock := newConcurrentMockVectorStore()
	embedMock := &mockEmbeddingService{}
	app := application.NewSemanticCacheApp(embedMock, storeMock)

	contract := &pb.CacheContract{CollectionName: "coll_test"}

	// Test 1: CheckCache with missing TenantId / AppId
	invalidIdentities := []*pb.InfrastructureIdentity{
		nil,
		{TenantId: "", AppId: "app1"},
		{TenantId: "   ", AppId: "app1"},
		{TenantId: "tenant1", AppId: ""},
		{TenantId: "tenant1", AppId: "   "},
	}

	for idx, id := range invalidIdentities {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("Case %d: CheckCache expected fail-fast panic for invalid identity %v, got none", idx, id)
				}
			}()
			_, _, _, _, _ = app.CheckCache(context.Background(), contract, id, nil)
		}()
	}

	// Test 2: StoreExtraction with missing DocumentTitle
	identityValid := &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1"}
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("StoreExtraction expected panic when document_title is empty, got none")
			}
		}()
		_ = app.StoreExtraction(context.Background(), "coll_test", identityValid, nil, "", "CLOSING", nil)
	}()

	// Test 3: StoreExtraction with invalid identity
	for idx, id := range invalidIdentities {
		func() {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("Case %d: StoreExtraction expected fail-fast panic for invalid identity %v, got none", idx, id)
				}
			}()
			_ = app.StoreExtraction(context.Background(), "coll_test", id, nil, "Title", "CLOSING", nil)
		}()
	}
}

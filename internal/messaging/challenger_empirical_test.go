package messaging

import "github.com/AndrewK4758/shared_utils/payloadkeys"

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// MockVectorStore implements domain.VectorStore for challenger tests.
type MockVectorStore struct {
	SearchFunc           func(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error)
	UpsertFunc           func(ctx context.Context, collectionName string, record domain.CacheRecord) error
	CheckMetadataFunc    func(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) (bool, error)
	GetByMetadataFunc    func(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) ([]domain.SearchResult, error)
	EnsureCollectionFunc func(ctx context.Context, collectionName string, denseDim uint64, pqCompression uint32) error
	UpsertHybridFunc     func(ctx context.Context, collectionName string, record domain.CacheRecord) error
	HybridSearchFunc     func(ctx context.Context, collectionName string, denseVector []float32, sparseVector *domain.SparseVector, identity *pb.InfrastructureIdentity, metadata map[string]any, alpha float32, limit int) ([]domain.SearchResult, error)
}

func (m *MockVectorStore) Search(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
	if m.SearchFunc != nil {
		return m.SearchFunc(ctx, collectionName, vector, identity, metadata, limit)
	}
	return nil, nil
}

func (m *MockVectorStore) Upsert(ctx context.Context, collectionName string, record domain.CacheRecord) error {
	if m.UpsertFunc != nil {
		return m.UpsertFunc(ctx, collectionName, record)
	}
	return nil
}

func (m *MockVectorStore) CheckMetadata(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) (bool, error) {
	if m.CheckMetadataFunc != nil {
		return m.CheckMetadataFunc(ctx, collectionName, identity, metadata)
	}
	return false, nil
}

func (m *MockVectorStore) GetByMetadata(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) ([]domain.SearchResult, error) {
	if m.GetByMetadataFunc != nil {
		return m.GetByMetadataFunc(ctx, collectionName, identity, metadata)
	}
	return nil, nil
}

func (m *MockVectorStore) EnsureCollection(ctx context.Context, collectionName string, denseDim uint64, pqCompression uint32) error {
	if m.EnsureCollectionFunc != nil {
		return m.EnsureCollectionFunc(ctx, collectionName, denseDim, pqCompression)
	}
	return nil
}

func (m *MockVectorStore) UpsertHybrid(ctx context.Context, collectionName string, record domain.CacheRecord) error {
	if m.UpsertHybridFunc != nil {
		return m.UpsertHybridFunc(ctx, collectionName, record)
	}
	return m.Upsert(ctx, collectionName, record)
}

func (m *MockVectorStore) HybridSearch(ctx context.Context, collectionName string, denseVector []float32, sparseVector *domain.SparseVector, identity *pb.InfrastructureIdentity, metadata map[string]any, alpha float32, limit int) ([]domain.SearchResult, error) {
	if m.HybridSearchFunc != nil {
		return m.HybridSearchFunc(ctx, collectionName, denseVector, sparseVector, identity, metadata, alpha, limit)
	}
	return m.Search(ctx, collectionName, denseVector, identity, metadata, limit)
}


// MockEmbedder implements domain.EmbeddingService for challenger tests.
type MockEmbedder struct {
	GenerateFunc func(ctx context.Context, text string) ([]float32, error)
}

func (m *MockEmbedder) Generate(ctx context.Context, text string) ([]float32, error) {
	if m.GenerateFunc != nil {
		return m.GenerateFunc(ctx, text)
	}
	return []float32{0.1, 0.2, 0.3}, nil
}

// MockJetStreamMsg implements jetstream.Msg for challenger tests.
type MockJetStreamMsg struct {
	subject string
	data    []byte
	headers nats.Header
	meta    *jetstream.MsgMetadata

	acked  bool
	nacked bool
	termed bool
	inProg bool
	mu     sync.Mutex
}

func (m *MockJetStreamMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return m.meta, nil
}

func (m *MockJetStreamMsg) Data() []byte {
	return m.data
}

func (m *MockJetStreamMsg) Subject() string {
	return m.subject
}

func (m *MockJetStreamMsg) Reply() string {
	return ""
}

func (m *MockJetStreamMsg) Headers() nats.Header {
	return m.headers
}

func (m *MockJetStreamMsg) Ack() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked = true
	return nil
}

func (m *MockJetStreamMsg) DoubleAck(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked = true
	return nil
}

func (m *MockJetStreamMsg) Nak() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nacked = true
	return nil
}

func (m *MockJetStreamMsg) NakWithDelay(delay time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nacked = true
	return nil
}

func (m *MockJetStreamMsg) Term() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.termed = true
	return nil
}

func (m *MockJetStreamMsg) TermWithReason(reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.termed = true
	return nil
}

func (m *MockJetStreamMsg) InProgress() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inProg = true
	return nil
}

// Empirical Test: Nil Metadata in Msg
func TestChallenger_NilMetadataInMsg(t *testing.T) {
	store := &MockVectorStore{}
	embedder := &MockEmbedder{}
	app := application.NewSemanticCacheApp(embedder, store)

	h := &JetStreamHandler{
		app: app,
	}

	payload, _ := structpb.NewStruct(map[string]any{
		payloadkeys.NodeConfigKey: map[string]any{"node_name": "TestNode"},
		payloadkeys.CacheKey: map[string]any{
			"collection_name": "test_coll",
		},
	})
	req := &pb.ExecuteTaskRequest{
		TaskSubject: "worker.cache.lookup",
		Identity: &pb.InfrastructureIdentity{
			TenantId: "t1", AppId: "a1", JobId: "j1",
		},
		Payload: payload,
	}

	data, _ := proto.Marshal(req)
	msg := &MockJetStreamMsg{
		subject: "worker.cache.lookup",
		data:    data,
		meta:    nil, // NIL Metadata!
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("CRITICAL BUG: handleMessage panicked on nil msg.Metadata(): %v", r)
		}
	}()

	h.handleMessage(msg)
}

// Empirical Test: Error Variable Overwriting in CheckCache Failure
func TestChallenger_CheckCacheErrorOverwrittenBySendReply(t *testing.T) {
	checkCacheErr := errors.New("simulated vector store search failure")
	store := &MockVectorStore{
		SearchFunc: func(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
			return nil, checkCacheErr
		},
	}
	embedder := &MockEmbedder{}
	app := application.NewSemanticCacheApp(embedder, store)

	h := &JetStreamHandler{
		app: app,
	}

	payload, _ := structpb.NewStruct(map[string]any{
		payloadkeys.NodeConfigKey: map[string]any{"node_name": "TestNode"},
		payloadkeys.CacheKey: map[string]any{
			"collection_name": "test_coll",
		},
	})
	req := &pb.ExecuteTaskRequest{
		TaskSubject: "worker.cache.lookup",
		Identity: &pb.InfrastructureIdentity{
			TenantId: "t1", AppId: "a1", JobId: "j1",
		},
		Payload: payload,
	}

	data, _ := proto.Marshal(req)
	// Without reply header, sendErrorReply returns nil immediately
	msg := &MockJetStreamMsg{
		subject: "worker.cache.lookup",
		data:    data,
		meta:    &jetstream.MsgMetadata{NumDelivered: 1},
	}

	h.handleMessage(msg)

	msg.mu.Lock()
	defer msg.mu.Unlock()
	if !msg.nacked {
		t.Errorf("Expected message to be NACKed on CheckCache failure")
	}
}

// Empirical Test: Extreme Concurrency Stress Test on JetStreamHandler and App
func TestChallenger_ExtremeConcurrencyStress(t *testing.T) {
	store := &MockVectorStore{
		SearchFunc: func(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
			return []domain.SearchResult{
				{
					Score: 0.95,
					Record: domain.CacheRecord{
						JSONPayload: `{"result": "success"}`,
					},
				},
			}, nil
		},
		UpsertFunc: func(ctx context.Context, collectionName string, record domain.CacheRecord) error {
			return nil
		},
	}
	embedder := &MockEmbedder{}
	app := application.NewSemanticCacheApp(embedder, store)

	h := &JetStreamHandler{
		app: app,
	}

	payload1, _ := structpb.NewStruct(map[string]any{
		payloadkeys.NodeConfigKey: map[string]any{"node_name": "TestNode"},
		payloadkeys.CacheKey: map[string]any{
			"collection_name": "coll1",
			"confidence":      0.8,
		},
	})
	req1 := &pb.ExecuteTaskRequest{
		TaskSubject: "worker.cache.lookup",
		Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
		Payload:     payload1,
	}
	data1, _ := proto.Marshal(req1)

	payload2, _ := structpb.NewStruct(map[string]any{
		payloadkeys.NodeConfigKey: map[string]any{"node_name": "TestNode2"},
		payloadkeys.CacheKey: map[string]any{
			"collection_name": "coll2",
		},
		payloadkeys.NodeOutputKey: map[string]any{"data": "value"},
		payloadkeys.MetadataKey: map[string]any{
			"subject_class":  "test",
			"TemplateHash":   "hash",
			"document_title": "title",
		},
	})
	req2 := &pb.ExecuteTaskRequest{
		TaskSubject: "worker.cache.background.update",
		Identity:    &pb.InfrastructureIdentity{TenantId: "t2", AppId: "a2", JobId: "j2"},
		Payload:     payload2,
	}
	data2, _ := proto.Marshal(req2)

	var wg sync.WaitGroup
	concurrency := 100

	for i := range concurrency {
		wg.Add(2)
		go func(idx int) {
			defer wg.Done()
			msg := &MockJetStreamMsg{
				subject: "worker.cache.lookup",
				data:    data1,
				meta:    &jetstream.MsgMetadata{NumDelivered: 1},
			}
			h.handleMessage(msg)
		}(i)

		go func(idx int) {
			defer wg.Done()
			msg := &MockJetStreamMsg{
				subject: "worker.cache.background.update",
				data:    data2,
				meta:    &jetstream.MsgMetadata{NumDelivered: 1},
			}
			h.handleMessage(msg)
		}(i)
	}

	wg.Wait()
}

// Empirical Test: Malformed Protobuf Data
func TestChallenger_MalformedProtobufData(t *testing.T) {
	store := &MockVectorStore{}
	embedder := &MockEmbedder{}
	app := application.NewSemanticCacheApp(embedder, store)
	h := &JetStreamHandler{app: app}

	msg := &MockJetStreamMsg{
		subject: "worker.cache.lookup",
		data:    []byte("invalid protobuf garbage data 12345"),
		meta:    &jetstream.MsgMetadata{NumDelivered: 1},
	}

	h.handleMessage(msg)

	msg.mu.Lock()
	defer msg.mu.Unlock()
	if !msg.termed {
		t.Errorf("Expected malformed protobuf message to be Termed")
	}
}

// Empirical Test: Background Update with Empty or Corrupt NodeOutput
func TestChallenger_BackgroundUpdateCorruptNodeOutput(t *testing.T) {
	store := &MockVectorStore{}
	embedder := &MockEmbedder{}
	app := application.NewSemanticCacheApp(embedder, store)
	h := &JetStreamHandler{app: app}

	payload, _ := structpb.NewStruct(map[string]any{
		payloadkeys.NodeConfigKey: map[string]any{"node_name": "TestNode"},
		payloadkeys.CacheKey: map[string]any{
			"collection_name": "coll1",
		},
	})
	req := &pb.ExecuteTaskRequest{
		TaskSubject: "worker.cache.background.update",
		Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
		Payload:     payload,
	}
	data, _ := proto.Marshal(req)

	msg := &MockJetStreamMsg{
		subject: "worker.cache.background.update",
		data:    data,
		meta:    &jetstream.MsgMetadata{NumDelivered: 1},
	}

	h.handleMessage(msg)
	
	msg.mu.Lock()
	defer msg.mu.Unlock()
	if !msg.nacked {
		t.Errorf("Expected msg to be nacked for missing node_output")
	}
}

// Empirical Test: StoreExtraction with Precomputed Vector Any
func TestChallenger_StoreExtractionWithPrecomputedVector(t *testing.T) {
	var storedRecord domain.CacheRecord
	store := &MockVectorStore{
		UpsertFunc: func(ctx context.Context, collectionName string, record domain.CacheRecord) error {
			storedRecord = record
			return nil
		},
	}
	embedder := &MockEmbedder{}
	app := application.NewSemanticCacheApp(embedder, store)

	// Create list value any for vector [0.5, 0.6, 0.7]
	listVal, _ := structpb.NewList([]any{0.5, 0.6, 0.7})
	anyVec, _ := anypb.New(listVal)

	err := app.StoreExtraction(context.Background(), "test_coll", &pb.InfrastructureIdentity{
		TenantId: "t1", AppId: "a1", JobId: "j1",
	}, map[string]any{"key": "val", "subject_class": "test", "document_title": "title", "TemplateHash": "hash"}, `{"payload": 1}`, anyVec)

	if err != nil {
		t.Fatalf("StoreExtraction failed: %v", err)
	}

	if len(storedRecord.Vector) != 3 || storedRecord.Vector[0] != 0.5 {
		t.Errorf("Expected vector [0.5, 0.6, 0.7], got %v", storedRecord.Vector)
	}
}

package messaging

import (
	"context"
	"fmt"
	"testing"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/payloadkeys"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
	"github.com/doc_processor/semantic_cache_service/internal/infrastructure/qdrant"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type dummyEmbedder struct{}

func (d *dummyEmbedder) Generate(ctx context.Context, text string) ([]float32, error) {
	return []float32{0.1, 0.2, 0.3}, nil
}

type dummyStore struct{}

func (d *dummyStore) Search(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
	return nil, nil
}
func (d *dummyStore) Upsert(ctx context.Context, collectionName string, record domain.CacheRecord) error {
	return nil
}
func (d *dummyStore) CheckMetadata(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) (bool, error) {
	return false, nil
}
func (d *dummyStore) GetByMetadata(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) ([]domain.SearchResult, error) {
	return nil, nil
}
func (d *dummyStore) EnsureCollection(ctx context.Context, collectionName string, denseDim uint64, pqCompression uint32) error {
	return nil
}
func (d *dummyStore) UpsertHybrid(ctx context.Context, collectionName string, record domain.CacheRecord) error {
	return nil
}
func (d *dummyStore) HybridSearch(ctx context.Context, collectionName string, denseVector []float32, sparseVector *domain.SparseVector, identity *pb.InfrastructureIdentity, metadata map[string]any, alpha float32, limit int) ([]domain.SearchResult, error) {
	return nil, nil
}

func protoMarshalReq(req *pb.ExecuteTaskRequest) ([]byte, error) {
	return proto.Marshal(req)
}

func assertPanicContains(t *testing.T, expectedSubstring string, fn func()) {
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
	if !testing.Short() {
		t.Logf("Caught expected panic: %v", panicStr)
	}
}

func TestChallenger2_StoreExtraction_BlankDocumentTitle_Panics(t *testing.T) {
	app := application.NewSemanticCacheApp(&dummyEmbedder{}, &dummyStore{})
	validId := &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"}

	assertPanicContains(t, "missing required property document_title", func() {
		_ = app.StoreExtraction(context.Background(), "coll", validId, map[string]any{"document_title": "   ", "subject_class": "test", "TemplateHash": "hash"}, "", nil)
	})

	assertPanicContains(t, "missing required property document_title", func() {
		_ = app.StoreExtraction(context.Background(), "coll", validId, map[string]any{}, "", nil)
	})
}

func TestChallenger2_StoreExtraction_NilOrInvalidIdentity_Panics(t *testing.T) {
	app := application.NewSemanticCacheApp(&dummyEmbedder{}, &dummyStore{})

	assertPanicContains(t, "TenantId", func() {
		_ = app.StoreExtraction(context.Background(), "coll", nil, map[string]any{}, "", nil)
	})

	assertPanicContains(t, "TenantId", func() {
		_ = app.StoreExtraction(context.Background(), "coll", &pb.InfrastructureIdentity{TenantId: "", AppId: "a1", JobId: "j1"}, map[string]any{}, "", nil)
	})
}

func TestChallenger2_CheckCache_NilContractOrIdentity_Panics(t *testing.T) {
	app := application.NewSemanticCacheApp(&dummyEmbedder{}, &dummyStore{})
	validId := &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"}

	assertPanicContains(t, "cache contract is required", func() {
		_, _, _, _ = app.CheckCache(context.Background(), nil, validId)
	})

	contract := &pb.CacheContract{CollectionName: "coll"}
	assertPanicContains(t, "TenantId", func() {
		_, _, _, _ = app.CheckCache(context.Background(), contract, nil)
	})
}

func TestChallenger2_JetStream_MissingIdentity_Panics(t *testing.T) {
	app := application.NewSemanticCacheApp(&dummyEmbedder{}, &dummyStore{})
	h := &JetStreamHandler{app: app}

	req := &pb.ExecuteTaskRequest{
		TaskSubject: "worker.cache.lookup",
	}
	data, _ := protoMarshalReq(req)
	msg := &mockCacheMsg{data: data, subject: "worker.cache.lookup"}

	h.handleMessage(msg)
	if !msg.nacked {
		t.Fatalf("expected msg to be nacked for missing Identity")
	}
}

func TestChallenger2_JetStream_MissingTaskSubject_Panics(t *testing.T) {
	app := application.NewSemanticCacheApp(&dummyEmbedder{}, &dummyStore{})
	h := &JetStreamHandler{app: app}

	req := &pb.ExecuteTaskRequest{
		Identity: &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
	}
	data, _ := protoMarshalReq(req)
	msg := &mockCacheMsg{data: data, subject: "worker.cache.lookup"}

	h.handleMessage(msg)
	if !msg.nacked {
		t.Fatalf("expected msg to be nacked for missing TaskSubject")
	}
}

func TestChallenger2_JetStream_MissingNodeConfig_Panics(t *testing.T) {
	app := application.NewSemanticCacheApp(&dummyEmbedder{}, &dummyStore{})
	h := &JetStreamHandler{app: app}

	req := &pb.ExecuteTaskRequest{
		Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
		TaskSubject: "worker.cache.lookup",
	}
	data, _ := protoMarshalReq(req)
	msg := &mockCacheMsg{data: data, subject: "worker.cache.lookup"}

	h.handleMessage(msg)
	if !msg.nacked {
		t.Fatalf("expected msg to be nacked for missing node_config")
	}
}

func TestChallenger2_JetStream_MissingCacheContract_Panics(t *testing.T) {
	app := application.NewSemanticCacheApp(&dummyEmbedder{}, &dummyStore{})
	h := &JetStreamHandler{app: app}

	payloadStruct, _ := structpb.NewStruct(map[string]any{
		payloadkeys.NodeConfigKey: map[string]any{
			"node_name": "SemanticCacheWorker",
		},
	})

	req := &pb.ExecuteTaskRequest{
		Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
		TaskSubject: "worker.cache.lookup",
		Payload:     payloadStruct,
	}
	data, _ := protoMarshalReq(req)
	msg := &mockCacheMsg{data: data, subject: "worker.cache.lookup"}

	h.handleMessage(msg)
	if !msg.nacked {
		t.Fatalf("expected msg to be nacked for missing cache contract")
	}
}

func TestChallenger2_JetStream_BackgroundUpdate_MissingNodeOutput_Panics(t *testing.T) {
	app := application.NewSemanticCacheApp(&dummyEmbedder{}, &dummyStore{})
	h := &JetStreamHandler{app: app}

	anyPayload, _ := anypb.New(wrapperspb.String("some text"))
	cacheStruct, _ := structpb.NewStruct(map[string]any{
		"collection_name": "test_coll",
	})
	_ = anyPayload

	payloadStruct, _ := structpb.NewStruct(map[string]any{
		payloadkeys.NodeConfigKey: map[string]any{
			"node_name": "SemanticCacheWorker",
		},
		payloadkeys.CacheKey: cacheStruct,
	})

	req := &pb.ExecuteTaskRequest{
		Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
		TaskSubject: "worker.cache.background.update",
		Payload:     payloadStruct,
	}

	// Inject non-empty EmbeddingLookup into cache struct via raw payload Map or proto
	// Create cache struct with embedding lookup or inference_input_key
	payloadStruct, _ = structpb.NewStruct(map[string]any{
		payloadkeys.NodeConfigKey: map[string]any{
			"node_name":           "SemanticCacheWorker",
			"inference_input_key": "inbound_text",
		},
		payloadkeys.GlobalStateKey: map[string]any{
			"functional": map[string]any{
				"inbound_text": "Sample text for cache embedding",
			},
		},
		payloadkeys.CacheKey: map[string]any{
			"collection_name": "test_coll",
		},
	})
	req.Payload = payloadStruct

	data, _ := protoMarshalReq(req)
	msg := &mockCacheMsg{data: data, subject: "worker.cache.background.update"}

	h.handleMessage(msg)
	if !msg.nacked {
		t.Fatalf("expected msg to be nacked for missing node_output")
	}
}

func TestChallenger2_JetStream_BackgroundUpdate_MissingDocumentTitle_Panics(t *testing.T) {
	app := application.NewSemanticCacheApp(&dummyEmbedder{}, &dummyStore{})
	h := &JetStreamHandler{app: app}

	payloadStruct, _ := structpb.NewStruct(map[string]any{
		payloadkeys.NodeConfigKey: map[string]any{
			"node_name":           "SemanticCacheWorker",
			"inference_input_key": "inbound_text",
		},
		payloadkeys.GlobalStateKey: map[string]any{
			"functional": map[string]any{
				"inbound_text": "Sample text for cache embedding",
			},
		},
		payloadkeys.CacheKey: map[string]any{
			"collection_name": "test_coll",
		},
		payloadkeys.NodeOutputKey: map[string]any{
			"other_field": "some_value",
		},
	})

	req := &pb.ExecuteTaskRequest{
		Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
		TaskSubject: "worker.cache.background.update",
		Payload:     payloadStruct,
	}
	data, _ := protoMarshalReq(req)
	msg := &mockCacheMsg{data: data, subject: "worker.cache.background.update"}

	h.handleMessage(msg)
	if !msg.nacked {
		t.Fatalf("expected msg to be nacked for missing document_title")
	}
}

func TestChallenger2_JetStream_ExceededMaxDeliveryAttempts_Panics(t *testing.T) {
	app := application.NewSemanticCacheApp(&dummyEmbedder{}, &dummyStore{})
	h := &JetStreamHandler{app: app}

	payloadStruct, _ := structpb.NewStruct(map[string]any{
		payloadkeys.NodeConfigKey: map[string]any{
			"node_name": "SemanticCacheWorker",
		},
	})

	req := &pb.ExecuteTaskRequest{
		Identity:    &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"},
		TaskSubject: "worker.cache.lookup",
		Payload:     payloadStruct,
	}
	data, _ := protoMarshalReq(req)
	msg := &mockCacheMsg{data: data, subject: "worker.cache.lookup", numDeliv: 6}

	h.handleMessage(msg)
	if !msg.nacked {
		t.Fatalf("expected msg to be nacked for exceeding max delivery attempts")
	}
}

func TestChallenger2_Qdrant_EmptyCollection_Panics(t *testing.T) {
	c := &qdrant.Client{}
	validId := &pb.InfrastructureIdentity{TenantId: "t1", AppId: "a1", JobId: "j1"}
	dummyMeta := map[string]any{"key": "val"}

	assertPanicContains(t, "collectionName is required", func() {
		_, _ = c.Search(context.Background(), "", []float32{0.1}, validId, dummyMeta, 1)
	})

	assertPanicContains(t, "collectionName is required", func() {
		_ = c.Upsert(context.Background(), "", domain.CacheRecord{Identity: validId, Metadata: dummyMeta})
	})

	assertPanicContains(t, "collectionName is required", func() {
		_, _ = c.CheckMetadata(context.Background(), "", validId, dummyMeta)
	})

	assertPanicContains(t, "collectionName is required", func() {
		_, _ = c.GetByMetadata(context.Background(), "", validId, dummyMeta)
	})
}

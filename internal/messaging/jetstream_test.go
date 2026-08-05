package messaging

import "github.com/AndrewK4758/shared_utils/payloadkeys"

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type mockCacheMsg struct {
	data     []byte
	subject  string
	numDeliv uint64
	termed   bool
	acked    bool
	nacked   bool
	headers  nats.Header
}

func (m *mockCacheMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.numDeliv == 0 {
		m.numDeliv = 1
	}
	return &jetstream.MsgMetadata{
		NumDelivered: m.numDeliv,
	}, nil
}
func (m *mockCacheMsg) Subject() string {
	return m.subject
}
func (m *mockCacheMsg) Data() []byte {
	return m.data
}
func (m *mockCacheMsg) Headers() nats.Header {
	return m.headers
}
func (m *mockCacheMsg) Reply() string {
	return ""
}
func (m *mockCacheMsg) Ack() error {
	m.acked = true
	return nil
}
func (m *mockCacheMsg) DoubleAck(context.Context) error {
	return nil
}
func (m *mockCacheMsg) Nak() error {
	m.nacked = true
	return nil
}
func (m *mockCacheMsg) NakWithDelay(delay time.Duration) error {
	m.nacked = true
	return nil
}
func (m *mockCacheMsg) InProgress() error {
	return nil
}
func (m *mockCacheMsg) Term() error {
	m.termed = true
	return nil
}
func (m *mockCacheMsg) TermWithReason(reason string) error {
	m.termed = true
	return nil
}

func TestHitStr(t *testing.T) {
	if hitStr(true) != "hit" {
		t.Fatalf("expected hitStr(true) == 'hit', got '%s'", hitStr(true))
	}
	if hitStr(false) != "miss" {
		t.Fatalf("expected hitStr(false) == 'miss', got '%s'", hitStr(false))
	}
}

func TestHandleMessage_MaxDeliveryAttemptsExceeded_TerminatesMessage(t *testing.T) {
	handler := &JetStreamHandler{}

	payload, _ := structpb.NewStruct(map[string]interface{}{
		"graph_id":        "graph-max-retries-101",
		"initial_node_id": "node-init-202",
	})
	req := &pb.ExecuteTaskRequest{
		TaskSubject: "worker.cache.lookup",
		Identity: &pb.InfrastructureIdentity{
			JobId:    "job-max-retries",
			TenantId: "t1",
			AppId:    "a1",
		},
		Payload: payload,
	}
	data, _ := proto.Marshal(req)

	msg := &mockCacheMsg{
		data:     data,
		subject:  "worker.cache.lookup",
		numDeliv: 6, // > 5
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("Expected panic when NumDelivered > 5, but got none")
		}
	}()

	handler.handleMessage(msg)
}

func TestHandleMessage_MissingNodeConfig_TermsMessage(t *testing.T) {
	handler := &JetStreamHandler{}

	payload, _ := structpb.NewStruct(map[string]interface{}{
		"graph_id":        "graph-missing-cfg",
		"initial_node_id": "node-missing-cfg",
		payloadkeys.CacheKey: map[string]interface{}{
			"collection_name": "test_coll",
		},
	})
	req := &pb.ExecuteTaskRequest{
		TaskSubject: "worker.cache.lookup",
		Identity: &pb.InfrastructureIdentity{
			JobId:    "job-missing-cfg",
			TenantId: "t1",
			AppId:    "a1",
		},
		Payload: payload,
	}
	data, _ := proto.Marshal(req)

	msg := &mockCacheMsg{
		data:     data,
		subject:  "worker.cache.lookup",
		numDeliv: 1,
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("Expected panic due to missing node_config.node_name, but got none")
		}
	}()

	handler.handleMessage(msg)
}

func TestHandleMessage_UnknownSubject_AcksMessage(t *testing.T) {
	handler := &JetStreamHandler{}

	payload, _ := structpb.NewStruct(map[string]interface{}{
		"graph_id":        "graph-unknown-subj",
		"initial_node_id": "node-unknown-subj",
		payloadkeys.NodeConfigKey: map[string]interface{}{"node_name": "Test Node"},
		payloadkeys.CacheKey: map[string]interface{}{
			"collection_name": "test_coll",
		},
	})
	req := &pb.ExecuteTaskRequest{
		TaskSubject: "worker.cache.unknown",
		Identity: &pb.InfrastructureIdentity{
			JobId:    "job-unknown-subj",
			TenantId: "t1",
			AppId:    "a1",
		},
		StepId:  "step-unknown",
		Payload: payload,
	}
	data, _ := proto.Marshal(req)

	msg := &mockCacheMsg{
		data:     data,
		subject:  "worker.cache.unknown",
		numDeliv: 1,
	}

	handler.handleMessage(msg)

	if !msg.acked {
		t.Fatalf("Expected msg.Ack() to be called for unknown subject, but it was not")
	}
}

func TestHandleMessage_MissingCacheContract_TermsMessage(t *testing.T) {
	handler := &JetStreamHandler{}

	payload, _ := structpb.NewStruct(map[string]interface{}{
		"graph_id":        "graph-missing-cache",
		"initial_node_id": "node-missing-cache",
		payloadkeys.NodeConfigKey: map[string]interface{}{"node_name": "Test Node"},
	})
	req := &pb.ExecuteTaskRequest{
		TaskSubject: "worker.cache.lookup",
		Identity: &pb.InfrastructureIdentity{
			JobId:    "job-missing-cache",
			TenantId: "t1",
			AppId:    "a1",
		},
		StepId:  "step-1",
		Payload: payload,
	}
	data, _ := proto.Marshal(req)

	msg := &mockCacheMsg{
		data:     data,
		subject:  "worker.cache.lookup",
		numDeliv: 1,
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("Expected panic due to missing cache config, but got none")
		}
	}()

	handler.handleMessage(msg)
}

func TestHandleMessage_GraphIdAndInitialNodeIdExtraction(t *testing.T) {
	embedMock := &mockEmbedder{}
	storeMock := &mockStore{}
	app := application.NewSemanticCacheApp(embedMock, storeMock)
	handler := &JetStreamHandler{app: app}

	payload, _ := structpb.NewStruct(map[string]interface{}{
		"graph_id":        "graph-audit-999",
		"initial_node_id": "node-start-111",
		payloadkeys.NodeConfigKey: map[string]interface{}{"node_name": "Cache Audit Node"},
		payloadkeys.CacheKey: map[string]interface{}{
			"collection_name": "test_coll",
		},
	})
	req := &pb.ExecuteTaskRequest{
		TaskSubject: "worker.cache.lookup",
		Identity: &pb.InfrastructureIdentity{
			JobId:    "job-audit-001",
			TenantId: "t1",
			AppId:    "a1",
		},
		StepId:  "step-audit",
		Payload: payload,
	}
	data, _ := proto.Marshal(req)

	msg := &mockCacheMsg{
		data:     data,
		subject:  "worker.cache.lookup",
		numDeliv: 1,
	}

	handler.handleMessage(msg)

	if !msg.acked {
		t.Fatalf("Expected msg.Ack() to be called for request with GraphId and InitialNodeId, but it was not")
	}
}

func TestSendSuccessReply_FailFastOnInvalidJSON(t *testing.T) {
	handler := &JetStreamHandler{}

	headers := make(nats.Header)
	headers.Set("Reply-Subject", "test.reply.subject")
	msg := &mockCacheMsg{
		headers: headers,
	}

	reqIdentity := &pb.InfrastructureIdentity{
		TenantId: "tenant-1",
		AppId:    "app-1",
		JobId:    "job-1",
	}

	err := handler.sendSuccessReply(context.Background(), msg, "{invalid-json-content}", reqIdentity)
	if err == nil {
		t.Fatalf("Expected error when sendSuccessReply receives invalid JSON, got nil (violates GEMINI.md Fail Fast rule)")
	}
}

type mockEmbedder struct{}

func (m *mockEmbedder) Generate(ctx context.Context, text string) ([]float32, error) {
	return []float32{0.1, 0.2, 0.3}, nil
}

type mockStore struct {
	mu           sync.Mutex
	storedRecord domain.CacheRecord
}

func (m *mockStore) Search(ctx context.Context, collectionName string, vector []float32, identity *pb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
	return []domain.SearchResult{
		{
			Record: domain.CacheRecord{JSONPayload: `{"cached":true}`},
			Score:  0.99,
		},
	}, nil
}

func (m *mockStore) Upsert(ctx context.Context, collectionName string, record domain.CacheRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.storedRecord = record
	return nil
}

func (m *mockStore) CheckMetadata(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) (bool, error) {
	return true, nil
}

func (m *mockStore) GetByMetadata(ctx context.Context, collectionName string, identity *pb.InfrastructureIdentity, metadata map[string]any) ([]domain.SearchResult, error) {
	return nil, nil
}

func TestHandleMessage_BackgroundUpdatePayloadExtraction(t *testing.T) {
	embedMock := &mockEmbedder{}
	storeMock := &mockStore{}
	app := application.NewSemanticCacheApp(embedMock, storeMock)
	handler := &JetStreamHandler{app: app}

	t.Run("Extract payload from NodeOutput", func(t *testing.T) {
		payload, _ := structpb.NewStruct(map[string]interface{}{
			"graph_id":        "graph-bg-1",
			"initial_node_id": "node-bg-1",
			payloadkeys.NodeConfigKey: map[string]interface{}{"node_name": "test"},
			payloadkeys.CacheKey: map[string]interface{}{
				"collection_name": "test_coll",
			},
			payloadkeys.NodeOutputKey: map[string]interface{}{
				"extracted_key": "val1",
			},
			payloadkeys.MetadataKey: map[string]interface{}{
				"subject_class":  "test",
				"TemplateHash":   "hash",
				"document_title": "title",
			},
		})
		req := &pb.ExecuteTaskRequest{
			TaskSubject: "worker.cache.background.update",
			Identity:    &pb.InfrastructureIdentity{JobId: "job1", TenantId: "t1", AppId: "a1"},
			Payload:     payload,
		}
		data, _ := proto.Marshal(req)
		msg := &mockCacheMsg{data: data, subject: "worker.cache.background.update", numDeliv: 1}

		handler.handleMessage(msg)

		if !msg.acked {
			t.Fatal("expected msg to be acked")
		}
	})

	t.Run("Fails fast when NodeOutput is empty", func(t *testing.T) {
		payload, _ := structpb.NewStruct(map[string]interface{}{
			"graph_id":        "graph-bg-2",
			"initial_node_id": "node-bg-2",
			payloadkeys.NodeConfigKey: map[string]interface{}{"node_name": "test"},
			payloadkeys.CacheKey: map[string]interface{}{
				"collection_name": "test_coll",
			},
		})
		req := &pb.ExecuteTaskRequest{
			TaskSubject: "worker.cache.background.update",
			Identity:    &pb.InfrastructureIdentity{JobId: "job1", TenantId: "t1", AppId: "a1"},
			Payload:     payload,
		}
		data, _ := proto.Marshal(req)
		msg := &mockCacheMsg{data: data, subject: "worker.cache.background.update", numDeliv: 1}

		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic due to missing NodeOutput, but got none")
			}
		}()
		handler.handleMessage(msg)
	})
}

func TestHandleMessage_ConcurrentStressTest(t *testing.T) {
	embedMock := &mockEmbedder{}
	storeMock := &mockStore{}
	app := application.NewSemanticCacheApp(embedMock, storeMock)
	handler := &JetStreamHandler{app: app}

	payload, _ := structpb.NewStruct(map[string]interface{}{
		"graph_id":        "graph-stress-99",
		"initial_node_id": "node-stress-99",
		payloadkeys.NodeConfigKey: map[string]interface{}{"node_name": "stress_node"},
		payloadkeys.CacheKey: map[string]interface{}{
			"collection_name": "stress_coll",
			"confidence":      0.8,
		},
		payloadkeys.NodeOutputKey: map[string]interface{}{
			"stress": true,
		},
		payloadkeys.MetadataKey: map[string]interface{}{
			"subject_class":  "test",
			"TemplateHash":   "hash",
			"document_title": "title",
		},
	})
	req := &pb.ExecuteTaskRequest{
		TaskSubject: "worker.cache.lookup",
		Identity:    &pb.InfrastructureIdentity{JobId: "job-stress", TenantId: "t-stress", AppId: "a-stress"},
		Payload:     payload,
	}
	data, _ := proto.Marshal(req)

	const goroutines = 50
	done := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			subj := "worker.cache.lookup"
			if id%2 == 0 {
				subj = "worker.cache.background.update"
			}
			msg := &mockCacheMsg{data: data, subject: subj, numDeliv: 1}
			handler.handleMessage(msg)
			if !msg.acked {
				t.Errorf("goroutine %d: msg not acked", id)
			}
			done <- true
		}(i)
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}
}

func unmarshalStruct(jsonStr string) *structpb.Struct {
	var s structpb.Struct
	if err := protojson.Unmarshal([]byte(jsonStr), &s); err != nil {
		panic(err)
	}
	return &s
}

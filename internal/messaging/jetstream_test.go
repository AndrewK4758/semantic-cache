package messaging

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

	req := &pb.ExecuteWorkflowNodeRequest{
		GraphId:       "graph-max-retries-101",
		InitialNodeId: "node-init-202",
		Identity: &pb.InfrastructureIdentity{
			JobId:    "job-max-retries",
			TenantId: "t1",
			AppId:    "a1",
		},
	}
	data, _ := proto.Marshal(req)

	msg := &mockCacheMsg{
		data:     data,
		subject:  "worker.cache.lookup",
		numDeliv: 6, // > 5
	}

	handler.handleMessage(msg)

	if !msg.termed {
		t.Fatalf("Expected msg.Term() to be called when NumDelivered > 5, but it was not")
	}
}

func TestHandleMessage_MissingNodeConfig_TermsMessage(t *testing.T) {
	handler := &JetStreamHandler{}

	req := &pb.ExecuteWorkflowNodeRequest{
		GraphId:       "graph-missing-cfg",
		InitialNodeId: "node-missing-cfg",
		Identity: &pb.InfrastructureIdentity{
			JobId:    "job-missing-cfg",
			TenantId: "t1",
			AppId:    "a1",
		},
		NodeConfig: nil,
	}
	data, _ := proto.Marshal(req)

	msg := &mockCacheMsg{
		data:     data,
		subject:  "worker.cache.lookup",
		numDeliv: 1,
	}

	handler.handleMessage(msg)

	if !msg.termed {
		t.Fatalf("Expected msg.Term() to be called when NodeConfig is missing, but it was not")
	}
}

func TestHandleMessage_UnknownSubject_AcksMessage(t *testing.T) {
	handler := &JetStreamHandler{}

	req := &pb.ExecuteWorkflowNodeRequest{
		GraphId:       "graph-unknown-subj",
		InitialNodeId: "node-unknown-subj",
		Identity: &pb.InfrastructureIdentity{
			JobId:    "job-unknown-subj",
			TenantId: "t1",
			AppId:    "a1",
		},
		StepId: "step-unknown",
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "Test Node",
		},
		Cache: &pb.CacheContract{
			CollectionName: "test_coll",
		},
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

	req := &pb.ExecuteWorkflowNodeRequest{
		GraphId:       "graph-missing-cache",
		InitialNodeId: "node-missing-cache",
		Identity: &pb.InfrastructureIdentity{
			JobId:    "job-missing-cache",
			TenantId: "t1",
			AppId:    "a1",
		},
		StepId: "step-1",
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "Test Node",
		},
		Cache: nil,
	}
	data, _ := proto.Marshal(req)

	msg := &mockCacheMsg{
		data:     data,
		subject:  "worker.cache.lookup",
		numDeliv: 1,
	}

	handler.handleMessage(msg)

	if !msg.termed {
		t.Fatalf("Expected msg.Term() to be called when Cache contract is missing, but it was not")
	}
}

func TestHandleMessage_GraphIdAndInitialNodeIdExtraction(t *testing.T) {
	embedMock := &mockEmbedder{}
	storeMock := &mockStore{}
	app := application.NewSemanticCacheApp(embedMock, storeMock)
	handler := &JetStreamHandler{app: app}

	req := &pb.ExecuteWorkflowNodeRequest{
		GraphId:       "graph-audit-999",
		InitialNodeId: "node-start-111",
		Identity: &pb.InfrastructureIdentity{
			JobId:    "job-audit-001",
			TenantId: "t1",
			AppId:    "a1",
		},
		StepId: "step-audit",
		NodeConfig: &pb.NodeConfiguration{
			NodeName: "Cache Audit Node",
		},
		Cache: &pb.CacheContract{
			CollectionName: "test_coll",
		},
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
		req := &pb.ExecuteWorkflowNodeRequest{
			GraphId:       "graph-bg-1",
			InitialNodeId: "node-bg-1",
			Identity:      &pb.InfrastructureIdentity{JobId: "job1", TenantId: "t1", AppId: "a1"},
			NodeConfig:    &pb.NodeConfiguration{NodeName: "test"},
			Cache:         &pb.CacheContract{CollectionName: "test_coll"},
			NodeOutput:    unmarshalStruct(`{"extracted_key":"val1"}`),
		}
		data, _ := proto.Marshal(req)
		msg := &mockCacheMsg{data: data, subject: "worker.cache.background.update", numDeliv: 1}

		handler.handleMessage(msg)

		if !msg.acked {
			t.Fatal("expected msg to be acked")
		}
	})

	t.Run("Fails fast when NodeOutput is empty", func(t *testing.T) {
		req := &pb.ExecuteWorkflowNodeRequest{
			GraphId:       "graph-bg-2",
			InitialNodeId: "node-bg-2",
			Identity:      &pb.InfrastructureIdentity{JobId: "job1", TenantId: "t1", AppId: "a1"},
			NodeConfig:    &pb.NodeConfiguration{NodeName: "test"},
			Cache:         &pb.CacheContract{CollectionName: "test_coll"},
		}
		data, _ := proto.Marshal(req)
		msg := &mockCacheMsg{data: data, subject: "worker.cache.background.update", numDeliv: 1}

		handler.handleMessage(msg)

		if !msg.termed {
			t.Fatal("expected msg to be termed due to missing NodeOutput")
		}
	})
}

func TestHandleMessage_ConcurrentStressTest(t *testing.T) {
	embedMock := &mockEmbedder{}
	storeMock := &mockStore{}
	app := application.NewSemanticCacheApp(embedMock, storeMock)
	handler := &JetStreamHandler{app: app}

	req := &pb.ExecuteWorkflowNodeRequest{
		GraphId:       "graph-stress-99",
		InitialNodeId: "node-stress-99",
		Identity:      &pb.InfrastructureIdentity{JobId: "job-stress", TenantId: "t-stress", AppId: "a-stress"},
		NodeConfig:    &pb.NodeConfiguration{NodeName: "stress_node"},
		Cache:         &pb.CacheContract{CollectionName: "stress_coll", Confidence: 0.8},
		NodeOutput:    unmarshalStruct(`{"stress":true}`),
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

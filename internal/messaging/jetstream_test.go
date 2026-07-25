package messaging

import (
	"context"
	"testing"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
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

func TestHandleMessage_MissingNodeConfig_AcksMessage(t *testing.T) {
	handler := &JetStreamHandler{}

	req := &pb.ExecuteWorkflowNodeRequest{
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

	if !msg.acked {
		t.Fatalf("Expected msg.Ack() to be called when NodeConfig is missing, but it was not")
	}
}

func TestHandleMessage_UnknownSubject_AcksMessage(t *testing.T) {
	handler := &JetStreamHandler{}

	req := &pb.ExecuteWorkflowNodeRequest{
		Identity: &pb.InfrastructureIdentity{
			JobId:    "job-unknown-subj",
			TenantId: "t1",
			AppId:    "a1",
		},
		NodeConfig: &pb.NodeConfiguration{
			Cache: &pb.CacheConfiguration{
				CollectionName: "test_coll",
				LookupInput:    "test_input",
			},
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

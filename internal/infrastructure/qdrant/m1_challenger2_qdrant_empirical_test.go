package qdrant

import (
	"context"
	"testing"

	docpb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
	pb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
)

type mockPointsClient struct {
	pb.PointsClient
	capturedUpsertReq *pb.UpsertPoints
}

func (m *mockPointsClient) Upsert(ctx context.Context, in *pb.UpsertPoints, opts ...grpc.CallOption) (*pb.PointsOperationResponse, error) {
	m.capturedUpsertReq = in
	return &pb.PointsOperationResponse{
		Result: &pb.UpdateResult{
			Status: pb.UpdateStatus_Completed,
		},
	}, nil
}

func TestChallenger2_EmpiricalQdrantUpsertPayloadPurity(t *testing.T) {
	mockServer := &mockPointsClient{}
	client := &Client{
		qdrantClient:      mockServer,
		defaultCollection: "test_coll",
	}

	record := domain.CacheRecord{
		ID: "123456781234123412341234567890ab",
		Identity: &docpb.InfrastructureIdentity{
			TenantId: "tenant_qdrant_123",
			AppId:    "app_qdrant_456",
			JobId:    "job_qdrant_789",
		},
		Metadata: map[string]any{
			"document_title": "Closing Disclosure Qdrant",
			"subject_class":  "CLOSING_QDRANT",
			"json_payload":   `{"large_payload": "data"}`,
			"job_id":         "job_qdrant_789",
		},
		Vector: []float32{0.1, 0.2, 0.3},
	}

	err := client.Upsert(context.Background(), "test_coll", record)
	if err != nil {
		t.Fatalf("Upsert returned unexpected error: %v", err)
	}

	if mockServer.capturedUpsertReq == nil || len(mockServer.capturedUpsertReq.Points) == 0 {
		t.Fatalf("Upsert did not capture points request")
	}

	pointPayload := mockServer.capturedUpsertReq.Points[0].Payload

	if tId, ok := pointPayload["tenant_id"]; !ok || tId.GetStringValue() != "tenant_qdrant_123" {
		t.Errorf("Expected payload['tenant_id'] = 'tenant_qdrant_123', got %v", tId)
	}
	if aId, ok := pointPayload["app_id"]; !ok || aId.GetStringValue() != "app_qdrant_456" {
		t.Errorf("Expected payload['app_id'] = 'app_qdrant_456', got %v", aId)
	}
	if dt, ok := pointPayload["document_title"]; !ok || dt.GetStringValue() != "Closing Disclosure Qdrant" {
		t.Errorf("Expected payload['document_title'] = 'Closing Disclosure Qdrant', got %v", dt)
	}
	if sc, ok := pointPayload["subject_class"]; !ok || sc.GetStringValue() != "CLOSING_QDRANT" {
		t.Errorf("Expected payload['subject_class'] = 'CLOSING_QDRANT', got %v", sc)
	}

	if jp, ok := pointPayload["json_payload"]; ok {
		t.Errorf("VIOLATION: Qdrant Upsert payload contains forbidden key 'json_payload': %v", jp.GetStringValue())
	}
	if jId, ok := pointPayload["job_id"]; ok {
		t.Errorf("VIOLATION: Qdrant Upsert payload contains job_id in payload: %v", jId.GetStringValue())
	}
}

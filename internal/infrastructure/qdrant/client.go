package qdrant

import (
	"context"
	"fmt"

	docpb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
	pb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	qdrantClient      pb.PointsClient
	defaultCollection string
	conn              *grpc.ClientConn
}

// NewClient creates a new Qdrant client.
func NewClient(addr string, collection string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to qdrant: %w", err)
	}

	client := pb.NewPointsClient(conn)

	return &Client{
		qdrantClient:      client,
		defaultCollection: collection,
		conn:              conn,
	}, nil
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func buildConditions(identity *docpb.InfrastructureIdentity, metadata map[string]any) []*pb.Condition {
	var conditions []*pb.Condition

	if identity != nil {
		if identity.TenantId != "" {
			conditions = append(conditions, makeKeywordCondition("tenant_id", identity.TenantId))
		}
		if identity.AppId != "" {
			conditions = append(conditions, makeKeywordCondition("app_id", identity.AppId))
		}
	}

	for k, v := range metadata {
		if strVal, ok := v.(string); ok {
			conditions = append(conditions, makeKeywordCondition(k, strVal))
		}
	}
	return conditions
}

func makeKeywordCondition(key, val string) *pb.Condition {
	return &pb.Condition{
		ConditionOneOf: &pb.Condition_Field{
			Field: &pb.FieldCondition{
				Key: key,
				Match: &pb.Match{
					MatchValue: &pb.Match_Keyword{
						Keyword: val,
					},
				},
			},
		},
	}
}

// Search implements the domain.VectorStore interface.
func (c *Client) Search(ctx context.Context, collectionName string, vector []float32, identity *docpb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
	if collectionName == "" {
		collectionName = c.defaultCollection
	}

	conditions := buildConditions(identity, metadata)
	filter := &pb.Filter{
		Must: conditions,
	}

	req := &pb.SearchPoints{
		CollectionName: collectionName,
		Vector:         vector,
		Filter:         filter,
		Limit:          uint64(limit),
		WithPayload: &pb.WithPayloadSelector{
			SelectorOptions: &pb.WithPayloadSelector_Enable{
				Enable: true,
			},
		},
	}

	resp, err := c.qdrantClient.Search(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("action failed for job QdrantSearch: grpc call failed: %w", err)
	}

	var results []domain.SearchResult
	for _, point := range resp.Result {
		idStr, recordIdentity, resultMetadata, payloadStr := parsePointData(point.Id, point.Payload, identity)

		results = append(results, domain.SearchResult{
			Record: domain.CacheRecord{
				ID:          idStr,
				Identity:    recordIdentity,
				Metadata:    resultMetadata,
				Vector:      vector,
				JSONPayload: payloadStr,
			},
			Score: point.Score,
		})
	}

	return results, nil
}

// Upsert implements the domain.VectorStore interface.
func (c *Client) Upsert(ctx context.Context, collectionName string, record domain.CacheRecord) error {
	if collectionName == "" {
		collectionName = c.defaultCollection
	}

	// Create UUID string from hex: 8-4-4-4-12
	if len(record.ID) >= 32 {
		record.ID = fmt.Sprintf("%s-%s-%s-%s-%s",
			record.ID[0:8], record.ID[8:12], record.ID[12:16], record.ID[16:20], record.ID[20:32])
	}

	pointId := &pb.PointId{
		PointIdOptions: &pb.PointId_Uuid{
			Uuid: record.ID,
		},
	}

	payload := map[string]*pb.Value{
		"json_payload": {
			Kind: &pb.Value_StringValue{StringValue: record.JSONPayload},
		},
	}

	if record.Identity != nil {
		if record.Identity.TenantId != "" {
			payload["tenant_id"] = &pb.Value{Kind: &pb.Value_StringValue{StringValue: record.Identity.TenantId}}
		}
		if record.Identity.AppId != "" {
			payload["app_id"] = &pb.Value{Kind: &pb.Value_StringValue{StringValue: record.Identity.AppId}}
		}
		if record.Identity.JobId != "" {
			payload["job_id"] = &pb.Value{Kind: &pb.Value_StringValue{StringValue: record.Identity.JobId}}
		}
	}

	for k, v := range record.Metadata {
		if strVal, ok := v.(string); ok {
			payload[k] = &pb.Value{
				Kind: &pb.Value_StringValue{StringValue: strVal},
			}
		}
	}

	req := &pb.UpsertPoints{
		CollectionName: collectionName,
		Points: []*pb.PointStruct{
			{
				Id: pointId,
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vector{
						Vector: &pb.Vector{
							Vector: &pb.Vector_Dense{
								Dense: &pb.DenseVector{
									Data: record.Vector,
								},
							},
						},
					},
				},
				Payload: payload,
			},
		},
	}

	_, err := c.qdrantClient.Upsert(ctx, req)
	if err != nil {
		return fmt.Errorf("action failed for job QdrantUpsert: grpc call failed: %w", err)
	}

	return nil
}

// CheckMetadata implements the domain.VectorStore interface.
func (c *Client) CheckMetadata(ctx context.Context, collectionName string, identity *docpb.InfrastructureIdentity, metadata map[string]any) (bool, error) {
	if collectionName == "" {
		collectionName = c.defaultCollection
	}

	conditions := buildConditions(identity, metadata)
	if len(conditions) == 0 {
		return false, fmt.Errorf("no valid metadata or identity provided for CheckMetadata")
	}

	filter := &pb.Filter{
		Must: conditions,
	}

	req := &pb.ScrollPoints{
		CollectionName: collectionName,
		Filter:         filter,
		Limit:          func() *uint32 { v := uint32(1); return &v }(),
		WithPayload: &pb.WithPayloadSelector{
			SelectorOptions: &pb.WithPayloadSelector_Enable{
				Enable: false,
			},
		},
		WithVectors: &pb.WithVectorsSelector{
			SelectorOptions: &pb.WithVectorsSelector_Enable{
				Enable: false,
			},
		},
	}

	resp, err := c.qdrantClient.Scroll(ctx, req)
	if err != nil {
		return false, fmt.Errorf("action failed for job QdrantScroll: grpc call failed: %w", err)
	}

	if len(resp.Result) > 0 {
		return true, nil
	}

	return false, nil
}

// GetByMetadata implements the domain.VectorStore interface.
func (c *Client) GetByMetadata(ctx context.Context, collectionName string, identity *docpb.InfrastructureIdentity, metadata map[string]any) ([]domain.SearchResult, error) {
	if collectionName == "" {
		collectionName = c.defaultCollection
	}

	conditions := buildConditions(identity, metadata)
	if len(conditions) == 0 {
		return nil, fmt.Errorf("no valid metadata or identity provided for GetByMetadata")
	}

	filter := &pb.Filter{
		Must: conditions,
	}

	req := &pb.ScrollPoints{
		CollectionName: collectionName,
		Filter:         filter,
		Limit:          func() *uint32 { v := uint32(1); return &v }(),
		WithPayload: &pb.WithPayloadSelector{
			SelectorOptions: &pb.WithPayloadSelector_Enable{
				Enable: true,
			},
		},
		WithVectors: &pb.WithVectorsSelector{
			SelectorOptions: &pb.WithVectorsSelector_Enable{
				Enable: false,
			},
		},
	}

	resp, err := c.qdrantClient.Scroll(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("action failed for job QdrantGetByMetadata: grpc call failed: %w", err)
	}

	var results []domain.SearchResult
	for _, point := range resp.Result {
		idStr, recordIdentity, resultMetadata, payloadStr := parsePointData(point.Id, point.Payload, identity)

		results = append(results, domain.SearchResult{
			Record: domain.CacheRecord{
				ID:          idStr,
				Identity:    recordIdentity,
				Metadata:    resultMetadata,
				Vector:      nil, // No vector retrieved
				JSONPayload: payloadStr,
			},
			Score: 1.0, // Exact match metadata is treated as 100% confidence
		})
	}

	return results, nil
}

// compile-time check to ensure Client implements domain.VectorStore
var _ domain.VectorStore = (*Client)(nil)

func parsePointData(pointId *pb.PointId, payload map[string]*pb.Value, identity *docpb.InfrastructureIdentity) (string, *docpb.InfrastructureIdentity, map[string]any, string) {
	payloadStr := ""
	if payloadVal, ok := payload["json_payload"]; ok {
		payloadStr = payloadVal.GetStringValue()
	}

	resultMetadata := make(map[string]any)
	var recordIdentity *docpb.InfrastructureIdentity
	for k, v := range payload {
		if k == "tenant_id" || k == "app_id" || k == "job_id" {
			if recordIdentity == nil {
				recordIdentity = &docpb.InfrastructureIdentity{}
			}
			switch k {
			case "tenant_id":
				recordIdentity.TenantId = v.GetStringValue()
			case "app_id":
				recordIdentity.AppId = v.GetStringValue()
			case "job_id":
				recordIdentity.JobId = v.GetStringValue()
			}
		} else if k != "json_payload" {
			resultMetadata[k] = v.GetStringValue()
		}
	}
	if recordIdentity == nil {
		recordIdentity = identity
	}

	idStr := ""
	if pointId != nil {
		if uuid := pointId.GetUuid(); uuid != "" {
			idStr = uuid
		}
	}
	return idStr, recordIdentity, resultMetadata, payloadStr
}

package qdrant

import (
	"context"
	"fmt"

	"github.com/doc_processor/semantic_cache_service/internal/domain"
	pb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	qdrantClient pb.PointsClient
	defaultCollection string
	conn         *grpc.ClientConn
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

// Search implements the domain.VectorStore interface.
func (c *Client) Search(ctx context.Context, collectionName string, vector []float32, metadata map[string]interface{}, limit int) ([]domain.SearchResult, error) {
	if collectionName == "" {
		collectionName = c.defaultCollection
	}

	// Construct the metadata filters
	var conditions []*pb.Condition
	for k, v := range metadata {
		if strVal, ok := v.(string); ok {
			conditions = append(conditions, &pb.Condition{
				ConditionOneOf: &pb.Condition_Field{
					Field: &pb.FieldCondition{
						Key: k,
						Match: &pb.Match{
							MatchValue: &pb.Match_Keyword{
								Keyword: strVal,
							},
						},
					},
				},
			})
		}
		// In a real application, you'd map arrays or other JSON types into Qdrant Conditions.
		// For simplicity, we just handle strings, since that covers our immediate use case.
	}

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
		payloadStr := ""
		if payloadVal, ok := point.Payload["json_payload"]; ok {
			payloadStr = payloadVal.GetStringValue()
		}

		resultMetadata := make(map[string]interface{})
		for k, v := range point.Payload {
			if k != "json_payload" {
				resultMetadata[k] = v.GetStringValue() // we can decode other types from qdrant if needed
			}
		}

		// Extract ID
		idStr := ""
		if point.Id != nil {
			if uuid := point.Id.GetUuid(); uuid != "" {
				idStr = uuid
			}
		}

		results = append(results, domain.SearchResult{
			Record: domain.CacheRecord{
				ID:          idStr,
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

// compile-time check to ensure Client implements domain.VectorStore
var _ domain.VectorStore = (*Client)(nil)

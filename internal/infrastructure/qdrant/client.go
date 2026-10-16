package qdrant

import (
	"context"
	"fmt"
	"sort"
	"strings"

	docpb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/domain"
	pb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	qdrantClient      pb.PointsClient
	collectionsClient pb.CollectionsClient
	defaultCollection string
	conn              *grpc.ClientConn
}

// NewClient creates a new Qdrant client.
func NewClient(addr string, collection string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to qdrant: %w", err)
	}

	pointsClient := pb.NewPointsClient(conn)
	collectionsClient := pb.NewCollectionsClient(conn)

	return &Client{
		qdrantClient:      pointsClient,
		collectionsClient: collectionsClient,
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
		if strVal, ok := v.(string); ok && strings.TrimSpace(strVal) != "" {
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

func (c *Client) resolveCollection(collectionName string) string {
	if collectionName == "" {
		collectionName = c.defaultCollection
	}
	if collectionName == "" {
		panic("collectionName is required")
	}
	return collectionName
}

func formatPointID(id string) *pb.PointId {
	if len(id) >= 32 {
		id = fmt.Sprintf("%s-%s-%s-%s-%s",
			id[0:8], id[8:12], id[12:16], id[16:20], id[20:32])
	}
	return &pb.PointId{
		PointIdOptions: &pb.PointId_Uuid{
			Uuid: id,
		},
	}
}

// EnsureCollection provisions the Qdrant collection with named vectors, PQ, and payload indexes.
func (c *Client) EnsureCollection(ctx context.Context, collectionName string, denseDim uint64, pqCompression uint32) error {
	collectionName = c.resolveCollection(collectionName)
	if denseDim == 0 {
		denseDim = 3072
	}

	_, err := c.collectionsClient.Get(ctx, &pb.GetCollectionInfoRequest{CollectionName: collectionName})
	if err == nil {
		return nil
	}

	vectorsConfig := &pb.VectorsConfig{
		Config: &pb.VectorsConfig_ParamsMap{
			ParamsMap: &pb.VectorParamsMap{
				Map: map[string]*pb.VectorParams{
					"dense": {
						Size:     denseDim,
						Distance: pb.Distance_Cosine,
					},
				},
			},
		},
	}

	sparseConfig := &pb.SparseVectorConfig{
		Map: map[string]*pb.SparseVectorParams{
			"sparse": {},
		},
	}

	quantizationConfig := &pb.QuantizationConfig{
		Quantization: &pb.QuantizationConfig_Product{
			Product: &pb.ProductQuantization{
				Compression: pb.CompressionRatio_x4,
                Memory: pb.Memory_Cached.Enum(),
			},
		},
	}

	createReq := &pb.CreateCollection{
		CollectionName:      collectionName,
		VectorsConfig:       vectorsConfig,
		SparseVectorsConfig: sparseConfig,
		QuantizationConfig:  quantizationConfig,
	}

	_, createErr := c.collectionsClient.Create(ctx, createReq)
	if createErr != nil {
		return fmt.Errorf("failed to create collection %s: %w", collectionName, createErr)
	}

	indexedFields := []string{"tenant_id", "app_id", "doc_type", "section_title"}
	fieldType := pb.FieldType_FieldTypeKeyword
	for _, field := range indexedFields {
		_, _ = c.qdrantClient.CreateFieldIndex(ctx, &pb.CreateFieldIndexCollection{
			CollectionName: collectionName,
			FieldName:      field,
			FieldType:      &fieldType,
		})
	}


	return nil
}

// UpsertHybrid upserts a CacheRecord with dual dense + sparse vectors.
func (c *Client) UpsertHybrid(ctx context.Context, collectionName string, record domain.CacheRecord) error {
	collectionName = c.resolveCollection(collectionName)
	pointId := formatPointID(record.ID)

	payload := map[string]*pb.Value{}
	if record.JSONPayload != "" {
		payload["json_payload"] = &pb.Value{Kind: &pb.Value_StringValue{StringValue: record.JSONPayload}}
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
			payload[k] = &pb.Value{Kind: &pb.Value_StringValue{StringValue: strVal}}
		}
	}

	namedVectors := map[string]*pb.Vector{}
	if len(record.Vector) > 0 {
		namedVectors["dense"] = pb.NewVectorDense(record.Vector)
	}
	if record.SparseVec != nil && len(record.SparseVec.Indices) > 0 {
		namedVectors["sparse"] = pb.NewVectorSparse(record.SparseVec.Indices, record.SparseVec.Values)
	}

	req := &pb.UpsertPoints{
		CollectionName: collectionName,
		Points: []*pb.PointStruct{
			{
				Id: pointId,
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vectors{
						Vectors: &pb.NamedVectors{
							Vectors: namedVectors,
						},
					},
				},
				Payload: payload,
			},
		},
	}

	_, err := c.qdrantClient.Upsert(ctx, req)
	if err != nil {
		return fmt.Errorf("action failed for job QdrantUpsertHybrid: grpc call failed: %w", err)
	}

	return nil
}

// HybridSearch executes convex combination search over dense and sparse named vectors.
func (c *Client) HybridSearch(ctx context.Context, collectionName string, denseVector []float32, sparseVector *domain.SparseVector, identity *docpb.InfrastructureIdentity, metadata map[string]any, alpha float32, limit int) ([]domain.SearchResult, error) {
	collectionName = c.resolveCollection(collectionName)
	if alpha <= 0 {
		alpha = 0.7
	}
	if limit <= 0 {
		limit = 10
	}

	conditions := buildConditions(identity, metadata)
	filter := &pb.Filter{
		Must: conditions,
	}

	type matchInfo struct {
		result      domain.SearchResult
		denseScore  float32
		sparseScore float32
	}
	merged := make(map[string]*matchInfo)

	denseName := "dense"
	if len(denseVector) > 0 {
		denseReq := &pb.SearchPoints{
			CollectionName: collectionName,
			Vector:         denseVector,
			VectorName:     &denseName,
			Filter:         filter,
			Limit:          uint64(limit),
			WithPayload:    &pb.WithPayloadSelector{SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true}},
		}
		denseResp, err := c.qdrantClient.Search(ctx, denseReq)
		if err == nil && denseResp != nil {
			for _, pt := range denseResp.Result {
				idStr, recId, meta, payload := parsePointData(pt.Id, pt.Payload, identity)
				merged[idStr] = &matchInfo{
					result: domain.SearchResult{
						Record: domain.CacheRecord{
							ID:          idStr,
							Identity:    recId,
							Metadata:    meta,
							Vector:      denseVector,
							JSONPayload: payload,
						},
					},
					denseScore: pt.Score,
				}
			}
		}
	}

	sparseName := "sparse"
	if sparseVector != nil && len(sparseVector.Indices) > 0 {
		sparseReq := &pb.SearchPoints{
			CollectionName: collectionName,
			Vector:         sparseVector.Values,
			SparseIndices:  &pb.SparseIndices{Data: sparseVector.Indices},
			VectorName:     &sparseName,
			Filter:         filter,
			Limit:          uint64(limit),
			WithPayload:    &pb.WithPayloadSelector{SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true}},
		}
		sparseResp, err := c.qdrantClient.Search(ctx, sparseReq)
		if err == nil && sparseResp != nil {
			for _, pt := range sparseResp.Result {
				idStr, recId, meta, payload := parsePointData(pt.Id, pt.Payload, identity)
				if existing, exists := merged[idStr]; exists {
					existing.sparseScore = pt.Score
				} else {
					merged[idStr] = &matchInfo{
						result: domain.SearchResult{
							Record: domain.CacheRecord{
								ID:          idStr,
								Identity:    recId,
								Metadata:    meta,
								JSONPayload: payload,
							},
						},
						sparseScore: pt.Score,
					}
				}
			}
		}
	}

	var results []domain.SearchResult
	for _, info := range merged {
		combinedScore := (alpha * info.denseScore) + ((1.0 - alpha) * info.sparseScore)
		res := info.result
		res.Score = combinedScore
		res.DenseScore = info.denseScore
		res.SparseScore = info.sparseScore
		results = append(results, res)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if len(results) > limit {
		results = results[:limit]
	}

	return results, nil
}

// Search implements the legacy domain.VectorStore interface.
func (c *Client) Search(ctx context.Context, collectionName string, vector []float32, identity *docpb.InfrastructureIdentity, metadata map[string]any, limit int) ([]domain.SearchResult, error) {
	collectionName = c.resolveCollection(collectionName)

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

// Upsert implements the legacy domain.VectorStore interface.
func (c *Client) Upsert(ctx context.Context, collectionName string, record domain.CacheRecord) error {
	collectionName = c.resolveCollection(collectionName)
	pointId := formatPointID(record.ID)

	payload := map[string]*pb.Value{}
	if record.JSONPayload != "" {
		payload["json_payload"] = &pb.Value{
			Kind: &pb.Value_StringValue{StringValue: record.JSONPayload},
		}
	}

	if record.Identity != nil {
		if record.Identity.TenantId != "" {
			payload["tenant_id"] = &pb.Value{Kind: &pb.Value_StringValue{StringValue: record.Identity.TenantId}}
		}
		if record.Identity.AppId != "" {
			payload["app_id"] = &pb.Value{Kind: &pb.Value_StringValue{StringValue: record.Identity.AppId}}
		}
	}

	for k, v := range record.Metadata {
		switch k {
		case "json_payload", "email_text", "raw_email", "inference_input", "inbound_email_text", "EmailBody", "job_id":
			continue
		}
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
	collectionName = c.resolveCollection(collectionName)

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
	collectionName = c.resolveCollection(collectionName)

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
				Vector:      nil,
				JSONPayload: payloadStr,
			},
			Score: 1.0,
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

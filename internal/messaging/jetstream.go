package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

type JetStreamHandler struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	app *application.SemanticCacheApp
}

func NewJetStreamHandler(natsURL string, app *application.SemanticCacheApp) (*JetStreamHandler, error) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	return &JetStreamHandler{
		nc:  nc,
		js:  js,
		app: app,
	}, nil
}

func (h *JetStreamHandler) StartConsumers(ctx context.Context) error {
	maxAckPending := 1000
	if envVal := os.Getenv("NATS_MAX_ACK_PENDING"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			maxAckPending = parsed
		}
	}

	pullMaxMessages := 100
	if envVal := os.Getenv("NATS_PULL_MAX_MESSAGES"); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			pullMaxMessages = parsed
		}
	}

	cons, err := h.js.CreateOrUpdateConsumer(ctx, "WORKERS", jetstream.ConsumerConfig{
		Durable:        "semantic_cache_service_consumer",
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        60 * time.Second,
		FilterSubjects: []string{"worker.cache.lookup", "worker.cache.background.update"},
		MaxAckPending:  maxAckPending,
	})
	if err != nil {
		return fmt.Errorf("failed to create cache consumer: %w", err)
	}

	logger.Info("SemanticCache", "%v", "Listening for Cache Requests on NATS JetStream (WORKERS stream)...")
	_, err = cons.Consume(func(msg jetstream.Msg) {
		go h.handleMessage(msg)
	}, jetstream.PullMaxMessages(pullMaxMessages))
	if err != nil {
		return fmt.Errorf("failed to consume cache requests: %w", err)
	}

	return nil
}

func (h *JetStreamHandler) handleMessage(msg jetstream.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	subject := msg.Subject()

	meta, err := msg.Metadata()
	if err == nil && meta.NumDelivered > 5 {
		logger.Info("SemanticCache", "Message exceeded max delivery attempts, terminating")
		h.sendErrorReply(ctx, msg, "Action failed after multiple retries due to rate limits.")
		msg.Term()
		return
	}

	// InProgress keep-alive loop
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case <-ticker.C:
				_ = msg.InProgress()
			case <-done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	logger.Info("SemanticCache", "Received action request on subject: %s", subject)

	req := &pb.ExecuteWorkflowNodeRequest{}
	if err := proto.Unmarshal(msg.Data(), req); err != nil {
		logger.Info("SemanticCache", "Error unmarshaling request: %v", err)
		h.sendErrorReply(ctx, msg, fmt.Sprintf("invalid payload: %v", err))
		msg.NakWithDelay(15 * time.Second)
		return
	}

	if req.NodeConfig == nil || req.NodeConfig.Cache == nil {
		h.sendErrorReply(ctx, msg, "missing NodeConfig or Cache config")
		msg.Ack()
		return
	}

	cacheConfig := req.NodeConfig.Cache
	metadata := make(map[string]any)
	for k, v := range cacheConfig.Metadata {
		metadata[k] = v
	}
	if req.Identity != nil {
		metadata["tenant_id"] = req.Identity.TenantId
		metadata["app_id"] = req.Identity.AppId
		metadata["job_id"] = req.Identity.JobId
	}

	if subject == "worker.cache.lookup" {
		// Cache Lookup
		hit, payload, confidence, err := h.app.CheckCache(ctx, cacheConfig.CollectionName, cacheConfig.LookupInput, metadata, cacheConfig.ConfidenceThreshold)
		if err != nil {
			logger.Error("SemanticCache", "CheckCache failed: %v", err)
			h.sendErrorReply(ctx, msg, fmt.Sprintf("CheckCache failed: %v", err))
			msg.NakWithDelay(15 * time.Second)
			return
		}

		resultMap := map[string]interface{}{
			"semanticCacheResult": hitStr(hit),
			"confidence":          confidence,
			"matchedPayload":      payload,
		}
		resultBytes, _ := json.Marshal(resultMap)

		h.sendSuccessReply(ctx, msg, string(resultBytes))
		msg.Ack()

	} else if subject == "worker.cache.background.update" {
		// Cache Update
		err := h.app.StoreExtraction(ctx, cacheConfig.CollectionName, cacheConfig.LookupInput, metadata, "blank_document")
		if err != nil {
			logger.Error("SemanticCache", "StoreExtraction failed: %v", err)
			h.sendErrorReply(ctx, msg, fmt.Sprintf("StoreExtraction failed: %v", err))
			msg.NakWithDelay(15 * time.Second)
			return
		}

		resultMap := map[string]interface{}{
			"semanticCacheResult": "updated",
		}
		resultBytes, _ := json.Marshal(resultMap)

		h.sendSuccessReply(ctx, msg, string(resultBytes))
		msg.Ack()
	} else {
		logger.Info("SemanticCache", "WARN: Unknown cache request subject: %s", subject)
		msg.Ack()
	}
}

func hitStr(hit bool) string {
	if hit {
		return "hit"
	}
	return "miss"
}

func (h *JetStreamHandler) sendErrorReply(ctx context.Context, msg jetstream.Msg, errorMsg string) {
	replySubj := ""
	if headers := msg.Headers(); headers != nil {
		replySubj = headers.Get("Reply-Subject")
	}
	if replySubj == "" {
		return
	}

	resp := &pb.PerformActionResponse{
		Success: false,
		ErrorContract: &pb.ErrorContract{
			Code:    "CACHE_ERROR",
			Message: errorMsg,
		},
	}
	h.publishReply(ctx, replySubj, resp)
}

func (h *JetStreamHandler) sendSuccessReply(ctx context.Context, msg jetstream.Msg, resultJson string) {
	replySubj := ""
	if headers := msg.Headers(); headers != nil {
		replySubj = headers.Get("Reply-Subject")
	}
	if replySubj == "" {
		return
	}

	resp := &pb.PerformActionResponse{
		Success:          true,
		ActionResultJson: resultJson,
	}
	h.publishReply(ctx, replySubj, resp)
}

func (h *JetStreamHandler) publishReply(ctx context.Context, subj string, resp *pb.PerformActionResponse) {
	data, err := proto.Marshal(resp)
	if err != nil {
		logger.Error("SemanticCache", "Failed to marshal reply: %v", err)
		return
	}
	_, err = h.js.Publish(ctx, subj, data)
	if err != nil {
		logger.Error("SemanticCache", "Failed to publish reply to %s: %v", subj, err)
	}
}

func (h *JetStreamHandler) Close() {
	if h.nc != nil {
		h.nc.Close()
	}
}

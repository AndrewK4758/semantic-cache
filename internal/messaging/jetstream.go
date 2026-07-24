package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/AndrewK4758/shared_utils/envutil"
	"strconv"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/AndrewK4758/shared_utils/logger"
	"github.com/AndrewK4758/shared_utils/natsutil"
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
	nc, err := natsutil.ConnectWithRetries(natsURL, "SemanticCache")
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
	if envVal := envutil.GetStringOrDefault("NATS_MAX_ACK_PENDING", ""); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			maxAckPending = parsed
		}
	}

	pullMaxMessages := 100
	if envVal := envutil.GetStringOrDefault("NATS_PULL_MAX_MESSAGES", ""); envVal != "" {
		if parsed, err := strconv.Atoi(envVal); err == nil {
			pullMaxMessages = parsed
		}
	}

	var cons jetstream.Consumer
	var err error
	for i := 0; i < 30; i++ {
		cons, err = h.js.CreateOrUpdateConsumer(ctx, "WORKERS", jetstream.ConsumerConfig{
			Durable:        "semantic_cache_service_consumer",
			AckPolicy:      jetstream.AckExplicitPolicy,
			AckWait:        60 * time.Second,
			FilterSubjects: []string{"worker.cache.lookup", "worker.cache.background.update"},
			MaxAckPending:  maxAckPending,
		})
		if err == nil {
			break
		}
		logger.Info("SemanticCache", "Waiting for WORKERS stream... (%v)", err)
		time.Sleep(2 * time.Second)
	}
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
	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	subject := msg.Subject()

	meta, err := msg.Metadata()
	var attempt uint64 = 1
	if meta != nil {
		attempt = meta.NumDelivered
	}

	t, a, j := "default", "default", "default"
	req := &pb.ExecuteWorkflowNodeRequest{}
	if unmarshalErr := proto.Unmarshal(msg.Data(), req); unmarshalErr == nil && req.Identity != nil {
		t, a, j = natsutil.ExtractIdentity(req.Identity)
	}

	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component: "SemanticCache",
		Event:     "DEQUEUED",
		Stream:    "WORKERS",
		Subject:   subject,
		Consumer:  "semantic_cache_service_consumer",
		TenantID:  t,
		AppID:     a,
		JobID:     j,
		SizeBytes: len(msg.Data()),
		Attempt:   attempt,
	})

	if err == nil && meta.NumDelivered > 5 {
		logger.Info("SemanticCache", "Message exceeded max delivery attempts, terminating")
		h.sendErrorReply(ctx, msg, "Action failed after multiple retries due to rate limits.", req.Identity)
		natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
			Component:  "SemanticCache",
			Event:      "TERMINATED",
			Stream:     "WORKERS",
			Subject:    subject,
			Consumer:   "semantic_cache_service_consumer",
			TenantID:   t,
			AppID:      a,
			JobID:      j,
			Attempt:    attempt,
			DurationMs: time.Since(startTime).Milliseconds(),
			Error:      fmt.Errorf("exceeded max delivery attempts (5)"),
		})
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
				if err := msg.InProgress(); err == nil {
					natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
						Component: "SemanticCache",
						Event:     "IN_PROGRESS",
						Stream:    "WORKERS",
						Subject:   subject,
						Consumer:  "semantic_cache_service_consumer",
						TenantID:  t,
						AppID:     a,
						JobID:     j,
						Attempt:   attempt,
					})
				}
			case <-done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	logger.Info("SemanticCache", "Received action request on subject: %s", subject)

	if req.NodeConfig == nil || req.NodeConfig.Cache == nil {
		h.sendErrorReply(ctx, msg, "missing NodeConfig or Cache config", req.Identity)
		durationMs := time.Since(startTime).Milliseconds()
		msg.Ack()
		natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
			Component:  "SemanticCache",
			Event:      "ACKNOWLEDGED",
			Stream:     "WORKERS",
			Subject:    subject,
			Consumer:   "semantic_cache_service_consumer",
			TenantID:   t,
			AppID:      a,
			JobID:      j,
			Attempt:    attempt,
			DurationMs: durationMs,
		})
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
			h.sendErrorReply(ctx, msg, fmt.Sprintf("CheckCache failed: %v", err), req.Identity)
			natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
				Component:  "SemanticCache",
				Event:      "NACKED",
				Stream:     "WORKERS",
				Subject:    subject,
				Consumer:   "semantic_cache_service_consumer",
				TenantID:   t,
				AppID:      a,
				JobID:      j,
				Attempt:    attempt,
				DurationMs: time.Since(startTime).Milliseconds(),
				Error:      err,
			})
			msg.NakWithDelay(15 * time.Second)
			return
		}

		resultMap := map[string]interface{}{
			"semanticCacheResult": hitStr(hit),
			"confidence":          confidence,
			"matchedPayload":      payload,
		}
		resultBytes, _ := json.Marshal(resultMap)

		h.sendSuccessReply(ctx, msg, string(resultBytes), req.Identity)
		durationMs := time.Since(startTime).Milliseconds()
		msg.Ack()
		natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
			Component:  "SemanticCache",
			Event:      "ACKNOWLEDGED",
			Stream:     "WORKERS",
			Subject:    subject,
			Consumer:   "semantic_cache_service_consumer",
			TenantID:   t,
			AppID:      a,
			JobID:      j,
			Attempt:    attempt,
			DurationMs: durationMs,
		})

	} else if subject == "worker.cache.background.update" {
		// Cache Update
		err := h.app.StoreExtraction(ctx, cacheConfig.CollectionName, cacheConfig.LookupInput, metadata, "blank_document")
		if err != nil {
			logger.Error("SemanticCache", "StoreExtraction failed: %v", err)
			h.sendErrorReply(ctx, msg, fmt.Sprintf("StoreExtraction failed: %v", err), req.Identity)
			natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
				Component:  "SemanticCache",
				Event:      "NACKED",
				Stream:     "WORKERS",
				Subject:    subject,
				Consumer:   "semantic_cache_service_consumer",
				TenantID:   t,
				AppID:      a,
				JobID:      j,
				Attempt:    attempt,
				DurationMs: time.Since(startTime).Milliseconds(),
				Error:      err,
			})
			msg.NakWithDelay(15 * time.Second)
			return
		}

		resultMap := map[string]interface{}{
			"semanticCacheResult": "updated",
		}
		resultBytes, _ := json.Marshal(resultMap)

		h.sendSuccessReply(ctx, msg, string(resultBytes), req.Identity)
		durationMs := time.Since(startTime).Milliseconds()
		msg.Ack()
		natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
			Component:  "SemanticCache",
			Event:      "ACKNOWLEDGED",
			Stream:     "WORKERS",
			Subject:    subject,
			Consumer:   "semantic_cache_service_consumer",
			TenantID:   t,
			AppID:      a,
			JobID:      j,
			Attempt:    attempt,
			DurationMs: durationMs,
		})
	} else {
		logger.Info("SemanticCache", "WARN: Unknown cache request subject: %s", subject)
		durationMs := time.Since(startTime).Milliseconds()
		msg.Ack()
		natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
			Component:  "SemanticCache",
			Event:      "ACKNOWLEDGED",
			Stream:     "WORKERS",
			Subject:    subject,
			Consumer:   "semantic_cache_service_consumer",
			TenantID:   t,
			AppID:      a,
			JobID:      j,
			Attempt:    attempt,
			DurationMs: durationMs,
		})
	}
}

func hitStr(hit bool) string {
	if hit {
		return "hit"
	}
	return "miss"
}

func (h *JetStreamHandler) sendErrorReply(ctx context.Context, msg jetstream.Msg, errorMsg string, reqIdentity *pb.InfrastructureIdentity) {
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
	h.publishReply(ctx, replySubj, resp, reqIdentity)
}

func (h *JetStreamHandler) sendSuccessReply(ctx context.Context, msg jetstream.Msg, resultJson string, reqIdentity *pb.InfrastructureIdentity) {
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
	h.publishReply(ctx, replySubj, resp, reqIdentity)
}

func (h *JetStreamHandler) publishReply(ctx context.Context, subj string, resp *pb.PerformActionResponse, reqIdentity *pb.InfrastructureIdentity) {
	data, err := proto.Marshal(resp)
	if err != nil {
		logger.Error("SemanticCache", "Failed to marshal reply: %v", err)
		return
	}
	_, pubErr := h.js.Publish(ctx, subj, data)
	t, a, j := natsutil.ExtractIdentity(reqIdentity)
	natsutil.LogJetStreamEvent(natsutil.JetStreamLogEvent{
		Component: "SemanticCache",
		Event:     "ENQUEUED",
		Stream:    "ORCHESTRATOR_RESPONSES",
		Subject:   subj,
		TenantID:  t,
		AppID:     a,
		JobID:     j,
		SizeBytes: len(data),
		Error:     pubErr,
	})
	if pubErr != nil {
		logger.Error("SemanticCache", "Failed to publish reply to %s: %v", subj, pubErr)
	}
}

func (h *JetStreamHandler) Close() {
	if h.nc != nil {
		h.nc.Close()
	}
}

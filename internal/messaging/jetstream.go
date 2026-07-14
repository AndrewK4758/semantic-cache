package messaging

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/encoding/protojson"
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

	// Ensure Cache Streams exist
	ctx := context.Background()
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "CACHE_REQUESTS",
		Subjects: []string{"cache.requests.>"},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create cache requests stream: %w", err)
	}

	return &JetStreamHandler{
		nc:  nc,
		js:  js,
		app: app,
	}, nil
}

func (h *JetStreamHandler) StartConsumers(ctx context.Context) error {
	cons, err := h.js.CreateOrUpdateConsumer(ctx, "CACHE_REQUESTS", jetstream.ConsumerConfig{
		Durable:       "semantic_cache_service_consumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		FilterSubject: "cache.requests.>",
	})
	if err != nil {
		return fmt.Errorf("failed to create cache consumer: %w", err)
	}

	log.Println("Listening for Cache Requests on NATS JetStream...")
	_, err = cons.Consume(func(msg jetstream.Msg) {
		go h.handleMessage(msg)
	})
	if err != nil {
		return fmt.Errorf("failed to consume cache requests: %w", err)
	}

	return nil
}

func (h *JetStreamHandler) handleMessage(msg jetstream.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subject := msg.Subject()

	if subject == "cache.requests.store" {
		var reqMsg pb.CacheStoreMessage
		if err := protojson.Unmarshal(msg.Data(), &reqMsg); err != nil {
			log.Printf("ERROR: Failed to unmarshal CacheStoreMessage: %v", err)
			_ = msg.Nak()
			return
		}

		metadata := make(map[string]interface{})
		if reqMsg.Request != nil && reqMsg.Request.Metadata != nil {
			for k, v := range reqMsg.Request.Metadata.Fields {
				metadata[k] = v.AsInterface()
			}
		}

		_ = h.app.StoreExtraction(ctx, reqMsg.Request.CollectionName, reqMsg.Request.Text, metadata, reqMsg.Request.ExtractedPayload)
		// Writes are fire-and-forget; no response message needed on JetStream
		_ = msg.Ack()

	} else {
		log.Printf("WARN: Unknown cache request subject: %s", subject)
		_ = msg.Ack()
	}
}

func (h *JetStreamHandler) Close() {
	if h.nc != nil {
		h.nc.Close()
	}
}

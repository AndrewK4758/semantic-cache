package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/AndrewK4758/shared_utils/logger"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/doc_processor/semantic_cache_service/internal/infrastructure/openai"
	"github.com/doc_processor/semantic_cache_service/internal/infrastructure/qdrant"
	"github.com/doc_processor/semantic_cache_service/internal/messaging"
	grpchandler "github.com/doc_processor/semantic_cache_service/internal/presentation/grpc"
	"github.com/joho/godotenv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	logger.InitLogger()
	// Attempt to load .env, but ignore error if it doesn't exist
	_ = godotenv.Load()

	// Configuration
	grpcPort := getEnv("SERVER_PORT", "50055")
	qdrantAddr := getEnv("QDRANT_URL", "localhost:6334")
	openaiURL := getEnv("OPENAI_BASE_URL", "http://localhost:11434/v1")
	openaiModel := getEnv("OPENAI_EMBEDDING_MODEL", "all-minilm:latest")
	collectionName := getEnv("QDRANT_COLLECTION", "incoming_email_templates")

	// Infrastructure
	openaiClient := openai.NewClient(openaiURL, openaiModel)

	qdrantClient, err := qdrant.NewClient(qdrantAddr, collectionName)
	if err != nil {
		logger.Fatal("SemanticCache", "Failed to initialize Qdrant client: %v", err)
	}
	defer qdrantClient.Close()

	// Application
	app := application.NewSemanticCacheApp(openaiClient, qdrantClient)

	// Presentation (gRPC)
	handler := grpchandler.NewSemanticCacheHandler(app)

	// Messaging (JetStream)
	natsURL := getEnv("NATS_URL", "nats://localhost:4222")
	jsHandler, err := messaging.NewJetStreamHandler(natsURL, app)
	if err != nil {
		logger.Fatal("SemanticCache", "Failed to initialize JetStream handler: %v", err)
	}
	defer jsHandler.Close()

	if err := jsHandler.StartConsumers(context.Background()); err != nil {
		logger.Fatal("SemanticCache", "Failed to start JetStream consumers: %v", err)
	}

	// gRPC Server Setup
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		logger.Fatal("SemanticCache", "Failed to listen on port %s: %v", grpcPort, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterSemanticCacheServiceServer(grpcServer, handler)
	reflection.Register(grpcServer)

	// Graceful Shutdown
	go func() {
		logger.Info("SemanticCache", "Semantic Cache Service listening on port %s", grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("SemanticCache", "Failed to serve gRPC server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	logger.Info("SemanticCache", "%v", "Shutting down Semantic Cache Service gracefully...")
	grpcServer.GracefulStop()
	logger.Info("SemanticCache", "%v", "Shutdown complete.")
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

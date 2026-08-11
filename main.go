package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/AndrewK4758/shared_utils/envutil"
	"github.com/AndrewK4758/shared_utils/logger"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/doc_processor/semantic_cache_service/internal/infrastructure/openai"
	"github.com/doc_processor/semantic_cache_service/internal/infrastructure/qdrant"
	"github.com/doc_processor/semantic_cache_service/internal/messaging"
	grpchandler "github.com/doc_processor/semantic_cache_service/internal/presentation/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Attempt to load .env but ignore error if it doesn't exist
	envutil.LoadEnv(".env", "semantic_cache_service/.env", "../.env", "../../.env")
	logger.InitLogger()

	// Configuration
	grpcPort := envutil.MustGetString("GRPC_PORT")
	qdrantAddr := envutil.MustGetString("QDRANT_URL")
	openaiURL := envutil.MustGetString("OPENAI_BASE_URL")
	openaiModel := envutil.MustGetEmbeddingModel()
	collectionName := envutil.MustGetString("QDRANT_COLLECTION")

	// Infrastructure
	openaiClient := openai.NewClient(openaiURL, openaiModel)

	qdrantClient, err := qdrant.NewClient(qdrantAddr, collectionName)
	if err != nil {
		logger.Error("SemanticCache", "Failed to initialize Qdrant client: %v", err)
		panic(fmt.Errorf("failed to initialize Qdrant client: %w", err))
	}
	defer qdrantClient.Close()

	// Application
	app := application.NewSemanticCacheApp(openaiClient, qdrantClient)

	// Presentation (gRPC)
	handler := grpchandler.NewSemanticCacheHandler(app)

	// Messaging (JetStream)
	natsURL := envutil.MustGetString("NATS_URL")
	jsHandler, err := messaging.NewJetStreamHandler(natsURL, app)
	if err != nil {
		logger.Error("SemanticCache", "Failed to initialize JetStream handler: %v", err)
		panic(fmt.Errorf("failed to initialize JetStream handler: %w", err))
	}
	defer jsHandler.Close()

	if err := jsHandler.StartConsumers(context.Background()); err != nil {
		logger.Error("SemanticCache", "Failed to start JetStream consumers: %v", err)
		panic(fmt.Errorf("failed to start JetStream consumers: %w", err))
	}

	// gRPC Server Setup
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		logger.Error("SemanticCache", "Failed to listen on port %s: %v", grpcPort, err)
		panic(fmt.Errorf("failed to listen on port %s: %w", grpcPort, err))
	}

	grpcServer := grpc.NewServer()
	pb.RegisterSemanticCacheServiceServer(grpcServer, handler)
	reflection.Register(grpcServer)

	// Graceful Shutdown
	go func() {
		logger.Info("SemanticCache", "Semantic Cache Service listening on port %s", grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("SemanticCache", "Failed to serve gRPC server: %v", err)
			panic(fmt.Errorf("failed to serve gRPC server: %w", err))
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	logger.Info("SemanticCache", "%v", "Shutting down Semantic Cache Service gracefully...")
	grpcServer.GracefulStop()
	logger.Info("SemanticCache", "%v", "Shutdown complete.")
}

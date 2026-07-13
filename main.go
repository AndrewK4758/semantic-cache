package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/AndrewK4758/shared_protos"
	"github.com/doc_processor/semantic_cache_service/internal/application"
	"github.com/doc_processor/semantic_cache_service/internal/infrastructure/openai"
	"github.com/doc_processor/semantic_cache_service/internal/infrastructure/qdrant"
	grpc_handler "github.com/doc_processor/semantic_cache_service/internal/presentation/grpc"
	"github.com/joho/godotenv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Attempt to load .env, but ignore error if it doesn't exist
	_ = godotenv.Load()

	// Configuration
	grpcPort := getEnv("SERVER_PORT", "50055")
	qdrantAddr := getEnv("QDRANT_URL", "localhost:6334")
	openaiURL := getEnv("OPENAI_BASE_URL", "http://localhost:11434/v1")
	openaiModel := getEnv("OPENAI_EMBEDDING_MODEL", "nomic-embed-text")
	collectionName := getEnv("QDRANT_COLLECTION", "document_chunks")

	// Infrastructure
	openaiClient := openai.NewClient(openaiURL, openaiModel)

	qdrantClient, err := qdrant.NewClient(qdrantAddr, collectionName)
	if err != nil {
		log.Fatalf("Failed to initialize Qdrant client: %v", err)
	}
	defer qdrantClient.Close()

	// Application
	app := application.NewSemanticCacheApp(openaiClient, qdrantClient)

	// Presentation
	handler := grpc_handler.NewSemanticCacheHandler(app)

	// gRPC Server Setup
	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", grpcPort, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterSemanticCacheServiceServer(grpcServer, handler)
	reflection.Register(grpcServer)

	// Graceful Shutdown
	go func() {
		log.Printf("Semantic Cache Service listening on port %s", grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down Semantic Cache Service gracefully...")
	grpcServer.GracefulStop()
	log.Println("Shutdown complete.")
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

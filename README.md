# Semantic Cache Service

The Semantic Cache Service is a standalone microservice responsible for generating and storing mathematical embeddings of document texts. By caching these semantic signatures, the upstream Go Orchestrator can bypass expensive and slow LLM generations when encountering semantically identical or highly similar documents.

This service is engineered strictly adhering to **Clean Architecture**, completely decoupling the application logic from the underlying storage (Qdrant) and generation (Ollama) infrastructure.

## 🏗 Architecture Layers

- **Domain:** Defines the core `EmbeddingService` and `VectorStore` abstractions. Zero external dependencies.
- **Application:** Wires the interfaces together to implement `CheckCache` and `StoreExtraction` use cases. Enforces the similarity thresholds mathematically.
- **Infrastructure:**
  - `qdrant`: Implements `VectorStore` via gRPC/REST Cosine Similarity search with strict metadata filtering.
  - `ollama`: Implements `EmbeddingService` via HTTP POST to the local `/api/embeddings` endpoint.
- **Presentation:** Exposes the Application layer to the Orchestrator via gRPC (`SemanticCacheService`).

## 🚀 Running the Semantic Cache Service

Because this service is a module within the larger `doc_processor_service` workspace, it must be run with the correct environment dependencies (Qdrant and Ollama).

### Docker (All Platforms - Recommended)
The fastest way to spin up the entire ecosystem, including the Semantic Cache Service, Qdrant, and Ollama, is via the root Docker Compose file.
```bash
cd /home/ak/projects/doc_processor_service
docker-compose up -d --build
```
The Semantic Cache Service will be available on **port 50053**.

### Local Development (Linux / macOS)
Ensure Qdrant is running locally on port 6334 and Ollama is available on 11434.
```bash
cd /home/ak/projects/doc_processor_service/semantic_cache_service

export GRPC_PORT=50053
export QDRANT_ADDR=localhost:6334
export OLLAMA_URL=http://localhost:11434
export OLLAMA_MODEL=all-minilm:l6-v2
export QDRANT_COLLECTION=semantic_cache

go run cmd/server/main.go
```

### Local Development (Windows PowerShell)
Ensure Qdrant and Ollama are running.
```powershell
cd \home\ak\projects\doc_processor_service\semantic_cache_service

$env:GRPC_PORT="50053"
$env:QDRANT_ADDR="localhost:6334"
$env:OLLAMA_URL="http://localhost:11434"
$env:OLLAMA_MODEL="nomic-embed-text"
$env:QDRANT_COLLECTION="semantic_cache"

go run cmd\server\main.go
```

## 🧪 Testing

To run the unit tests and ensure clean architecture boundaries are respected:
```bash
cd /home/ak/projects/doc_processor_service/semantic_cache_service
go test ./... -v
```

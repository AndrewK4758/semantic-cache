# Semantic Cache Service

The Semantic Cache Service is a standalone microservice responsible for generating and storing mathematical embeddings of document texts. By caching these semantic signatures, the upstream Go Orchestrator can bypass expensive and slow LLM generations when encountering semantically identical or highly similar documents.

This service is engineered strictly adhering to **Clean Architecture**, completely decoupling the application logic from the underlying storage (Qdrant) and generation (OpenAI spec) infrastructure.

## 🏗 Architecture Layers

- **Domain:** Defines the core `EmbeddingService` and `VectorStore` abstractions. Zero external dependencies.
- **Application:** Wires the interfaces together to implement `CheckCache` and `StoreExtraction` use cases. Enforces the similarity thresholds mathematically.
- **Infrastructure:**
  - `qdrant`: Implements `VectorStore` via gRPC/REST Cosine Similarity search with strict metadata filtering.
  - `openai`: Implements `EmbeddingService` via HTTP POST to an OpenAI-compatible `/v1/embeddings` endpoint.
- **Presentation:** Exposes the Application layer to the Orchestrator via gRPC (`SemanticCacheService`).

## 🚀 Running the Semantic Cache Service

Because this service is a module within the larger `doc_processor_service` workspace, it must be run with the correct environment dependencies (Qdrant and an OpenAI-compatible server like Ollama or vLLM).

### Docker (All Platforms - Recommended)
The fastest way to spin up the entire ecosystem, including the Semantic Cache Service, Qdrant, and Ollama, is via the root Docker Compose file.
```bash
cd /home/ak/projects/doc_processor_service
docker-compose up -d --build
```
The Semantic Cache Service will be available on **port 50055**.

### Local Development (Linux / macOS)
Ensure Qdrant is running locally on port 6334 and your AI engine is available.
```bash
cd /home/ak/projects/doc_processor_service/semantic_cache_service

export SERVER_PORT=50055
export QDRANT_URL=localhost:6334
export OPENAI_BASE_URL=http://localhost:11434/v1
export OPENAI_EMBEDDING_MODEL=nomic-embed-text
export QDRANT_COLLECTION=document_chunks

go run main.go
```

### Local Development (Windows PowerShell)
Ensure Qdrant and your AI engine are running.
```powershell
cd \home\ak\projects\doc_processor_service\semantic_cache_service

$env:SERVER_PORT="50055"
$env:QDRANT_URL="localhost:6334"
$env:OPENAI_BASE_URL="http://localhost:11434/v1"
$env:OPENAI_EMBEDDING_MODEL="nomic-embed-text"
$env:QDRANT_COLLECTION="document_chunks"

go run main.go
```

## 🧪 Testing

To run the unit tests and ensure clean architecture boundaries are respected:
```bash
cd /home/ak/projects/doc_processor_service/semantic_cache_service
go test ./... -v
```

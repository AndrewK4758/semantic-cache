# Core Components

The Semantic Cache Service is a standalone microservice responsible for generating and storing mathematical embeddings of document texts. By caching semantic signatures, the upstream Go Orchestrator can bypass expensive LLM generations for highly similar documents. 

This service adheres strictly to **Clean Architecture**, completely decoupling the application logic from the underlying storage and generation infrastructure.

## Architecture Layers

### 1. Domain Layer
Defines the core `EmbeddingService` and `VectorStore` abstractions. It has zero external dependencies, establishing the fundamental business rules.

### 2. Application Layer
Wires the interfaces together to implement use cases such as `CheckCache`, `StoreExtraction`, and `SeedCache`. It mathematically enforces the similarity thresholds.

### 3. Infrastructure Layer
- **Qdrant**: Implements `VectorStore` via gRPC/REST Cosine Similarity search with strict metadata filtering.
- **OpenAI**: Implements `EmbeddingService` via HTTP POST to an OpenAI-compatible `/v1/embeddings` endpoint.

### 4. Presentation Layer
Utilizes a **Hybrid Architecture**:
- `gRPC` (`SemanticCacheService`): Synchronous API for high-speed `CheckCache` reads.
- `NATS JetStream` (`JetStreamHandler`): Asynchronous, durable fire-and-forget message consumer for indexing writes (`cache.requests.store`).

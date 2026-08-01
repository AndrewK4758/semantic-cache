# Service Communication

## Hybrid Interfaces
The Semantic Cache Service exposes functionality through two distinct mechanisms:
- **Synchronous Reads (gRPC)**: Upstream orchestrators use gRPC for ultra-fast, blocking calls when executing `CheckCache`. It searches for semantically similar embeddings with very low latency.
- **Asynchronous Writes (NATS JetStream)**: Write operations like `StoreExtraction` are published to JetStream (`cache.requests.store`). The service consumes these in the background as a fire-and-forget queue to ensure indexing doesn't block the critical path.

## Data Contracts & No Preemptive Serialization
This service adheres to the strict **No Preemptive Serialization** mandate. 
- All JSON-based structural state must be parsed natively using `google.protobuf.Struct`.
- Preemptive stringification is forbidden to prevent schema drift, whitespace corruption, and nested parsing bugs.
- Boundary interfaces rely on typed object mappings rather than dynamic regex or fuzzy key lookups.

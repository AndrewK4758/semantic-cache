# How To Implement and Use

## Running the Service
This service requires Qdrant and an OpenAI-compatible server (like Ollama or vLLM) to be running.

### Docker (All Platforms - Recommended)
The fastest way to spin up the entire ecosystem is via the root Docker Compose file:
```bash
cd /home/ak/projects/doc_processor_service
docker-compose up -d --build
```
The service will be available on **port 11115**.

### Local Development (Linux / macOS)
Ensure Qdrant is running on port `6334` and your AI engine is available. Set your environment variables:
```bash
cd /home/ak/projects/doc_processor_service/semantic_cache_service

export SERVER_PORT=11115
export QDRANT_URL=localhost:6334
export OPENAI_BASE_URL=http://localhost:11434/v1
export OPENAI_EMBEDDING_MODEL=all-MiniLM-L6-v2
export QDRANT_COLLECTION=document_chunks

go run main.go
```

### Testing
To run the unit tests and ensure clean architecture boundaries are respected:
```bash
cd /home/ak/projects/doc_processor_service/semantic_cache_service
go test ./... -v
```

FROM golang:alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

WORKDIR /app

# Pull in shared_protos via Docker additional contexts
COPY --from=shared_protos . /app/shared_protos
COPY --from=shared_utils . /app/shared_utils
COPY . /app/semantic_cache_service

WORKDIR /app/semantic_cache_service

# Add local replace directive for development
RUN go mod edit -replace github.com/AndrewK4758/shared_protos=../shared_protos
RUN go mod edit -replace github.com/AndrewK4758/shared_utils=../shared_utils
RUN go mod download

# Build the executable
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o semantic_cache_bin main.go

# --- Final minimal stage ---
FROM alpine:latest  
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /app/semantic_cache_service/semantic_cache_bin .

EXPOSE 50055

CMD ["./semantic_cache_bin"]

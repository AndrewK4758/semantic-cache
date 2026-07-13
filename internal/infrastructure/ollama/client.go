package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/doc_processor/semantic_cache_service/internal/domain"
)

type Client struct {
	baseURL    string
	modelName  string
	httpClient *http.Client
}

// NewClient creates a new Ollama REST client.
func NewClient(baseURL, modelName string) *Client {
	return &Client{
		baseURL:    baseURL,
		modelName:  modelName,
		httpClient: &http.Client{},
	}
}

type embeddingRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type embeddingResponse struct {
	Embedding []float32 `json:"embedding"`
}

// Generate implements the domain.EmbeddingService interface.
func (c *Client) Generate(ctx context.Context, text string) ([]float32, error) {
	reqBody := embeddingRequest{
		Model:  c.modelName,
		Prompt: text,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("action failed for job OllamaGenerate: failed to marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/api/embeddings", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("action failed for job OllamaGenerate: failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("action failed for job OllamaGenerate: http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("action failed for job OllamaGenerate: unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var embResp embeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&embResp); err != nil {
		return nil, fmt.Errorf("action failed for job OllamaGenerate: failed to decode response: %w", err)
	}

	return embResp.Embedding, nil
}

// compile-time check to ensure Client implements domain.EmbeddingService
var _ domain.EmbeddingService = (*Client)(nil)

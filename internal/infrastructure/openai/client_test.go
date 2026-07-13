package openai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOpenAIClient_Generate(t *testing.T) {
	tests := []struct {
		name           string
		mockStatusCode int
		mockRespBody   embeddingResponse
		mockErrorMsg   string
		text           string
		wantResult     []float32
		wantErr        bool
	}{
		{
			name:           "Success",
			mockStatusCode: http.StatusOK,
			mockRespBody: embeddingResponse{
				Data: []struct {
					Embedding []float32 `json:"embedding"`
				}{
					{
						Embedding: []float32{0.1, 0.2, 0.3},
					},
				},
			},
			text:       "test input",
			wantResult: []float32{0.1, 0.2, 0.3},
			wantErr:    false,
		},
		{
			name:           "Error - 500 status",
			mockStatusCode: http.StatusInternalServerError,
			mockErrorMsg:   "internal error",
			text:           "test input",
			wantResult:     nil,
			wantErr:        true,
		},
		{
			name:           "Error - empty data",
			mockStatusCode: http.StatusOK,
			mockRespBody:   embeddingResponse{},
			text:           "test input",
			wantResult:     nil,
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/embeddings" {
					t.Errorf("Expected path /embeddings, got %s", r.URL.Path)
				}
				if r.Method != "POST" {
					t.Errorf("Expected method POST, got %s", r.Method)
				}

				w.WriteHeader(tt.mockStatusCode)
				if tt.mockErrorMsg != "" {
					w.Write([]byte(tt.mockErrorMsg))
				} else {
					json.NewEncoder(w).Encode(tt.mockRespBody)
				}
			}))
			defer server.Close()

			client := NewClient(server.URL, "test-embed-model")
			ctx := context.Background()
			
			got, err := client.Generate(ctx, tt.text)

			if (err != nil) != tt.wantErr {
				t.Errorf("Client.Generate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			
			if len(got) != len(tt.wantResult) {
				t.Errorf("Client.Generate() = %v, want %v", got, tt.wantResult)
				return
			}
			
			for i, v := range got {
				if v != tt.wantResult[i] {
					t.Errorf("Client.Generate() = %v, want %v", got, tt.wantResult)
					return
				}
			}
		})
	}
}

package proxy

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// ParseSSEStream reads SSE events from r, calling fn for each data payload.
// Stops on "data: [DONE]" or EOF. Returns first non-EOF error.
func ParseSSEStream(r io.Reader, fn func(data []byte) error) error {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := line[len("data: "):]
		if payload == "[DONE]" {
			return nil
		}
		if err := fn([]byte(payload)); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// chunkContentFields is used for JSON unmarshaling.
type chunkContentFields struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
}

// ChunkContent extracts delta.content from an OpenAI SSE chunk JSON.
// Returns ("", false) if no content delta.
func ChunkContent(data []byte) (string, bool) {
	var chunk chunkContentFields
	if err := json.Unmarshal(data, &chunk); err != nil {
		return "", false
	}
	if len(chunk.Choices) == 0 {
		return "", false
	}
	content := chunk.Choices[0].Delta.Content
	if content == "" {
		return "", false
	}
	return content, true
}

// chunkToolCallFields is used for JSON unmarshaling of tool_call deltas.
type chunkToolCallFields struct {
	Choices []struct {
		Delta struct {
			ToolCalls []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
	} `json:"choices"`
}

// ChunkToolCall extracts the first tool_call from an SSE chunk.
// Returns (id, name, ok). Only returns ok=true when a function name is present.
func ChunkToolCall(data []byte) (id, name string, ok bool) {
	var chunk chunkToolCallFields
	if err := json.Unmarshal(data, &chunk); err != nil {
		return "", "", false
	}
	if len(chunk.Choices) == 0 {
		return "", "", false
	}
	toolCalls := chunk.Choices[0].Delta.ToolCalls
	if len(toolCalls) == 0 {
		return "", "", false
	}
	tc := toolCalls[0]
	if tc.Function.Name == "" {
		return "", "", false
	}
	return tc.ID, tc.Function.Name, true
}

// SSEWriter writes SSE events to an http.ResponseWriter with flush after each.
type SSEWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
}

// NewSSEWriter creates an SSEWriter. Sets necessary response headers.
func NewSSEWriter(w http.ResponseWriter) *SSEWriter {
	flusher, _ := w.(http.Flusher)
	return &SSEWriter{w: w, flusher: flusher}
}

// WriteChunk writes a raw SSE data line (the []byte is the JSON payload).
func (sw *SSEWriter) WriteChunk(data []byte) error {
	_, err := fmt.Fprintf(sw.w, "data: %s\n\n", data)
	if err != nil {
		return err
	}
	if sw.flusher != nil {
		sw.flusher.Flush()
	}
	return nil
}

// WriteContent writes a synthetic content delta SSE chunk.
// Useful for indicator text or injecting proxy messages.
func (sw *SSEWriter) WriteContent(text string) error {
	type delta struct {
		Content string `json:"content"`
	}
	type choice struct {
		Delta        delta   `json:"delta"`
		Index        int     `json:"index"`
		FinishReason *string `json:"finish_reason"`
	}
	type chunk struct {
		ID      string   `json:"id"`
		Object  string   `json:"object"`
		Choices []choice `json:"choices"`
	}
	c := chunk{
		ID:     "loom-proxy",
		Object: "chat.completion.chunk",
		Choices: []choice{
			{
				Delta:        delta{Content: text},
				Index:        0,
				FinishReason: nil,
			},
		},
	}
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return sw.WriteChunk(data)
}

// WriteDone writes "data: [DONE]".
func (sw *SSEWriter) WriteDone() error {
	_, err := fmt.Fprintf(sw.w, "data: [DONE]\n\n")
	if err != nil {
		return err
	}
	if sw.flusher != nil {
		sw.flusher.Flush()
	}
	return nil
}

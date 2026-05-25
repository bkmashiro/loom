package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// TestBuildLoomSpec_NonEmpty verifies the spec contains expected keywords.
func TestBuildLoomSpec_NonEmpty(t *testing.T) {
	spec := BuildLoomSpec()
	if spec == "" {
		t.Fatal("BuildLoomSpec returned empty string")
	}
	keywords := []string{"io", "pure", "fetch"}
	for _, kw := range keywords {
		if !strings.Contains(spec, kw) {
			t.Errorf("spec missing keyword %q", kw)
		}
	}
}

// TestLoomDescribeToolDef verifies the tool def has the correct name.
func TestLoomDescribeToolDef(t *testing.T) {
	tool := LoomDescribeToolDef()
	if tool.Type != "function" {
		t.Errorf("expected type=function, got %q", tool.Type)
	}
	if tool.Function.Name != "loom_describe" {
		t.Errorf("expected name=loom_describe, got %q", tool.Function.Name)
	}
	if tool.Function.Description == "" {
		t.Error("expected non-empty description")
	}
}

// TestInjectLoomDescribeTool verifies the tool is appended to the request.
func TestInjectLoomDescribeTool(t *testing.T) {
	h := newTestHandler(t, "http://localhost:9999/v1")
	req := ChatCompletionRequest{
		Model:    "gpt-4",
		Messages: []Message{{Role: "user", Content: "hi"}},
	}
	result := h.injectLoomDescribeTool(req)

	found := false
	for _, tool := range result.Tools {
		if tool.Function.Name == "loom_describe" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected loom_describe to be injected into tools")
	}
}

// TestInjectLoomDescribeTool_AlreadyPresent verifies no duplication.
func TestInjectLoomDescribeTool_AlreadyPresent(t *testing.T) {
	h := newTestHandler(t, "http://localhost:9999/v1")
	req := ChatCompletionRequest{
		Model:    "gpt-4",
		Messages: []Message{{Role: "user", Content: "hi"}},
		Tools:    []Tool{LoomDescribeToolDef()},
	}
	result := h.injectLoomDescribeTool(req)

	count := 0
	for _, tool := range result.Tools {
		if tool.Function.Name == "loom_describe" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 loom_describe tool, got %d", count)
	}
}

// TestChunkToolCall_DetectsCall verifies parsing of a tool_call SSE chunk.
func TestChunkToolCall_DetectsCall(t *testing.T) {
	data := []byte(`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_abc","type":"function","function":{"name":"loom_describe","arguments":""}}]}}]}`)
	id, name, ok := ChunkToolCall(data)
	if !ok {
		t.Fatal("expected ok=true for tool_call chunk")
	}
	if id != "call_abc" {
		t.Errorf("expected id=call_abc, got %q", id)
	}
	if name != "loom_describe" {
		t.Errorf("expected name=loom_describe, got %q", name)
	}
}

// TestChunkToolCall_NoToolCall verifies ok=false for a regular content chunk.
func TestChunkToolCall_NoToolCall(t *testing.T) {
	data := []byte(`{"choices":[{"delta":{"content":"hello"}}]}`)
	_, _, ok := ChunkToolCall(data)
	if ok {
		t.Error("expected ok=false for content-only chunk")
	}
}

// toolCallChunk builds a tool_call SSE chunk for loom_describe.
func toolCallChunk(id string) string {
	return fmt.Sprintf(
		`{"id":"tc1","object":"chat.completion.chunk","choices":[{"delta":{"tool_calls":[{"index":0,"id":%q,"type":"function","function":{"name":"loom_describe","arguments":""}}]},"index":0}]}`,
		id,
	)
}

// TestEndToEnd_LoomDescribeCall is an integration test verifying the two-call flow.
func TestEndToEnd_LoomDescribeCall(t *testing.T) {
	var callCount int32
	var capturedRequests []ChatCompletionRequest

	// Call 1: upstream returns a tool_call for loom_describe.
	// Call 2: upstream receives the spec and returns final content.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&callCount, 1)

		body, _ := io.ReadAll(r.Body)
		var req ChatCompletionRequest
		json.Unmarshal(body, &req) //nolint:errcheck
		capturedRequests = append(capturedRequests, req)

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)

		var chunks []string
		if n == 1 {
			// First call: LLM decides to call loom_describe.
			chunks = []string{toolCallChunk("call_xyz")}
		} else {
			// Second call: LLM responds with real content after seeing the spec.
			chunks = []string{contentChunk("final", "Here is a Loom plan for you!")}
		}
		for _, chunk := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", chunk)
			flusher.Flush()
		}
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer upstream.Close()

	cfg := DefaultConfig()
	cfg.Upstream = upstream.URL + "/v1"
	cfg.LoomDescribeEnabled = true
	h := newTestHandlerWithConfig(t, cfg)

	rec := httptest.NewRecorder()
	req := makeStreamRequest([]Message{{Role: "user", Content: "explain loom"}}, "describe-test")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// Should have made exactly 2 upstream calls.
	if got := atomic.LoadInt32(&callCount); got != 2 {
		t.Errorf("expected 2 upstream calls, got %d", got)
	}

	// The second call should include a tool result message with the spec.
	if len(capturedRequests) < 2 {
		t.Fatalf("expected 2 captured requests, got %d", len(capturedRequests))
	}
	secondReqMsgs := capturedRequests[1].Messages
	foundToolResult := false
	for _, m := range secondReqMsgs {
		if m.Role == "tool" && strings.Contains(m.Content, "Loom") {
			foundToolResult = true
			break
		}
	}
	if !foundToolResult {
		t.Errorf("second upstream call missing tool result message with Loom spec")
		for i, m := range secondReqMsgs {
			t.Logf("msg[%d] role=%s content=%q", i, m.Role, m.Content)
		}
	}

	// The second call should NOT have tools (to avoid infinite loop).
	if len(capturedRequests[1].Tools) != 0 {
		t.Errorf("second upstream call should have no tools, got %d", len(capturedRequests[1].Tools))
	}

	// Client should receive the final content.
	respBody := rec.Body.String()
	if !strings.Contains(respBody, "Here is a Loom plan for you!") {
		t.Errorf("client did not receive final content:\n%s", respBody)
	}
	if !strings.Contains(respBody, "[DONE]") {
		t.Errorf("client missing [DONE]:\n%s", respBody)
	}
}

// TestLoomDescribeDisabled verifies that when disabled, loom_describe is not injected.
func TestLoomDescribeDisabled(t *testing.T) {
	var capturedTools []Tool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req ChatCompletionRequest
		json.Unmarshal(body, &req) //nolint:errcheck
		capturedTools = req.Tools

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		fmt.Fprintf(w, "data: %s\n\n", contentChunk("1", "ok"))
		flusher.Flush()
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer upstream.Close()

	cfg := DefaultConfig()
	cfg.Upstream = upstream.URL + "/v1"
	cfg.LoomDescribeEnabled = false
	h := newTestHandlerWithConfig(t, cfg)

	rec := httptest.NewRecorder()
	req := makeStreamRequest([]Message{{Role: "user", Content: "hi"}}, "")
	h.ServeHTTP(rec, req)

	for _, tool := range capturedTools {
		if tool.Function.Name == "loom_describe" {
			t.Error("loom_describe should not be injected when disabled")
		}
	}
}

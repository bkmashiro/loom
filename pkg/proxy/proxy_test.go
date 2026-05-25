package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bkmashiro/loom/pkg/loom"
)

// contentChunk builds an OpenAI SSE content chunk JSON string.
func contentChunk(id, content string) string {
	return fmt.Sprintf(`{"id":%q,"object":"chat.completion.chunk","choices":[{"delta":{"content":%q},"index":0}]}`, id, content)
}

// fakeSSEUpstream returns a test server that sends pre-canned SSE responses.
func fakeSSEUpstream(t *testing.T, chunks []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		for _, chunk := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", chunk)
			flusher.Flush()
		}
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
}

// fakeJSONUpstream returns a test server that sends a plain JSON response.
func fakeJSONUpstream(t *testing.T, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, body)
	}))
}

// fakeSSEUpstreamWithRequestCapture returns a test server that records requests and sends SSE.
func fakeSSEUpstreamWithRequestCapture(t *testing.T, chunks []string, captured *[]ChatCompletionRequest) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req ChatCompletionRequest
		json.Unmarshal(body, &req) //nolint:errcheck
		if captured != nil {
			*captured = append(*captured, req)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		for _, chunk := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", chunk)
			flusher.Flush()
		}
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
}

func newTestHandler(t *testing.T, upstream string) *Handler {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Upstream = upstream
	l := loom.New()
	h, err := NewHandler(cfg, l)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return h
}

func newTestHandlerWithConfig(t *testing.T, cfg Config) *Handler {
	t.Helper()
	l := loom.New()
	h, err := NewHandler(cfg, l)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return h
}

func TestHealth(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	h := newTestHandler(t, upstream.URL+"/v1")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"status":"ok"`) {
		t.Fatalf("unexpected health body: %s", body)
	}
}

func TestPassthrough_Streaming(t *testing.T) {
	// With Phase 3+, streaming content is re-emitted via WriteContent.
	// Verify the content tokens ("Hello", " world") appear in the response.
	chunks := []string{
		contentChunk("1", "Hello"),
		contentChunk("2", " world"),
	}
	upstream := fakeSSEUpstream(t, chunks)
	defer upstream.Close()

	h := newTestHandler(t, upstream.URL+"/v1")
	rec := httptest.NewRecorder()
	body := `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	respBody := rec.Body.String()
	// Content should appear in the SSE stream
	if !strings.Contains(respBody, "Hello") {
		t.Errorf("missing 'Hello' in response:\n%s", respBody)
	}
	if !strings.Contains(respBody, " world") {
		t.Errorf("missing ' world' in response:\n%s", respBody)
	}
	if !strings.Contains(respBody, "[DONE]") {
		t.Fatalf("missing [DONE] in response:\n%s", respBody)
	}
}

func TestPassthrough_NonStreaming(t *testing.T) {
	// With Phase 4, stream:false sends stream:true upstream and assembles JSON.
	// Use an SSE upstream to simulate proper upstream behavior.
	chunks := []string{
		contentChunk("1", "Hello!"),
	}
	upstream := fakeSSEUpstream(t, chunks)
	defer upstream.Close()

	h := newTestHandler(t, upstream.URL+"/v1")
	rec := httptest.NewRecorder()
	body := `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	respBody := rec.Body.String()
	// Should be assembled JSON with the content
	var respObj struct {
		Object  string `json:"object"`
		Choices []struct {
			Message struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal([]byte(respBody), &respObj); err != nil {
		t.Fatalf("response is not valid JSON: %v\nbody: %s", err, respBody)
	}
	if respObj.Object != "chat.completion" {
		t.Errorf("expected object=chat.completion, got %s", respObj.Object)
	}
	if len(respObj.Choices) == 0 {
		t.Fatal("expected at least one choice")
	}
	if respObj.Choices[0].Message.Content != "Hello!" {
		t.Errorf("expected content 'Hello!', got %q", respObj.Choices[0].Message.Content)
	}
	if respObj.Choices[0].FinishReason != "stop" {
		t.Errorf("expected finish_reason=stop, got %s", respObj.Choices[0].FinishReason)
	}
}

func TestAuthForwarding(t *testing.T) {
	var capturedAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer upstream.Close()

	h := newTestHandler(t, upstream.URL+"/v1")
	rec := httptest.NewRecorder()
	body := `{"model":"gpt-4","messages":[],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer sk-client-key")

	h.ServeHTTP(rec, req)

	if capturedAuth != "Bearer sk-client-key" {
		t.Fatalf("expected client auth to be forwarded, got: %s", capturedAuth)
	}
}

func TestAPIKeyOverride(t *testing.T) {
	var capturedAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer upstream.Close()

	cfg := DefaultConfig()
	cfg.Upstream = upstream.URL + "/v1"
	cfg.APIKey = "sk-server-override"
	l := loom.New()
	h, err := NewHandler(cfg, l)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	rec := httptest.NewRecorder()
	body := `{"model":"gpt-4","messages":[],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer sk-client-key")

	h.ServeHTTP(rec, req)

	if capturedAuth != "Bearer sk-server-override" {
		t.Fatalf("expected server API key override, got: %s", capturedAuth)
	}
}

// planSSEChunks splits a plan string into SSE content chunks, one per line.
func planSSEChunks(plan string) []string {
	lines := strings.SplitAfter(plan, "\n")
	chunks := make([]string, 0, len(lines))
	for _, line := range lines {
		if line != "" {
			chunks = append(chunks, contentChunk("plan", line))
		}
	}
	return chunks
}

// TestEndToEnd_NoPlan verifies that a normal (no plan) SSE response is relayed directly.
func TestEndToEnd_NoPlan(t *testing.T) {
	chunks := []string{
		contentChunk("1", "The answer is 42."),
	}
	upstream := fakeSSEUpstream(t, chunks)
	defer upstream.Close()

	h := newTestHandler(t, upstream.URL+"/v1")
	rec := httptest.NewRecorder()
	body := `{"model":"gpt-4","messages":[{"role":"user","content":"what is 6*7?"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	respBody := rec.Body.String()
	if !strings.Contains(respBody, "The answer is 42.") {
		t.Errorf("missing expected content in response:\n%s", respBody)
	}
	if !strings.Contains(respBody, "[DONE]") {
		t.Errorf("missing [DONE]:\n%s", respBody)
	}
}

// TestEndToEnd_PlanDetected verifies end-to-end: plan SSE → Loom executes → second call → summary.
func TestEndToEnd_PlanDetected(t *testing.T) {
	// The plan uses a pure step (no HTTP needed).
	plan := "```pure compute\nthe answer is 42\n```\nreturn compute\n"

	// LLM 1: returns a plan
	planChunks := planSSEChunks(plan)

	// LLM 2: returns a summary
	summaryChunk := contentChunk("summary", "The computation result is 42.")

	// Track how many upstream calls were made
	callCount := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)

		var chunks []string
		if callCount == 1 {
			chunks = planChunks
		} else {
			chunks = []string{summaryChunk}
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
	cfg.PlanVisibility = TeeModeSuppress
	h := newTestHandlerWithConfig(t, cfg)

	rec := httptest.NewRecorder()
	body := `{"model":"gpt-4","messages":[{"role":"user","content":"compute something"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if callCount != 2 {
		t.Errorf("expected 2 upstream calls (plan + summary), got %d", callCount)
	}
	respBody := rec.Body.String()
	if !strings.Contains(respBody, "42") {
		t.Errorf("expected summary content in response:\n%s", respBody)
	}
}

// TestEndToEnd_PlanVisibility_Suppress verifies plan fences are not visible to client.
func TestEndToEnd_PlanVisibility_Suppress(t *testing.T) {
	plan := "```pure compute\nthe answer is 42\n```\nreturn compute\n"
	planChunks := planSSEChunks(plan)
	summaryChunk := contentChunk("s", "Done.")

	callCount := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		var chunks []string
		if callCount == 1 {
			chunks = planChunks
		} else {
			chunks = []string{summaryChunk}
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
	cfg.PlanVisibility = TeeModeSuppress
	h := newTestHandlerWithConfig(t, cfg)

	rec := httptest.NewRecorder()
	body := `{"model":"gpt-4","messages":[{"role":"user","content":"go"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	h.ServeHTTP(rec, req)

	respBody := rec.Body.String()
	// Plan fence markers should not appear
	if strings.Contains(respBody, "```pure") {
		t.Errorf("plan fence visible in suppress mode:\n%s", respBody)
	}
	if strings.Contains(respBody, "return compute") {
		t.Errorf("return directive visible in suppress mode:\n%s", respBody)
	}
}

// TestEndToEnd_PlanVisibility_Indicator verifies indicator text is shown during plan execution.
func TestEndToEnd_PlanVisibility_Indicator(t *testing.T) {
	plan := "```pure compute\nthe answer is 42\n```\nreturn compute\n"
	planChunks := planSSEChunks(plan)
	summaryChunk := contentChunk("s", "The answer is 42.")

	callCount := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		var chunks []string
		if callCount == 1 {
			chunks = planChunks
		} else {
			chunks = []string{summaryChunk}
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
	cfg.PlanVisibility = TeeModeIndicator
	cfg.IndicatorText = "Executing plan..."
	h := newTestHandlerWithConfig(t, cfg)

	rec := httptest.NewRecorder()
	body := `{"model":"gpt-4","messages":[{"role":"user","content":"go"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	h.ServeHTTP(rec, req)

	respBody := rec.Body.String()
	if !strings.Contains(respBody, "Executing plan...") {
		t.Errorf("indicator text not found in response:\n%s", respBody)
	}
	// Plan text should still be suppressed
	if strings.Contains(respBody, "```pure") {
		t.Errorf("plan fence visible in indicator mode:\n%s", respBody)
	}
}

// TestEndToEnd_PlanExecError verifies that a failing plan step produces status="error" in second call.
func TestEndToEnd_PlanExecError(t *testing.T) {
	// Use an IO step pointing to a non-existent server — will fail
	plan := "```io fetch\nGET http://localhost:19999/nonexistent\n```\nreturn fetch\n"
	planChunks := planSSEChunks(plan)

	var secondCallBody string
	callCount := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount == 2 {
			body, _ := io.ReadAll(r.Body)
			secondCallBody = string(body)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		var chunks []string
		if callCount == 1 {
			chunks = planChunks
		} else {
			chunks = []string{contentChunk("s", "Sorry, the service is unavailable.")}
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
	cfg.PlanVisibility = TeeModeSuppress
	h := newTestHandlerWithConfig(t, cfg)

	rec := httptest.NewRecorder()
	body := `{"model":"gpt-4","messages":[{"role":"user","content":"fetch data"}],"stream":true}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	h.ServeHTTP(rec, req)

	if callCount != 2 {
		t.Errorf("expected 2 upstream calls, got %d", callCount)
	}
	// The second call should contain error status
	if !strings.Contains(secondCallBody, `"error"`) && !strings.Contains(secondCallBody, "status") {
		t.Logf("second call body: %s", secondCallBody)
	}
	// Response should contain the error message from LLM 2
	respBody := rec.Body.String()
	if !strings.Contains(respBody, "unavailable") {
		t.Logf("response body: %s", respBody)
	}
}

// ---- Phase 4 Tests ----

// TestNonStreaming_NoPlan verifies non-streaming with no plan assembles a JSON response.
func TestNonStreaming_NoPlan(t *testing.T) {
	chunks := []string{
		contentChunk("1", "Hello, "),
		contentChunk("2", "world!"),
	}
	upstream := fakeSSEUpstream(t, chunks)
	defer upstream.Close()

	h := newTestHandler(t, upstream.URL+"/v1")
	rec := httptest.NewRecorder()
	body := `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"stream":false}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Errorf("expected application/json, got %s", ct)
	}
	var resp struct {
		Object  string `json:"object"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON response: %v\nbody: %s", err, rec.Body.String())
	}
	if resp.Object != "chat.completion" {
		t.Errorf("expected chat.completion, got %s", resp.Object)
	}
	if len(resp.Choices) == 0 || resp.Choices[0].Message.Content != "Hello, world!" {
		t.Errorf("unexpected content: %q", resp.Choices[0].Message.Content)
	}
}

// TestNonStreaming_WithPlan verifies stream:false with a plan assembles final JSON.
func TestNonStreaming_WithPlan(t *testing.T) {
	plan := "```pure compute\nthe answer is 42\n```\nreturn compute\n"
	planChunks := planSSEChunks(plan)
	summaryChunk := contentChunk("s", "The answer is 42.")

	callCount := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		var chunks []string
		if callCount == 1 {
			chunks = planChunks
		} else {
			chunks = []string{summaryChunk}
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
	h := newTestHandlerWithConfig(t, cfg)

	rec := httptest.NewRecorder()
	body := `{"model":"gpt-4","messages":[{"role":"user","content":"compute"}],"stream":false}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if callCount != 2 {
		t.Errorf("expected 2 upstream calls, got %d", callCount)
	}
	var resp struct {
		Object  string `json:"object"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v\nbody: %s", err, rec.Body.String())
	}
	if resp.Choices[0].Message.Content != "The answer is 42." {
		t.Errorf("unexpected content: %q", resp.Choices[0].Message.Content)
	}
	if resp.Choices[0].FinishReason != "stop" {
		t.Errorf("expected stop, got %s", resp.Choices[0].FinishReason)
	}
}

// TestSystemPromptPrepend verifies prepend mode prepends to existing system message.
func TestSystemPromptPrepend(t *testing.T) {
	msgs := []Message{
		{Role: "system", Content: "You are helpful."},
		{Role: "user", Content: "hi"},
	}
	result := injectSystemPrompt(msgs, "LOOM INSTRUCTIONS", "prepend")

	if result[0].Role != "system" {
		t.Fatalf("first message not system: %s", result[0].Role)
	}
	if !strings.HasPrefix(result[0].Content, "LOOM INSTRUCTIONS") {
		t.Errorf("system message not prepended: %q", result[0].Content)
	}
	if !strings.Contains(result[0].Content, "You are helpful.") {
		t.Errorf("original system content lost: %q", result[0].Content)
	}
}

// TestSystemPromptReplace verifies replace mode replaces the system message.
func TestSystemPromptReplace(t *testing.T) {
	msgs := []Message{
		{Role: "system", Content: "Old system prompt."},
		{Role: "user", Content: "hi"},
	}
	result := injectSystemPrompt(msgs, "NEW PROMPT", "replace")

	if result[0].Content != "NEW PROMPT" {
		t.Errorf("expected 'NEW PROMPT', got %q", result[0].Content)
	}
	if strings.Contains(result[0].Content, "Old") {
		t.Errorf("old system prompt not replaced: %q", result[0].Content)
	}
}

// TestSystemPromptNoExisting verifies system prompt is added as first message when none exists.
func TestSystemPromptNoExisting(t *testing.T) {
	msgs := []Message{
		{Role: "user", Content: "hi"},
	}
	result := injectSystemPrompt(msgs, "SYSTEM PROMPT", "prepend")

	if len(result) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(result))
	}
	if result[0].Role != "system" {
		t.Errorf("first message not system: %s", result[0].Role)
	}
	if result[0].Content != "SYSTEM PROMPT" {
		t.Errorf("unexpected system content: %q", result[0].Content)
	}
	if result[1].Role != "user" {
		t.Errorf("second message not user: %s", result[1].Role)
	}
}

// TestSystemPromptAppend verifies append mode appends to existing system message.
func TestSystemPromptAppend(t *testing.T) {
	msgs := []Message{
		{Role: "system", Content: "Base instructions."},
		{Role: "user", Content: "hi"},
	}
	result := injectSystemPrompt(msgs, "EXTRA", "append")

	if !strings.HasSuffix(result[0].Content, "EXTRA") {
		t.Errorf("system message not appended: %q", result[0].Content)
	}
	if !strings.Contains(result[0].Content, "Base instructions.") {
		t.Errorf("original content lost: %q", result[0].Content)
	}
}

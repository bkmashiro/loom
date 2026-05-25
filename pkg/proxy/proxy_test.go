package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

// fakeSSEUpstreamWithRequestCapture records requests and sends SSE.
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

// makeStreamRequest builds a streaming POST /v1/chat/completions request.
func makeStreamRequest(messages []Message, sessionID string) *http.Request {
	req := ChatCompletionRequest{
		Model:    "gpt-4",
		Messages: messages,
		Stream:   true,
	}
	data, _ := json.Marshal(req)
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(data)))
	r.Header.Set("Content-Type", "application/json")
	if sessionID != "" {
		r.Header.Set("X-Loom-Session-ID", sessionID)
	}
	return r
}

// ---- Basic passthrough tests (no plan) ----

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
	if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Fatalf("unexpected health body: %s", rec.Body.String())
	}
}

func TestPassthrough_Streaming(t *testing.T) {
	chunks := []string{
		contentChunk("1", "Hello"),
		contentChunk("2", " world"),
	}
	upstream := fakeSSEUpstream(t, chunks)
	defer upstream.Close()

	h := newTestHandler(t, upstream.URL+"/v1")
	rec := httptest.NewRecorder()
	req := makeStreamRequest([]Message{{Role: "user", Content: "hi"}}, "")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	respBody := rec.Body.String()
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
	chunks := []string{contentChunk("1", "Hello!")}
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
	var respObj struct {
		Object  string `json:"object"`
		Choices []struct {
			Message      struct{ Content string `json:"content"` } `json:"message"`
			FinishReason string                                     `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &respObj); err != nil {
		t.Fatalf("response is not valid JSON: %v\nbody: %s", err, rec.Body.String())
	}
	if respObj.Object != "chat.completion" {
		t.Errorf("expected object=chat.completion, got %s", respObj.Object)
	}
	if len(respObj.Choices) == 0 {
		t.Fatal("expected at least one choice")
	}
	if respObj.Choices[0].Message.Content != "Hello!" {
		t.Errorf("expected 'Hello!', got %q", respObj.Choices[0].Message.Content)
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
	req := makeStreamRequest(nil, "")
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
		w.(http.Flusher).Flush()
		fmt.Fprintf(w, "data: [DONE]\n\n")
		w.(http.Flusher).Flush()
	}))
	defer upstream.Close()

	cfg := DefaultConfig()
	cfg.Upstream = upstream.URL + "/v1"
	cfg.APIKey = "sk-server-override"
	h := newTestHandlerWithConfig(t, cfg)

	rec := httptest.NewRecorder()
	req := makeStreamRequest(nil, "")
	req.Header.Set("Authorization", "Bearer sk-client-key")
	h.ServeHTTP(rec, req)

	if capturedAuth != "Bearer sk-server-override" {
		t.Fatalf("expected server API key override, got: %s", capturedAuth)
	}
}

// ---- Plan detection tests (v2: one round only, background execution) ----

// TestEndToEnd_NoPlan verifies that a normal (no plan) SSE response is relayed directly.
func TestEndToEnd_NoPlan(t *testing.T) {
	chunks := []string{contentChunk("1", "The answer is 42.")}
	upstream := fakeSSEUpstream(t, chunks)
	defer upstream.Close()

	h := newTestHandler(t, upstream.URL+"/v1")
	rec := httptest.NewRecorder()
	req := makeStreamRequest([]Message{{Role: "user", Content: "what is 6*7?"}}, "")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	respBody := rec.Body.String()
	if !strings.Contains(respBody, "The answer is 42.") {
		t.Errorf("missing expected content:\n%s", respBody)
	}
	if !strings.Contains(respBody, "[DONE]") {
		t.Errorf("missing [DONE]:\n%s", respBody)
	}
}

// TestEndToEnd_PlanDetected_V2 verifies the v2 two-round flow:
// Round 1 → plan detected → background execution → [DONE]
// Round 2 → results injected into messages → upstream sees injected messages
func TestEndToEnd_PlanDetected_V2(t *testing.T) {
	// Round 1: LLM returns a pure compute plan.
	plan := "```pure compute\nthe answer is 42\n```\n"
	planChunks := planSSEChunks(plan)

	var capturedRequests []ChatCompletionRequest
	callCount := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		body, _ := io.ReadAll(r.Body)
		var req ChatCompletionRequest
		json.Unmarshal(body, &req) //nolint:errcheck
		capturedRequests = append(capturedRequests, req)

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)

		var chunks []string
		if callCount == 1 {
			chunks = planChunks
		} else {
			chunks = []string{contentChunk("r", "The computation returned 42.")}
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

	sessionID := "test-v2-session"

	// === Round 1: Send request with plan. ===
	rec1 := httptest.NewRecorder()
	req1 := makeStreamRequest([]Message{{Role: "user", Content: "compute something"}}, sessionID)
	h.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("round 1: expected 200, got %d", rec1.Code)
	}
	// Round 1 response: plan suppressed, [DONE] sent, only 1 upstream call so far.
	if callCount != 1 {
		t.Errorf("expected 1 upstream call after round 1, got %d", callCount)
	}
	// Client gets [DONE] — plan content suppressed.
	r1body := rec1.Body.String()
	if !strings.Contains(r1body, "[DONE]") {
		t.Errorf("round 1: missing [DONE]\n%s", r1body)
	}
	if strings.Contains(r1body, "```pure") {
		t.Errorf("round 1: plan text should be suppressed\n%s", r1body)
	}

	// === Round 2: Send follow-up. The proxy blocks until background execution done,
	// then injects results into the messages before forwarding to upstream. ===
	rec2 := httptest.NewRecorder()
	req2 := makeStreamRequest([]Message{
		{Role: "user", Content: "compute something"},
		{Role: "assistant", Content: ""},   // client's (potentially empty) assistant msg
		{Role: "user", Content: "what did you get?"},
	}, sessionID)
	h.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("round 2: expected 200, got %d", rec2.Code)
	}
	if callCount != 2 {
		t.Errorf("expected 2 upstream calls total, got %d", callCount)
	}

	// Verify the second upstream call received injected result messages.
	if len(capturedRequests) < 2 {
		t.Fatalf("expected 2 captured requests, got %d", len(capturedRequests))
	}
	r2msgs := capturedRequests[1].Messages
	// Should have: user, assistant(full plan), tool(results), user(follow-up)
	if len(r2msgs) < 4 {
		t.Errorf("round 2: expected at least 4 messages (with injection), got %d: %+v", len(r2msgs), r2msgs)
	}

	// Find the tool/user result message.
	foundResults := false
	for _, m := range r2msgs {
		if (m.Role == "tool" || m.Role == "user") && strings.Contains(m.Content, "compute") {
			foundResults = true
			break
		}
	}
	if !foundResults {
		// Log the messages for debugging.
		for i, m := range r2msgs {
			t.Logf("msg[%d] role=%s content=%q", i, m.Role, m.Content)
		}
		t.Errorf("round 2: expected Loom results in injected messages")
	}
}

// TestEndToEnd_PlanVisibility_Suppress verifies plan fences are not visible to client.
func TestEndToEnd_PlanVisibility_Suppress(t *testing.T) {
	plan := "```pure compute\nthe answer is 42\n```\n"
	planChunks := planSSEChunks(plan)
	upstream := fakeSSEUpstream(t, planChunks)
	defer upstream.Close()

	cfg := DefaultConfig()
	cfg.Upstream = upstream.URL + "/v1"
	cfg.PlanVisibility = TeeModeSuppress
	h := newTestHandlerWithConfig(t, cfg)

	rec := httptest.NewRecorder()
	req := makeStreamRequest([]Message{{Role: "user", Content: "go"}}, "sess-suppress")
	h.ServeHTTP(rec, req)

	respBody := rec.Body.String()
	if strings.Contains(respBody, "```pure") {
		t.Errorf("plan fence visible in suppress mode:\n%s", respBody)
	}
	if strings.Contains(respBody, "the answer is 42") {
		t.Errorf("plan body visible in suppress mode:\n%s", respBody)
	}
	// [DONE] should still be sent.
	if !strings.Contains(respBody, "[DONE]") {
		t.Errorf("missing [DONE] in suppress mode:\n%s", respBody)
	}
}

// TestEndToEnd_PlanVisibility_Passthrough verifies plan fences are forwarded to client.
func TestEndToEnd_PlanVisibility_Passthrough(t *testing.T) {
	plan := "```pure compute\nthe answer is 42\n```\n"
	planChunks := planSSEChunks(plan)
	upstream := fakeSSEUpstream(t, planChunks)
	defer upstream.Close()

	cfg := DefaultConfig()
	cfg.Upstream = upstream.URL + "/v1"
	cfg.PlanVisibility = TeeModePassthrough
	h := newTestHandlerWithConfig(t, cfg)

	rec := httptest.NewRecorder()
	req := makeStreamRequest([]Message{{Role: "user", Content: "go"}}, "sess-passthrough")
	h.ServeHTTP(rec, req)

	respBody := rec.Body.String()
	if !strings.Contains(respBody, "```pure") {
		t.Errorf("plan fence missing in passthrough mode:\n%s", respBody)
	}
}

// TestEndToEnd_PlanVisibility_Indicator verifies indicator text is shown.
func TestEndToEnd_PlanVisibility_Indicator(t *testing.T) {
	plan := "```pure compute\nthe answer is 42\n```\n"
	planChunks := planSSEChunks(plan)
	upstream := fakeSSEUpstream(t, planChunks)
	defer upstream.Close()

	cfg := DefaultConfig()
	cfg.Upstream = upstream.URL + "/v1"
	cfg.PlanVisibility = TeeModeIndicator
	cfg.IndicatorText = "Executing plan..."
	h := newTestHandlerWithConfig(t, cfg)

	rec := httptest.NewRecorder()
	req := makeStreamRequest([]Message{{Role: "user", Content: "go"}}, "sess-indicator")
	h.ServeHTTP(rec, req)

	respBody := rec.Body.String()
	if !strings.Contains(respBody, "Executing plan...") {
		t.Errorf("indicator text not found:\n%s", respBody)
	}
	// Plan fences should still be suppressed.
	if strings.Contains(respBody, "```pure") {
		t.Errorf("plan fence visible in indicator mode:\n%s", respBody)
	}
}

// TestEndToEnd_BackgroundExecution_Completes verifies execution runs after [DONE].
func TestEndToEnd_BackgroundExecution_Completes(t *testing.T) {
	plan := "```pure compute\nreturn 42\n```\n"
	planChunks := planSSEChunks(plan)
	upstream := fakeSSEUpstream(t, planChunks)
	defer upstream.Close()

	h := newTestHandler(t, upstream.URL+"/v1")
	sessionID := "exec-test-session"

	rec := httptest.NewRecorder()
	req := makeStreamRequest([]Message{{Role: "user", Content: "run"}}, sessionID)
	h.ServeHTTP(rec, req)

	// After round 1, background execution should be in-flight or done.
	// Wait for it by making a round 2 request (proxy blocks until done).
	// We don't need a real upstream for round 2 to verify execution completed.
	// Just check the session has pending results or ExecutionDone channel.

	session := h.sessions.Get(sessionID)
	if session == nil {
		t.Fatal("expected session to exist after round 1")
	}

	// Block until execution completes (with timeout).
	ctx := &timeoutCtx{deadline: time.Now().Add(5 * time.Second)}
	if err := h.waitForPendingExecution(ctx, session); err != nil {
		t.Fatalf("waitForPendingExecution: %v", err)
	}

	session.Mu.Lock()
	results := session.PendingResults
	session.Mu.Unlock()

	if results == nil {
		t.Error("expected PendingResults to be set after execution")
	}
}

// timeoutCtx is a minimal context.Context with a deadline for testing.
type timeoutCtx struct {
	deadline time.Time
}

func (c *timeoutCtx) Deadline() (time.Time, bool)          { return c.deadline, true }
func (c *timeoutCtx) Done() <-chan struct{}                  { return makeDeadlineChan(c.deadline) }
func (c *timeoutCtx) Err() error                            { return nil }
func (c *timeoutCtx) Value(_ any) any                       { return nil }

func makeDeadlineChan(deadline time.Time) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		time.Sleep(time.Until(deadline))
		close(ch)
	}()
	return ch
}

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

// TestNonStreaming_WithPlan_V2 verifies non-streaming with a plan returns immediately
// and starts background execution (no second LLM call in same request).
func TestNonStreaming_WithPlan_V2(t *testing.T) {
	plan := "```pure compute\nthe answer is 42\n```\n"
	planChunks := planSSEChunks(plan)

	callCount := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		for _, chunk := range planChunks {
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
	// v2: only 1 upstream call (no second LLM call within the same request).
	if callCount != 1 {
		t.Errorf("v2: expected 1 upstream call, got %d", callCount)
	}
	// Response should be valid JSON.
	var resp struct {
		Object  string `json:"object"`
		Choices []struct {
			Message      struct{ Content string `json:"content"` } `json:"message"`
			FinishReason string                                     `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v\nbody: %s", err, rec.Body.String())
	}
	if resp.Object != "chat.completion" {
		t.Errorf("expected chat.completion, got %s", resp.Object)
	}
	if resp.Choices[0].FinishReason != "stop" {
		t.Errorf("expected stop, got %s", resp.Choices[0].FinishReason)
	}
}

// ---- System prompt injection tests ----

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

func TestSystemPromptReplace(t *testing.T) {
	msgs := []Message{
		{Role: "system", Content: "Old system prompt."},
		{Role: "user", Content: "hi"},
	}
	result := injectSystemPrompt(msgs, "NEW PROMPT", "replace")

	if result[0].Content != "NEW PROMPT" {
		t.Errorf("expected 'NEW PROMPT', got %q", result[0].Content)
	}
}

func TestSystemPromptNoExisting(t *testing.T) {
	msgs := []Message{{Role: "user", Content: "hi"}}
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
}

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

// ---- Session resolution tests ----

func TestResolveSessionID_ExplicitHeader(t *testing.T) {
	h := newTestHandler(t, "http://localhost:9999/v1")

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("X-Loom-Session-ID", "my-session-id")

	msgs := []Message{{Role: "user", Content: "hi"}}
	id := h.resolveSessionID(req, msgs)

	if id != "my-session-id" {
		t.Errorf("expected explicit session ID, got %q", id)
	}
}

func TestResolveSessionID_Derived(t *testing.T) {
	h := newTestHandler(t, "http://localhost:9999/v1")

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	// No X-Loom-Session-ID header.

	msgs := []Message{
		{Role: "user", Content: "q1"},
		{Role: "assistant", Content: "a1"},
		{Role: "user", Content: "q2"},
	}
	id := h.resolveSessionID(req, msgs)

	if len(id) != 16 {
		t.Errorf("expected 16-char derived ID, got %q (len=%d)", id, len(id))
	}

	// Same messages → same ID.
	id2 := h.resolveSessionID(req, msgs)
	if id != id2 {
		t.Errorf("derived ID not stable: %q vs %q", id, id2)
	}
}

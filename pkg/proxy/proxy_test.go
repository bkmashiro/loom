package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bkmashiro/loom/pkg/loom"
)

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
	chunks := []string{
		`{"id":"1","object":"chat.completion.chunk","choices":[{"delta":{"content":"Hello"},"index":0}]}`,
		`{"id":"2","object":"chat.completion.chunk","choices":[{"delta":{"content":" world"},"index":0}]}`,
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
	for _, chunk := range chunks {
		if !strings.Contains(respBody, chunk) {
			t.Fatalf("missing chunk in response: %s\nfull response:\n%s", chunk, respBody)
		}
	}
	if !strings.Contains(respBody, "[DONE]") {
		t.Fatalf("missing [DONE] in response:\n%s", respBody)
	}
}

func TestPassthrough_NonStreaming(t *testing.T) {
	jsonResp := `{"id":"chatcmpl-1","object":"chat.completion","choices":[{"message":{"role":"assistant","content":"Hello!"},"index":0}]}`
	upstream := fakeJSONUpstream(t, jsonResp)
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
	if respBody != jsonResp {
		t.Fatalf("unexpected response body:\ngot:  %s\nwant: %s", respBody, jsonResp)
	}
}

func TestAuthForwarding(t *testing.T) {
	var capturedAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{}`) //nolint:errcheck
	}))
	defer upstream.Close()

	h := newTestHandler(t, upstream.URL+"/v1")
	rec := httptest.NewRecorder()
	body := `{"model":"gpt-4","messages":[]}`
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
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{}`) //nolint:errcheck
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
	body := `{"model":"gpt-4","messages":[]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer sk-client-key")

	h.ServeHTTP(rec, req)

	if capturedAuth != "Bearer sk-server-override" {
		t.Fatalf("expected server API key override, got: %s", capturedAuth)
	}
}

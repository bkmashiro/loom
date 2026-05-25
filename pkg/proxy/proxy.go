package proxy

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/bkmashiro/loom/pkg/loom"
)

// Handler is the main HTTP handler for Loom Proxy.
type Handler struct {
	upstream   *url.URL
	httpClient *http.Client
	loom       *loom.Loom
	config     Config
	logger     *slog.Logger
}

// NewHandler creates a new Handler with the given config and Loom runtime.
func NewHandler(cfg Config, l *loom.Loom) (*Handler, error) {
	u, err := url.Parse(cfg.Upstream)
	if err != nil {
		return nil, err
	}

	level := slog.LevelInfo
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	return &Handler{
		upstream:   u,
		httpClient: &http.Client{Timeout: cfg.Timeout},
		loom:       l,
		config:     cfg,
		logger:     logger,
	}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/v1/chat/completions":
		h.handleChatCompletions(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/health":
		h.handleHealth(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/models":
		h.handleModels(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`)) //nolint:errcheck
}

func (h *Handler) handleModels(w http.ResponseWriter, r *http.Request) {
	h.proxyRequest(w, r, h.upstream.String()+"/models")
}

func (h *Handler) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	h.proxyRequest(w, r, h.upstream.String()+"/chat/completions")
}

// proxyRequest forwards the request to targetURL and relays the response back.
func (h *Handler) proxyRequest(w http.ResponseWriter, r *http.Request, targetURL string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, strings.NewReader(string(body)))
	if err != nil {
		http.Error(w, "failed to create upstream request", http.StatusInternalServerError)
		return
	}

	// Copy headers
	for key, values := range r.Header {
		for _, v := range values {
			req.Header.Add(key, v)
		}
	}

	// Auth forwarding: use LOOM_API_KEY if set, otherwise forward client's Authorization header
	if h.config.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+h.config.APIKey)
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		http.Error(w, "upstream request failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Copy response headers
	for key, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(key, v)
		}
	}
	w.WriteHeader(resp.StatusCode)

	// Check if it's an SSE stream
	contentType := resp.Header.Get("Content-Type")
	if strings.Contains(contentType, "text/event-stream") {
		// Relay SSE stream
		sw := NewSSEWriter(w)
		err = ParseSSEStream(resp.Body, func(data []byte) error {
			return sw.WriteChunk(data)
		})
		if err != nil {
			h.logger.Error("SSE relay error", "err", err)
			return
		}
		sw.WriteDone() //nolint:errcheck
	} else {
		// Relay non-streaming response
		io.Copy(w, resp.Body) //nolint:errcheck
	}
}

// streamingBody is a helper to check if the request body contains "stream":true.
func isStreamingRequest(body []byte) bool {
	var req struct {
		Stream *bool `json:"stream"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return false
	}
	return req.Stream != nil && *req.Stream
}

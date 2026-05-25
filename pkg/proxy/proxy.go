package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/bkmashiro/loom/pkg/dag"
	"github.com/bkmashiro/loom/pkg/loom"
)

// errStopSSE is a sentinel error used to break out of ParseSSEStream when a plan
// is complete. ParseSSEStream treats this as a clean stop (not propagated).
var errStopSSE = errors.New("proxy: plan complete, stop SSE")

// Handler is the main HTTP handler for Loom Proxy.
type Handler struct {
	upstream     *url.URL
	httpClient   *http.Client
	loom         *loom.Loom
	config       Config
	logger       *slog.Logger
	systemPrompt string // loaded from file at startup
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

	h := &Handler{
		upstream:   u,
		httpClient: &http.Client{Timeout: cfg.Timeout},
		loom:       l,
		config:     cfg,
		logger:     logger,
	}

	// Load system prompt from file if configured
	if cfg.SystemPromptFile != "" {
		data, err := os.ReadFile(cfg.SystemPromptFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read system prompt file: %w", err)
		}
		h.systemPrompt = strings.TrimSpace(string(data))
	}

	return h, nil
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
	h.proxyPassthrough(w, r, h.upstream.String()+"/models")
}

// handleChatCompletions is the main entry point for chat completion requests.
func (h *Handler) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}

	// Parse the request
	var chatReq ChatCompletionRequest
	if err := json.Unmarshal(body, &chatReq); err != nil {
		http.Error(w, "failed to parse request body", http.StatusBadRequest)
		return
	}

	// Inject system prompt if configured
	if h.systemPrompt != "" {
		chatReq.Messages = injectSystemPrompt(chatReq.Messages, h.systemPrompt, h.config.SystemPromptMode)
	}

	// Check if client wants non-streaming
	clientWantsStream := chatReq.Stream

	if clientWantsStream {
		h.handleStreamingRequest(w, r, chatReq)
	} else {
		h.handleNonStreamingRequest(w, r, chatReq)
	}
}

// handleStreamingRequest processes a streaming request end-to-end.
func (h *Handler) handleStreamingRequest(w http.ResponseWriter, r *http.Request, chatReq ChatCompletionRequest) {
	chatReq.Stream = true

	upstreamResp, err := h.forwardToUpstream(r, chatReq)
	if err != nil {
		http.Error(w, "upstream request failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer upstreamResp.Body.Close()

	if upstreamResp.StatusCode != http.StatusOK {
		// Forward error response as-is
		for key, values := range upstreamResp.Header {
			for _, v := range values {
				w.Header().Add(key, v)
			}
		}
		w.WriteHeader(upstreamResp.StatusCode)
		io.Copy(w, upstreamResp.Body) //nolint:errcheck
		return
	}

	// Set up SSE response headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	sseWriter := NewSSEWriter(w)
	planText, prePlanText, planDetected := h.processFirstStream(r.Context(), upstreamResp.Body, sseWriter, chatReq)

	if !planDetected {
		sseWriter.WriteDone() //nolint:errcheck
		return
	}

	// Execute Loom plan
	planCtx, cancel := context.WithTimeout(r.Context(), h.config.PlanTimeout)
	defer cancel()

	results := collectStepResults(planCtx, h.loom, planText)

	// Build second request
	injector := &ResultInjector{
		OriginalMessages: chatReq.Messages,
		PrePlanText:      prePlanText,
		Results:          results,
		Model:            chatReq.Model,
	}
	secondReq := injector.BuildRequest()

	secondResp, err := h.forwardToUpstream(r, secondReq)
	if err != nil {
		h.logger.Error("second LLM call failed", "err", err)
		sseWriter.WriteContent("Error: " + err.Error()) //nolint:errcheck
		sseWriter.WriteDone()                           //nolint:errcheck
		return
	}
	defer secondResp.Body.Close()

	// Forward second response to client
	err = ParseSSEStream(secondResp.Body, func(data []byte) error {
		return sseWriter.WriteChunk(data)
	})
	if err != nil {
		h.logger.Error("SSE relay error (second call)", "err", err)
	}
	sseWriter.WriteDone() //nolint:errcheck
}

// handleNonStreamingRequest processes a non-streaming request.
// Internally always uses streaming to detect plans, then assembles a JSON response.
func (h *Handler) handleNonStreamingRequest(w http.ResponseWriter, r *http.Request, chatReq ChatCompletionRequest) {
	// Force streaming upstream for plan detection
	chatReq.Stream = true

	upstreamResp, err := h.forwardToUpstream(r, chatReq)
	if err != nil {
		http.Error(w, "upstream request failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer upstreamResp.Body.Close()

	if upstreamResp.StatusCode != http.StatusOK {
		for key, values := range upstreamResp.Header {
			for _, v := range values {
				w.Header().Add(key, v)
			}
		}
		w.WriteHeader(upstreamResp.StatusCode)
		io.Copy(w, upstreamResp.Body) //nolint:errcheck
		return
	}

	// Accumulate all content via a capturing SSE writer.
	capWriter := &capturingSSEWriter{}
	planText, prePlanText, planDetected := h.processFirstStreamCore(r.Context(), upstreamResp.Body, capWriter, chatReq)

	var finalContent string

	if !planDetected {
		finalContent = capWriter.String()
	} else {
		// Execute Loom plan
		planCtx, cancel := context.WithTimeout(r.Context(), h.config.PlanTimeout)
		defer cancel()

		results := collectStepResults(planCtx, h.loom, planText)

		// Build second request
		injector := &ResultInjector{
			OriginalMessages: chatReq.Messages,
			PrePlanText:      prePlanText,
			Results:          results,
			Model:            chatReq.Model,
		}
		secondReq := injector.BuildRequest()

		secondResp, err := h.forwardToUpstream(r, secondReq)
		if err != nil {
			http.Error(w, "second LLM call failed: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer secondResp.Body.Close()

		// Accumulate final response
		var finalBuf strings.Builder
		ParseSSEStream(secondResp.Body, func(data []byte) error { //nolint:errcheck
			if content, ok := ChunkContent(data); ok {
				finalBuf.WriteString(content)
			}
			return nil
		})
		finalContent = finalBuf.String()
	}

	// Assemble JSON response
	type respMessage struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	type choice struct {
		Message      respMessage `json:"message"`
		FinishReason string      `json:"finish_reason"`
		Index        int         `json:"index"`
	}
	type response struct {
		ID      string   `json:"id"`
		Object  string   `json:"object"`
		Model   string   `json:"model"`
		Choices []choice `json:"choices"`
	}
	resp := response{
		ID:     "chatcmpl-loom-proxy",
		Object: "chat.completion",
		Model:  chatReq.Model,
		Choices: []choice{
			{
				Message:      respMessage{Role: "assistant", Content: finalContent},
				FinishReason: "stop",
				Index:        0,
			},
		},
	}
	data, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "failed to marshal response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(data) //nolint:errcheck
}

// capturingSSEWriter captures all WriteContent calls (for non-streaming accumulation).
type capturingSSEWriter struct {
	buf strings.Builder
}

func (c *capturingSSEWriter) WriteChunk(_ []byte) error   { return nil }
func (c *capturingSSEWriter) WriteContent(s string) error { c.buf.WriteString(s); return nil }
func (c *capturingSSEWriter) WriteDone() error            { return nil }
func (c *capturingSSEWriter) String() string              { return c.buf.String() }

// sseWriterIface is satisfied by *SSEWriter and capturing/null writers.
type sseWriterIface interface {
	WriteChunk([]byte) error
	WriteContent(string) error
	WriteDone() error
}

// processFirstStream processes the upstream SSE stream, forwarding tokens per visibility config.
// Returns (planText, prePlanText, planDetected).
func (h *Handler) processFirstStream(ctx context.Context, body io.Reader, sw *SSEWriter, chatReq ChatCompletionRequest) (string, string, bool) {
	return h.processFirstStreamCore(ctx, body, sw, chatReq)
}

// processFirstStreamCore is the core SSE processing logic. Uses sw to forward content.
// Returns (planText, prePlanText, planDetected).
// Note: prePlanText is only populated when a plan is detected (for pre-plan assistant message).
func (h *Handler) processFirstStreamCore(ctx context.Context, body io.Reader, sw sseWriterIface, chatReq ChatCompletionRequest) (string, string, bool) {
	var (
		prePlanText     strings.Builder
		lastBufLen      int // tracks how many bytes of prePlanText have been added for ActionBuffer
		planDetector    = &PlanDetector{}
		planDone        bool
	)

	cfg := h.config

	err := ParseSSEStream(body, func(data []byte) error {
		content, ok := ChunkContent(data)
		if !ok {
			// Non-content chunk (e.g., role delta) — forward raw unless plan active
			if planDetector.State() == StateIdle || cfg.PlanVisibility == TeeModePassthrough {
				return sw.WriteChunk(data)
			}
			return nil
		}

		actions := planDetector.Feed(content)
		for _, act := range actions {
			switch act.Type {
			case ActionForward:
				if planDetector.State() == StateIdle {
					// Trim any buffered content already in prePlanText for this line.
					// On ActionForward, act.Content includes the whole line (with newline).
					// We need to avoid double-counting if ActionBuffer was emitted before.
					if lastBufLen > 0 {
						// The ActionBuffer already wrote partial content; reset counter.
						// The completed line ActionForward already includes that partial content.
						// Since prePlanText was written via ActionBuffer, and now we get the full
						// line via ActionForward, we need to undo the partial write and rewrite.
						// Simplest: trim the last lastBufLen bytes and re-append.
						cur := prePlanText.String()
						if len(cur) >= lastBufLen {
							prePlanText.Reset()
							prePlanText.WriteString(cur[:len(cur)-lastBufLen])
						}
						lastBufLen = 0
					}
					prePlanText.WriteString(act.Content)
				}
				// Forward in passthrough mode, or when we're still in idle
				if cfg.PlanVisibility == TeeModePassthrough || planDetector.State() == StateIdle {
					if err := sw.WriteContent(act.Content); err != nil {
						return err
					}
				}
			case ActionBuffer:
				// Partial line in idle — forward tentatively. act.Content = FULL lineBuf.
				// We track how much we've already added to avoid double-counting.
				if planDetector.State() == StateIdle {
					cur := prePlanText.String()
					// Remove old buffer portion, add new full buffer
					if lastBufLen > 0 && len(cur) >= lastBufLen {
						prePlanText.Reset()
						prePlanText.WriteString(cur[:len(cur)-lastBufLen])
					}
					// Incremental content = new full buf - old buf (in the SSEWriter sense)
					// But since we're tracking via sw, just write the new character(s).
					// act.Content is the full lineBuf; compute delta
					delta := act.Content
					if lastBufLen > 0 && len(act.Content) > lastBufLen {
						delta = act.Content[lastBufLen:]
					} else if lastBufLen > 0 {
						// Buffer didn't grow (shouldn't happen normally)
						delta = ""
					}
					prePlanText.WriteString(act.Content)
					lastBufLen = len(act.Content)
					if delta != "" {
						if err := sw.WriteContent(delta); err != nil {
							return err
						}
					}
				} else if cfg.PlanVisibility == TeeModePassthrough {
					if err := sw.WriteContent(act.Content); err != nil {
						return err
					}
				}
			case ActionSuppress:
				if cfg.PlanVisibility == TeeModePassthrough {
					if err := sw.WriteContent(act.Content); err != nil {
						return err
					}
				}
				// else: suppress
			case ActionFlush:
				if lastBufLen > 0 {
					// Flush: the buffer turned out not to be a fence — act.Content
					// includes the full flushed content. We already forwarded it
					// incrementally via ActionBuffer, so no additional write needed.
					// But prePlanText already has it. Reset lastBufLen.
					lastBufLen = 0
				} else {
					prePlanText.WriteString(act.Content)
					if cfg.PlanVisibility == TeeModePassthrough || planDetector.State() == StateIdle {
						if err := sw.WriteContent(act.Content); err != nil {
							return err
						}
					}
				}
			case ActionPlanComplete:
				lastBufLen = 0
				planDone = true
				if cfg.PlanVisibility == TeeModeIndicator {
					sw.WriteContent(cfg.IndicatorText) //nolint:errcheck
				}
				return errStopSSE
			}
		}
		return nil
	})

	if err != nil && !errors.Is(err, errStopSSE) {
		h.logger.Error("SSE stream error", "err", err)
	}

	if !planDone {
		return "", prePlanText.String(), false
	}

	return planDetector.PlanText(), prePlanText.String(), true
}

// collectStepResults drains a loom.Stream channel and returns all step results.
func collectStepResults(ctx context.Context, l *loom.Loom, planText string) []dag.StepResult {
	ch := l.Stream(ctx, strings.NewReader(planText))
	var results []dag.StepResult
	for sr := range ch {
		results = append(results, sr)
	}
	return results
}

// forwardToUpstream sends a ChatCompletionRequest to the upstream LLM and returns the response.
func (h *Handler) forwardToUpstream(r *http.Request, chatReq ChatCompletionRequest) (*http.Response, error) {
	data, err := json.Marshal(chatReq)
	if err != nil {
		return nil, err
	}

	targetURL := h.upstream.String() + "/chat/completions"
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, targetURL, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	// Copy relevant headers from original request
	req.Header.Set("Content-Type", "application/json")
	if auth := r.Header.Get("Authorization"); auth != "" {
		req.Header.Set("Authorization", auth)
	}
	if accept := r.Header.Get("Accept"); accept != "" {
		req.Header.Set("Accept", accept)
	}

	// Auth override
	if h.config.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+h.config.APIKey)
	}

	return h.httpClient.Do(req)
}

// proxyPassthrough forwards a request to targetURL and relays the response back unchanged.
func (h *Handler) proxyPassthrough(w http.ResponseWriter, r *http.Request, targetURL string) {
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

	// Auth forwarding
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

	contentType := resp.Header.Get("Content-Type")
	if strings.Contains(contentType, "text/event-stream") {
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
		io.Copy(w, resp.Body) //nolint:errcheck
	}
}

// injectSystemPrompt merges prompt into messages according to mode.
// Modes: "prepend" (default), "append", "replace".
func injectSystemPrompt(messages []Message, prompt, mode string) []Message {
	if mode == "" {
		mode = "prepend"
	}

	// Find existing system message index
	sysIdx := -1
	for i, m := range messages {
		if m.Role == "system" {
			sysIdx = i
			break
		}
	}

	switch mode {
	case "replace":
		if sysIdx >= 0 {
			messages[sysIdx].Content = prompt
		} else {
			messages = append([]Message{{Role: "system", Content: prompt}}, messages...)
		}
	case "append":
		if sysIdx >= 0 {
			messages[sysIdx].Content = messages[sysIdx].Content + "\n" + prompt
		} else {
			messages = append([]Message{{Role: "system", Content: prompt}}, messages...)
		}
	default: // "prepend"
		if sysIdx >= 0 {
			messages[sysIdx].Content = prompt + "\n" + messages[sysIdx].Content
		} else {
			messages = append([]Message{{Role: "system", Content: prompt}}, messages...)
		}
	}
	return messages
}

// isStreamingRequest checks if the request body contains "stream":true.
// Kept for backwards compatibility / reference.
func isStreamingRequest(body []byte) bool {
	var req struct {
		Stream *bool `json:"stream"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return false
	}
	return req.Stream != nil && *req.Stream
}

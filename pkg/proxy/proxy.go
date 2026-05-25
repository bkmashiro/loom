package proxy

import (
	"bytes"
	"context"
	"encoding/json"
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

// Handler is the main HTTP handler for Loom Proxy v2.
type Handler struct {
	upstream     *url.URL
	httpClient   *http.Client
	loom         *loom.Loom
	config       Config
	logger       *slog.Logger
	sessions     *SessionStore
	metrics      *Metrics
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
		sessions:   NewSessionStore(cfg.SessionTTL),
		metrics:    &Metrics{},
	}

	// Load system prompt from file if configured.
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
	case r.Method == http.MethodGet && r.URL.Path == "/metrics":
		h.metrics.Handler(h.sessions.Len).ServeHTTP(w, r)
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
	h.metrics.RequestsTotal.Add(1)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}

	var chatReq ChatCompletionRequest
	if err := json.Unmarshal(body, &chatReq); err != nil {
		http.Error(w, "failed to parse request body", http.StatusBadRequest)
		return
	}

	// === INGRESS: Resolve session and inject any pending results. ===
	sessionID := h.resolveSessionID(r, chatReq.Messages)
	session := h.sessions.Get(sessionID)

	if session != nil {
		// Block until any in-flight background execution completes.
		if err := h.waitForPendingExecution(r.Context(), session); err != nil {
			http.Error(w, "timed out waiting for plan execution: "+err.Error(), http.StatusGatewayTimeout)
			return
		}

		// Inject pending results into the messages.
		session.Mu.Lock()
		if session.PendingResults != nil {
			h.logger.Debug("injecting pending results", "session", sessionID, "steps", len(session.PendingResults))
			chatReq.Messages = InjectResults(
				chatReq.Messages,
				session.LastAssistantMessage,
				session.PendingResults,
				h.config.InjectionRole,
			)
			session.PendingResults = nil
			session.ExecutionDone = nil
			h.metrics.Injections.Add(1)
		}
		session.Mu.Unlock()
	}

	// Inject system prompt if configured.
	if h.systemPrompt != "" {
		chatReq.Messages = injectSystemPrompt(chatReq.Messages, h.systemPrompt, h.config.SystemPromptMode)
	}

	clientWantsStream := chatReq.Stream
	if clientWantsStream {
		h.handleStreamingRequest(w, r, chatReq, sessionID)
	} else {
		h.handleNonStreamingRequest(w, r, chatReq, sessionID)
	}
}

// streamResult holds the outcome of processStream.
type streamResult struct {
	PlanText      string
	AssistantText string
	ToolCallID    string // non-empty if loom_describe was called
}

// injectLoomDescribeTool appends the loom_describe tool to req.Tools, unless already present or disabled.
func (h *Handler) injectLoomDescribeTool(req ChatCompletionRequest) ChatCompletionRequest {
	if !h.config.LoomDescribeEnabled {
		return req
	}
	for _, t := range req.Tools {
		if t.Function.Name == "loom_describe" {
			return req // already present
		}
	}
	req.Tools = append(req.Tools, LoomDescribeToolDef())
	return req
}

// handleStreamingRequest processes a streaming request end-to-end.
//
// v2 flow:
//  1. Forward to upstream, relay SSE to client.
//  2. After [DONE], if a plan was detected, launch background execution.
//  3. Next request for this session will block until execution finishes, then inject results.
func (h *Handler) handleStreamingRequest(w http.ResponseWriter, r *http.Request, chatReq ChatCompletionRequest, sessionID string) {
	chatReq.Stream = true
	chatReq = h.injectLoomDescribeTool(chatReq)

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

	// Set up SSE response headers.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	sseWriter := NewSSEWriter(w)
	result := h.processStream(r.Context(), upstreamResp.Body, sseWriter, h.config.PlanVisibility)

	if result.ToolCallID != "" {
		// LLM called loom_describe — serve spec, re-call upstream, and finish.
		h.handleLoomDescribeCall(w, r, chatReq, result.ToolCallID, sseWriter, sessionID)
		return
	}

	// If plan detected, emit indicator text before [DONE].
	if result.PlanText != "" && h.config.PlanVisibility == TeeModeIndicator {
		sseWriter.WriteContent(h.config.IndicatorText) //nolint:errcheck
	}

	sseWriter.WriteDone() //nolint:errcheck

	// === EGRESS: Launch background execution if plan detected. ===
	if result.PlanText != "" {
		h.logger.Debug("plan detected, launching background execution", "session", sessionID)
		h.metrics.PlansDetected.Add(1)
		h.executeInBackground(sessionID, result.PlanText, result.AssistantText)
	}
}

// handleLoomDescribeCall handles the case where the LLM called loom_describe.
// It injects the spec as a tool result and makes a second upstream call,
// streaming the final response to the client.
func (h *Handler) handleLoomDescribeCall(w http.ResponseWriter, r *http.Request, origReq ChatCompletionRequest, toolCallID string, sw *SSEWriter, sessionID string) {
	// Build messages: original + assistant(tool_calls) + tool(spec result)
	specMessages := append(origReq.Messages,
		Message{
			Role: "assistant",
			ToolCalls: []ToolCall{{
				ID:   toolCallID,
				Type: "function",
				Function: ToolCallFunction{Name: "loom_describe", Arguments: "{}"},
			}},
		},
		Message{
			Role:       "tool",
			ToolCallID: toolCallID,
			Content:    BuildLoomSpec(),
		},
	)
	secondReq := origReq
	secondReq.Messages = specMessages
	secondReq.Tools = nil // don't re-inject to avoid infinite loop

	resp, err := h.forwardToUpstream(r, secondReq)
	if err != nil {
		h.logger.Error("loom_describe second upstream call failed", "err", err)
		sw.WriteDone() //nolint:errcheck
		return
	}
	defer resp.Body.Close()

	result := h.processStream(r.Context(), resp.Body, sw, h.config.PlanVisibility)
	if result.PlanText != "" {
		if h.config.PlanVisibility == TeeModeIndicator {
			sw.WriteContent(h.config.IndicatorText) //nolint:errcheck
		}
		h.metrics.PlansDetected.Add(1)
		h.executeInBackground(sessionID, result.PlanText, result.AssistantText)
	}
	sw.WriteDone() //nolint:errcheck
}

// handleNonStreamingRequest processes a non-streaming request.
// Internally always uses streaming for plan detection, then assembles a JSON response.
func (h *Handler) handleNonStreamingRequest(w http.ResponseWriter, r *http.Request, chatReq ChatCompletionRequest, sessionID string) {
	chatReq.Stream = true
	chatReq = h.injectLoomDescribeTool(chatReq)

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

	// Accumulate content via a capturing writer.
	capWriter := &capturingSSEWriter{}
	result := h.processStream(r.Context(), upstreamResp.Body, capWriter, h.config.PlanVisibility)

	// If loom_describe was called, make a second upstream call and accumulate that response.
	if result.ToolCallID != "" {
		specMessages := append(chatReq.Messages,
			Message{
				Role: "assistant",
				ToolCalls: []ToolCall{{
					ID:   result.ToolCallID,
					Type: "function",
					Function: ToolCallFunction{Name: "loom_describe", Arguments: "{}"},
				}},
			},
			Message{
				Role:       "tool",
				ToolCallID: result.ToolCallID,
				Content:    BuildLoomSpec(),
			},
		)
		secondReq := chatReq
		secondReq.Messages = specMessages
		secondReq.Tools = nil

		resp2, err := h.forwardToUpstream(r, secondReq)
		if err != nil {
			http.Error(w, "loom_describe second upstream call failed: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer resp2.Body.Close()

		capWriter = &capturingSSEWriter{}
		result = h.processStream(r.Context(), resp2.Body, capWriter, h.config.PlanVisibility)
	}

	// Determine what the client sees as the final content.
	finalContent := capWriter.String()

	// === EGRESS: Launch background execution if plan detected. ===
	if result.PlanText != "" {
		h.logger.Debug("plan detected (non-streaming), launching background execution", "session", sessionID)
		h.metrics.PlansDetected.Add(1)
		h.executeInBackground(sessionID, result.PlanText, result.AssistantText)
	}

	// Assemble JSON response.
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

// processStream reads the upstream SSE body, forwards content to sw per visibility config,
// and returns a streamResult. If no plan was detected, PlanText and AssistantText are empty.
// If loom_describe was called, ToolCallID is non-empty.
//
// v2 simplification: no mid-stream stop, no ActionPlanComplete. Plan completion
// is determined after [DONE] via detector.HasPlan().
func (h *Handler) processStream(ctx context.Context, body io.Reader, sw sseWriterIface, visibility TeeMode) streamResult {
	detector := &PlanDetector{}
	// lastBufLen tracks how many bytes of partial-line content we've sent to the
	// client via ActionBuffer, so ActionForward can send only the remaining delta.
	var lastBufLen int
	var toolCallID, toolCallName string

	err := ParseSSEStream(body, func(data []byte) error {
		// Check for tool_call before content processing.
		if id, name, ok := ChunkToolCall(data); ok {
			if toolCallID == "" {
				toolCallID = id
			}
			if toolCallName == "" {
				toolCallName = name
			}
		}

		content, ok := ChunkContent(data)
		if !ok {
			// Non-content chunk (role delta, etc.) — forward unless in plan context.
			if !detector.HasPlan() || visibility == TeeModePassthrough {
				return sw.WriteChunk(data)
			}
			return nil
		}

		actions := detector.Feed(content)
		for _, act := range actions {
			switch act.Type {
			case ActionForward:
				// Complete line in StateIdle — adjust for any partial already sent.
				if lastBufLen > 0 {
					// The first lastBufLen bytes were already forwarded via ActionBuffer.
					if len(act.Content) > lastBufLen {
						remaining := act.Content[lastBufLen:]
						if err := sw.WriteContent(remaining); err != nil {
							return err
						}
					}
					lastBufLen = 0
				} else {
					if err := sw.WriteContent(act.Content); err != nil {
						return err
					}
				}
			case ActionBuffer:
				// Partial line in StateIdle — forward only the new delta.
				if !detector.HasPlan() {
					delta := act.Content
					if lastBufLen > 0 && len(act.Content) > lastBufLen {
						delta = act.Content[lastBufLen:]
					} else if lastBufLen > 0 {
						delta = ""
					}
					lastBufLen = len(act.Content)
					if delta != "" {
						if err := sw.WriteContent(delta); err != nil {
							return err
						}
					}
				}
			case ActionSuppress:
				// Plan content — suppress unless passthrough mode.
				if visibility == TeeModePassthrough {
					if err := sw.WriteContent(act.Content); err != nil {
						return err
					}
				}
				// Reset lastBufLen since we've transitioned out of idle.
				if lastBufLen > 0 {
					lastBufLen = 0
				}
			}
		}
		return nil
	})

	if err != nil {
		h.logger.Error("SSE stream error", "err", err)
	}

	// If loom_describe was called, return immediately with the tool call info.
	if toolCallName == "loom_describe" {
		return streamResult{ToolCallID: toolCallID}
	}

	if !detector.HasPlan() {
		return streamResult{}
	}

	return streamResult{
		PlanText:      detector.PlanText(),
		AssistantText: detector.PrePlanText() + detector.PlanText(),
	}
}

// resolveSessionID determines the session ID for this request.
// Priority: X-Loom-Session-ID header > derived from messages prefix.
func (h *Handler) resolveSessionID(r *http.Request, messages []Message) string {
	if id := r.Header.Get("X-Loom-Session-ID"); id != "" {
		return id
	}
	return DeriveSessionID(messages)
}

// waitForPendingExecution blocks until any in-flight background execution for
// the session completes, or the context is cancelled.
func (h *Handler) waitForPendingExecution(ctx context.Context, session *SessionState) error {
	session.Mu.Lock()
	done := session.ExecutionDone
	session.Mu.Unlock()

	if done == nil {
		return nil
	}

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for pending plan execution: %w", ctx.Err())
	}
}

// executeInBackground stores the execution channel in the session and launches
// a goroutine to execute the plan. Results are written to session.PendingResults
// when execution completes.
func (h *Handler) executeInBackground(sessionID, planText, assistantText string) {
	session := h.sessions.GetOrCreate(sessionID)
	done := make(chan struct{})

	session.Mu.Lock()
	session.ExecutionDone = done
	session.LastAssistantMessage = assistantText
	session.Mu.Unlock()

	h.metrics.PendingExecs.Add(1)
	go func() {
		defer close(done)
		defer h.metrics.PendingExecs.Add(-1)

		ctx, cancel := context.WithTimeout(context.Background(), h.config.Timeout)
		defer cancel()

		results := collectStepResults(ctx, h.loom, planText)

		session.Mu.Lock()
		session.PendingResults = results
		session.Mu.Unlock()

		h.logger.Debug("background execution complete", "session", sessionID, "steps", len(results))
	}()
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

	req.Header.Set("Content-Type", "application/json")
	if auth := r.Header.Get("Authorization"); auth != "" {
		req.Header.Set("Authorization", auth)
	}
	if accept := r.Header.Get("Accept"); accept != "" {
		req.Header.Set("Accept", accept)
	}

	// Auth override.
	if h.config.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+h.config.APIKey)
	}

	return h.httpClient.Do(req)
}

// proxyPassthrough forwards a request to targetURL and relays the response unchanged.
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

	for key, values := range r.Header {
		for _, v := range values {
			req.Header.Add(key, v)
		}
	}

	if h.config.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+h.config.APIKey)
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		http.Error(w, "upstream request failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

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

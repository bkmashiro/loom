# Loom Proxy — Design Document

> A transparent streaming proxy that intercepts Loom execution plans from LLM output,
> executes them in parallel, and injects results back for a final natural-language response.

---

## 1. Overview

Loom Proxy is an HTTP reverse proxy that sits between any AI client (CLI, web UI, agent framework) and an LLM API. It speaks the OpenAI chat completions protocol — both streaming (`stream: true`) and non-streaming — making it a drop-in replacement for any OpenAI-compatible endpoint.

### Full Flow

```
┌──────────┐        ┌──────────────────────────────────────────────────────┐        ┌──────────┐
│          │  POST   │                   LOOM PROXY                         │  POST   │          │
│  Client  │───────>│  ┌─────────┐   ┌───────────┐   ┌──────────────────┐ │───────>│ Upstream │
│ (any AI  │        │  │ Ingress │   │  Stream   │   │  Plan Detector   │ │        │  LLM API │
│  client) │<───────│  │ Handler │<──│   Tee     │<──│  State Machine   │ │<───────│          │
│          │  SSE   │  └─────────┘   └───────────┘   └────────┬─────────┘ │  SSE   │          │
└──────────┘        │                                         │           │        └──────────┘
                    │                                         │ plan      │
                    │                                    ┌────▼────┐      │
                    │                                    │  Loom   │      │
                    │                                    │ Runtime │      │
                    │                                    └────┬────┘      │
                    │                                         │ results   │
                    │                                    ┌────▼────────┐  │
                    │                                    │  2nd LLM    │  │
                    │                                    │  Call       │──┘
                    │                                    │ (summarize) │
                    │                                    └─────────────┘  │
                    └──────────────────────────────────────────────────────┘
```

### Sequence Diagram

```
Client            Loom Proxy           Upstream LLM          Loom Runtime
  │                   │                     │                     │
  │──POST /v1/chat───>│                     │                     │
  │  completions      │──POST /v1/chat────->│                     │
  │                   │  completions        │                     │
  │                   │<──SSE chunks────────│                     │
  │                   │                     │                     │
  │  (if no plan      │                     │                     │
  │   detected)       │                     │                     │
  │<──SSE chunks──────│  (pass-through)     │                     │
  │                   │                     │                     │
  │  (if plan         │                     │                     │
  │   detected)       │                     │                     │
  │<──"thinking..."───│                     │                     │
  │  (configurable)   │──────────plan text──────────────────────->│
  │                   │                     │                     │
  │                   │<─────────results──────────────────────────│
  │                   │                     │                     │
  │                   │──POST /v1/chat────->│                     │
  │                   │  (with results)     │                     │
  │                   │<──SSE chunks────────│                     │
  │<──SSE chunks──────│  (final answer)     │                     │
  │                   │                     │                     │
```

---

## 2. API Compatibility

### Decision: OpenAI Chat Completions Only

Loom Proxy implements one API format: **OpenAI Chat Completions** (`POST /v1/chat/completions`). This is sufficient because:

1. **Anthropic has first-party OpenAI-compatible endpoints.** The Anthropic API supports `/v1/messages` natively, but most proxy chains (LiteLLM, OpenRouter, and tools like onecli) already translate to/from OpenAI format. By targeting OpenAI format, Loom Proxy slots into any existing proxy chain without additional translation.

2. **Client ecosystem convergence.** OpenAI's chat completions format is the de facto standard. Every major client library, agent framework, and UI speaks it.

3. **Proxy chaining.** When `LOOM_UPSTREAM` points to another proxy (e.g., onecli at `http://localhost:8080`), that proxy handles provider-specific translation. Loom Proxy never needs to know what model or provider is behind the upstream.

### Supported Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `POST /v1/chat/completions` | POST | Main proxy endpoint — forwards to upstream, intercepts plans |
| `GET /health` | GET | Health check |
| `GET /v1/models` | GET | Proxied to upstream (pass-through) |

### Request/Response Format

Standard OpenAI chat completions. Loom Proxy is transparent — it does not modify the request schema or add custom fields. All Loom-specific behavior is configured via server-side env vars / flags, not per-request parameters.

---

## 3. Stream Tee Architecture

The core challenge: simultaneously forward SSE chunks to the client AND accumulate text for Loom fence detection, without buffering the entire response.

### Design: Dual-Writer with Bounded Accumulator

```go
// StreamTee splits an SSE stream from upstream into two paths:
// 1. Client writer — forwards chunks to the client (possibly with modifications)
// 2. Accumulator — feeds text content to the plan detector state machine
type StreamTee struct {
    clientWriter  SSEWriter       // writes SSE events to the client
    detector      *PlanDetector   // fed content tokens as they arrive
    mode          TeeMode         // passthrough | suppress | indicator
    accumulated   strings.Builder // only accumulates content within a detected plan
}

type TeeMode int

const (
    // TeeModePassthrough forwards all chunks to client as-is.
    TeeModePassthrough TeeMode = iota
    // TeeModeSuppress swallows plan fence chunks, only forwards non-plan text.
    TeeModeSuppress
    // TeeModeIndicator replaces plan fences with a configurable indicator.
    TeeModeIndicator
)
```

### How It Works

1. **Upstream SSE parsing.** Each `data: {...}` line from upstream is parsed as an OpenAI `ChatCompletionChunk`. The `delta.content` field is extracted.

2. **Token feeding.** Each content token is fed to the `PlanDetector` state machine (see Section 4). The detector returns a `DetectorAction`:
   - `ActionForward` — send this token to the client as-is
   - `ActionBuffer` — we might be inside a fence opener; hold the token
   - `ActionSuppress` — we are inside a confirmed plan fence; suppress or replace
   - `ActionFlush` — buffered tokens turned out not to be a fence; flush them to client
   - `ActionPlanComplete` — the `return` directive has been seen; trigger Loom execution

3. **Client forwarding.** Based on the action and the configured `TeeMode`, the tee either forwards, suppresses, or replaces the chunk.

4. **No full buffering.** Only the text inside detected plan fences is accumulated (for passing to Loom). Non-plan text streams through with zero additional latency. The bounded accumulator resets after each plan execution.

### SSE Writer Interface

```go
// SSEWriter writes Server-Sent Events to an http.ResponseWriter.
type SSEWriter interface {
    // WriteEvent writes a single SSE data line. Calls Flush().
    WriteEvent(data []byte) error
    // WriteIndicator writes a "thinking..." type indicator chunk.
    WriteIndicator(text string) error
    // Close sends the final `data: [DONE]` event.
    Close() error
}
```

### Non-Streaming Mode

When the client sends `stream: false`, Loom Proxy:
1. Overrides to `stream: true` for the upstream call (so it can detect plans mid-generation)
2. Accumulates the full response
3. If a plan is detected: executes it, makes the second LLM call, returns the final response as a single JSON object
4. If no plan: returns the upstream response as-is (reassembled from chunks)

---

## 4. Plan Detection State Machine

The detector must identify Loom plan boundaries in a token stream without false positives. Key insight: a Loom plan is a sequence of annotated code fences (with known type keywords) followed by a bare `return <id>` directive.

### State Machine

```
                 ┌──────────────────────────────────────────────┐
                 │                                              │
                 ▼                                              │
          ┌─────────────┐   fence opener     ┌──────────────┐  │
    ──────│   IDLE      │───────────────────>│  IN_FENCE    │  │
          │             │                    │              │  │
          │  (forward   │<───────────────────│  (accumulate │  │
          │   all)      │   fence closer     │   body)      │  │
          └──────┬──────┘                    └──────────────┘  │
                 │                                              │
                 │  "return <id>" outside fence                 │
                 ▼                                              │
          ┌─────────────┐                                      │
          │ PLAN_DONE   │──────────────────────────────────────┘
          │             │   (after Loom exec + 2nd LLM call)
          │  (trigger   │
          │   execution)│
          └─────────────┘
```

### States

```go
type DetectorState int

const (
    // StateIdle — no plan context. Looking for a fence opener with a known
    // Loom step type keyword (io, write, pure, shell, async, escape).
    StateIdle DetectorState = iota

    // StateInFence — inside a code fence. Accumulating body lines.
    StateInFence

    // StateBetweenSteps — at least one valid step has been seen. Looking for
    // another fence opener or a "return" directive. If we see non-fence,
    // non-return text, we stay here (LLMs often emit brief prose between steps).
    StateBetweenSteps

    // StatePlanComplete — "return <id>" seen. Plan is ready for execution.
    StatePlanComplete
)
```

### Detection Rules

1. **Fence opener recognition.** A line matching `` ^```(io|write|pure|shell|async|escape) `` transitions from `StateIdle` or `StateBetweenSteps` to `StateInFence`. The known-keyword requirement prevents false positives on normal code fences (e.g., `` ```python ``).

2. **Fence closer.** A line matching `` ^``` `` (exactly three backticks, no trailing word chars) closes the fence. Transition to `StateBetweenSteps`.

3. **Return directive.** A line matching `^return [a-zA-Z_][a-zA-Z0-9_]*$` in `StateBetweenSteps` transitions to `StatePlanComplete`.

4. **No plan fallback.** If the upstream response completes (`data: [DONE]`) without ever entering `StatePlanComplete`, the response was plan-free. All tokens were forwarded to the client (or flushed from the buffer). No Loom execution occurs.

5. **Token-level buffering.** Because SSE tokens may split a fence opener across chunks (e.g., token "```" then token "io fetch"), the detector buffers partial lines and only evaluates on newline boundaries. This is implemented as a simple line accumulator:

```go
type PlanDetector struct {
    state       DetectorState
    lineBuf     strings.Builder  // partial line accumulator
    planText    strings.Builder  // full plan text (for Loom)
    stepCount   int              // number of completed steps seen
    fenceDepth  int              // 0 = outside fence, 1 = inside
    returnID    string           // set when "return X" is seen
}

// Feed processes a content token. Returns actions for the tee.
func (d *PlanDetector) Feed(token string) []DetectorAction

// DetectorAction tells the tee what to do with buffered content.
type DetectorAction struct {
    Type    ActionType
    Content string  // text to forward/flush (empty for suppress/planComplete)
}

type ActionType int

const (
    ActionForward      ActionType = iota // send content to client
    ActionBuffer                         // hold, might be fence start
    ActionSuppress                       // plan content, don't show
    ActionFlush                          // false alarm, flush buffer
    ActionPlanComplete                   // plan ready, trigger execution
)
```

### Edge Cases

- **No plan at all.** The most common case. Detector stays in `StateIdle`, everything forwards. Zero overhead beyond per-token string comparison.
- **Code fences that aren't Loom steps.** `` ```python `` does not match a Loom type keyword, so it's forwarded as-is.
- **Multiple plans in one response.** Not supported in v1. If a second set of fences appears after a `return`, they are forwarded as text.
- **Missing `return` directive.** If fences are seen but no `return`, the response is treated as plan-free. The accumulated fence text is flushed to the client.

---

## 5. Result Injection Protocol

After Loom executes the plan, Loom Proxy makes a second LLM API call to generate a natural-language summary incorporating the step results.

### Second Call Message Structure

```json
{
  "model": "<same model as original request>",
  "stream": true,
  "messages": [
    // ... all messages from the original request ...
    {
      "role": "assistant",
      "content": "<text the LLM generated BEFORE the plan fences>"
    },
    {
      "role": "user",
      "content": "Here are the results of the execution plan:\n\n<results>\n<step id=\"fetch_user\" status=\"ok\">\n{\"id\": 42, \"name\": \"Alice\"}\n</step>\n<step id=\"fetch_posts\" status=\"ok\">\n[{\"title\": \"Hello World\"}]\n</step>\n<step id=\"build_feed\" status=\"ok\">\n[{\"title\": \"Hello World\", \"score\": 0.95}]\n</step>\n</results>\n\nPlease provide a natural response to the user based on these results. Do not output any Loom execution plans."
    }
  ]
}
```

### Design Decisions

1. **XML-tagged results.** Step results are wrapped in `<step id="..." status="ok|error">` tags. This is unambiguous, well-handled by all LLMs, and allows the model to reference specific steps.

2. **Status field.** Each step has `status="ok"` or `status="error"`. On error, the content is the error message. This lets the LLM generate a graceful error response rather than the proxy returning a raw error.

3. **Pre-plan text preserved.** If the LLM wrote prose before the plan (e.g., "Let me look that up for you"), that text becomes an `assistant` message to maintain conversational continuity.

4. **Suppression instruction.** The injected user message explicitly says "Do not output any Loom execution plans" to prevent recursive plan generation.

5. **Same model.** The second call uses the same model as the original request. This ensures consistent voice and capability.

### Go Structure

```go
// ResultInjector builds the second LLM call from original messages + plan results.
type ResultInjector struct {
    originalMessages []Message      // from the client's request
    prePlanText      string         // assistant text before plan fences
    results          []StepResult   // from Loom execution
    model            string         // model from original request
}

// BuildRequest constructs the OpenAI chat completion request for the summary call.
func (ri *ResultInjector) BuildRequest() ChatCompletionRequest {
    messages := make([]Message, 0, len(ri.originalMessages)+2)
    messages = append(messages, ri.originalMessages...)

    if ri.prePlanText != "" {
        messages = append(messages, Message{
            Role:    "assistant",
            Content: ri.prePlanText,
        })
    }

    messages = append(messages, Message{
        Role:    "user",
        Content: ri.formatResults(),
    })

    return ChatCompletionRequest{
        Model:    ri.model,
        Messages: messages,
        Stream:   true,
    }
}

func (ri *ResultInjector) formatResults() string {
    var sb strings.Builder
    sb.WriteString("Here are the results of the execution plan:\n\n<results>\n")
    for _, r := range ri.results {
        status := "ok"
        if r.Err != nil {
            status = "error"
        }
        fmt.Fprintf(&sb, "<step id=%q status=%q>\n", r.StepID, status)
        if r.Err != nil {
            sb.WriteString(r.Err.Error())
        } else {
            data, _ := json.Marshal(r.Data)
            sb.Write(data)
        }
        sb.WriteString("\n</step>\n")
    }
    sb.WriteString("</results>\n\n")
    sb.WriteString("Please provide a natural response to the user based on these results. ")
    sb.WriteString("Do not output any Loom execution plans.")
    return sb.String()
}
```

### System Prompt Injection

To make the LLM generate Loom plans, a system prompt must be injected into the first LLM call. This is configurable:

- `LOOM_SYSTEM_PROMPT_FILE` — path to a file containing the Loom instruction prompt to prepend to (or merge into) the system message.
- `LOOM_SYSTEM_PROMPT_MODE` — `prepend` (default) | `append` | `replace`. Controls how the Loom instructions are merged with any existing system message.
- If unset, no system prompt modification occurs — the upstream model must already know how to generate Loom plans (e.g., via fine-tuning or prior instructions).

---

## 6. Multi-Proxy Chaining

Loom Proxy is designed to be one link in a proxy chain. It does not need to be the first or last proxy.

### Chain Topology

```
Client ──> Loom Proxy ──> onecli ──> Anthropic API
                │
                └──> (2nd call also goes through onecli)

Client ──> Auth Proxy ──> Loom Proxy ──> OpenRouter ──> Any LLM
```

### Configuration

```
LOOM_UPSTREAM=http://localhost:8080/v1    # upstream base URL
```

Loom Proxy appends `/chat/completions` to `LOOM_UPSTREAM` for LLM calls and `/models` for model listing.

### Extracting the Upstream

The proxy reads `LOOM_UPSTREAM` and constructs:
- LLM endpoint: `${LOOM_UPSTREAM}/chat/completions`
- Models endpoint: `${LOOM_UPSTREAM}/models`

If `LOOM_UPSTREAM` already ends in `/v1`, no `/v1` is added.

### Auth Forwarding

Loom Proxy forwards the `Authorization` header from the client request to the upstream, unless `LOOM_API_KEY` is set, in which case it uses that instead. This allows:
- **Client-provided keys:** client sends `Authorization: Bearer sk-...`, Loom Proxy forwards it.
- **Server-side key:** `LOOM_API_KEY=sk-...` overrides client auth for the upstream.

For the second LLM call (result injection), the same auth is used.

### Chain Position Independence

Because Loom Proxy only processes SSE content tokens and does not modify request headers or response metadata (beyond content), it works at any position in a proxy chain. Upstream and downstream proxies that add auth, rate limiting, logging, etc. are fully compatible.

---

## 7. Configuration

All configuration is via environment variables, with optional CLI flag overrides.

| Env Var | Flag | Default | Description |
|---|---|---|---|
| `LOOM_ADDR` | `--addr` | `:8081` | Listen address |
| `LOOM_UPSTREAM` | `--upstream` | `http://localhost:8080/v1` | Upstream LLM API base URL |
| `LOOM_API_KEY` | `--api-key` | (none) | Override auth for upstream calls |
| `LOOM_PLAN_VISIBILITY` | `--plan-visibility` | `suppress` | `passthrough`, `suppress`, or `indicator` |
| `LOOM_INDICATOR_TEXT` | `--indicator-text` | `Executing plan...` | Text shown to client when plan is running (only for `indicator` mode) |
| `LOOM_TIMEOUT` | `--timeout` | `120s` | Per-request timeout (covers both LLM calls + plan execution) |
| `LOOM_PLAN_TIMEOUT` | `--plan-timeout` | `30s` | Timeout for Loom plan execution only |
| `LOOM_SYSTEM_PROMPT_FILE` | `--system-prompt-file` | (none) | Path to Loom system prompt to inject |
| `LOOM_SYSTEM_PROMPT_MODE` | `--system-prompt-mode` | `prepend` | How to merge: `prepend`, `append`, `replace` |
| `LOOM_LOG_LEVEL` | `--log-level` | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `LOOM_MAX_PLAN_SIZE` | `--max-plan-size` | `65536` | Max bytes of plan text before rejecting (safety limit) |

### Plan Visibility Modes

| Mode | Behavior |
|---|---|
| `passthrough` | Client sees the raw plan fences in the stream, then sees the final answer after a pause |
| `suppress` | Plan fences are silently swallowed; client only sees the final answer |
| `indicator` | Plan fences are replaced with a configurable indicator message; client sees the indicator, then the final answer |

---

## 8. Error Handling

### Plan Execution Failures

When one or more plan steps fail:

1. **Partial results are still injected.** The second LLM call receives all results, including errors (with `status="error"`). The LLM is instructed to handle errors gracefully — e.g., "I was able to fetch your profile but the recommendations service is currently unavailable."

2. **Total plan failure.** If the Loom runtime returns a top-level error (e.g., all steps failed, context timeout), Loom Proxy:
   - If `plan_visibility` is `passthrough`: the client already saw the plan text. Send an SSE error event.
   - If `plan_visibility` is `suppress` or `indicator`: make the second LLM call with all step errors, letting the LLM generate a user-friendly error message.

### Upstream API Errors

| Scenario | Behavior |
|---|---|
| Upstream returns non-200 on first call | Forward the error response to client as-is |
| Upstream SSE stream drops mid-response (no plan detected yet) | Forward whatever was received; client sees a truncated response |
| Upstream SSE stream drops mid-plan | Treat as plan-free; flush accumulated text to client |
| Upstream returns non-200 on second call (result injection) | Send an SSE error event to client with the upstream error |
| Upstream timeout | Return 504 to client |

### Error SSE Event

When Loom Proxy needs to signal an error during streaming:

```
data: {"error": {"message": "Plan execution failed: step fetch_user timed out", "type": "loom_proxy_error", "code": "plan_execution_failed"}}

data: [DONE]
```

This is non-standard OpenAI but commonly understood by clients. The `type: "loom_proxy_error"` field distinguishes Loom errors from upstream errors.

---

## 9. State Management (Multi-Turn)

### Decision: Stateless Proxy, Client Owns History

Loom Proxy is **stateless**. It does not maintain conversation history across requests. Each request contains the full `messages` array, as is standard for the OpenAI chat completions API.

### What the Client Sees

From the client's perspective, the assistant's response is the **final answer** from the second LLM call. The plan fences, step results, and injected user message are internal to Loom Proxy and never appear in the response that the client stores in its history.

### Conversation History Fidelity

This means the conversation history is:

```
User: "What are my latest posts?"
Assistant: "Here are your latest posts: 1. Hello World (posted 2h ago)..."
User: "Tell me more about the first one"
Assistant: "..."
```

The LLM in the second call sees the original messages + results, so it has full context. But on the next turn, the LLM only sees the final answer in history, not the raw step data. This is acceptable because:

1. **Follow-up questions are a new request.** If the user asks about something from the previous answer, the previous answer text contains enough information (it was generated by the LLM specifically to be a useful summary).

2. **No state leakage.** Intermediate plan data does not accumulate in the conversation, keeping token counts manageable.

3. **Client compatibility.** Every client that stores `messages` works without modification.

### Future Enhancement: Optional History Injection

A future version could accept a `X-Loom-Include-Results: true` header that causes the proxy to include step results in the response metadata (e.g., in a custom `loom_results` field on the response object). Clients that understand this field could include it in follow-up requests for richer context.

---

## 10. Implementation Plan

### File Structure

```
cmd/loom-proxy/
    main.go              # CLI entry point, flag parsing, server startup

pkg/proxy/
    proxy.go             # Core proxy handler (http.Handler)
    proxy_test.go        # Integration tests

    tee.go               # StreamTee implementation
    tee_test.go

    detector.go          # PlanDetector state machine
    detector_test.go

    injector.go          # ResultInjector for second LLM call
    injector_test.go

    sse.go               # SSE parser and writer utilities
    sse_test.go

    config.go            # Configuration struct, env/flag parsing
```

### Phase 1: Transparent Pass-Through Proxy (Week 1)

**Goal:** A working proxy that forwards all requests to upstream unchanged and returns responses unchanged. Proves the SSE relay works.

**Files:** `cmd/loom-proxy/main.go`, `pkg/proxy/proxy.go`, `pkg/proxy/sse.go`, `pkg/proxy/config.go`

**Acceptance Criteria:**
- `curl http://localhost:8081/v1/chat/completions -d '{"model":"...","messages":[...],"stream":true}'` produces identical SSE output as calling upstream directly
- Non-streaming requests produce identical JSON output
- `Authorization` header is forwarded
- `/health` returns 200
- `/v1/models` is proxied

### Phase 2: Plan Detection (Week 2)

**Goal:** The `PlanDetector` state machine correctly identifies Loom plans in a token stream without false positives.

**Files:** `pkg/proxy/detector.go`, `pkg/proxy/detector_test.go`, `pkg/proxy/tee.go`, `pkg/proxy/tee_test.go`

**Acceptance Criteria:**
- Unit tests: streams with no fences → all tokens forwarded, zero allocations in hot path
- Unit tests: streams with non-Loom fences (`` ```python ``) → all tokens forwarded
- Unit tests: streams with valid Loom plans → plan text captured, `ActionPlanComplete` emitted
- Unit tests: streams with Loom-like fences but no `return` → treated as plan-free, text flushed
- Unit tests: tokens split across chunk boundaries (mid-line splits)
- Fuzz test: random token splits of known plans always produce correct detection

### Phase 3: Loom Integration + Result Injection (Week 3)

**Goal:** Detected plans are executed via the Loom runtime, and results are injected into a second LLM call.

**Files:** `pkg/proxy/injector.go`, `pkg/proxy/injector_test.go`, update `pkg/proxy/proxy.go`

**Acceptance Criteria:**
- End-to-end test: client request → plan generated → Loom executes → second LLM call → final answer streamed to client
- Error handling: failed steps appear as `status="error"` in injected results
- Plan timeout: execution respects `LOOM_PLAN_TIMEOUT`
- Plan visibility modes all work (`passthrough`, `suppress`, `indicator`)

### Phase 4: Polish + Non-Streaming + System Prompt (Week 4)

**Goal:** Non-streaming mode, system prompt injection, configuration finalization.

**Files:** update `pkg/proxy/proxy.go`, `pkg/proxy/config.go`

**Acceptance Criteria:**
- Non-streaming requests with plans work correctly (response is a single JSON object)
- `LOOM_SYSTEM_PROMPT_FILE` injects Loom instructions into the system message
- `LOOM_API_KEY` override works
- Graceful shutdown
- Structured logging with `LOOM_LOG_LEVEL`
- README with usage examples

---

## 11. Key Interfaces

### Core Handler

```go
// Handler is the main HTTP handler for the Loom Proxy.
type Handler struct {
    upstream    *url.URL
    httpClient  *http.Client
    loom        *loom.Loom
    config      Config
    systemPrompt string  // loaded from file at startup
}

// ServeHTTP handles /v1/chat/completions requests.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request)
```

### SSE Utilities

```go
// ParseSSEStream reads SSE events from an io.Reader, calling fn for each event.
// Stops on "data: [DONE]" or reader EOF. Returns first error encountered.
func ParseSSEStream(r io.Reader, fn func(data []byte) error) error

// ChunkContent extracts the delta.content string from an OpenAI SSE chunk.
// Returns ("", false) if the chunk has no content delta.
func ChunkContent(data []byte) (string, bool)

// NewSSEWriter creates an SSEWriter that writes to w and flushes after each event.
func NewSSEWriter(w http.ResponseWriter) SSEWriter
```

### Proxy Flow (Pseudocode)

```go
func (h *Handler) handleStream(w http.ResponseWriter, req ChatCompletionRequest) {
    // 1. Optionally inject system prompt
    if h.systemPrompt != "" {
        req.Messages = injectSystemPrompt(req.Messages, h.systemPrompt, h.config.SystemPromptMode)
    }

    // 2. Forward to upstream with stream:true
    upstreamResp, err := h.forwardToUpstream(req)
    if err != nil { writeError(w, err); return }
    defer upstreamResp.Body.Close()

    // 3. Set up stream tee + plan detector
    sseWriter := NewSSEWriter(w)
    detector := NewPlanDetector()
    tee := NewStreamTee(sseWriter, detector, h.config.PlanVisibility)

    // 4. Process upstream SSE stream
    var prePlanText strings.Builder
    planDetected := false

    ParseSSEStream(upstreamResp.Body, func(data []byte) error {
        content, ok := ChunkContent(data)
        if !ok {
            // Non-content chunk (e.g., tool_calls, role) — forward as-is
            return tee.ForwardRaw(data)
        }

        actions := detector.Feed(content)
        for _, action := range actions {
            switch action.Type {
            case ActionForward:
                if detector.State() == StateIdle {
                    prePlanText.WriteString(action.Content)
                }
                tee.Forward(action.Content)
            case ActionSuppress:
                // plan content — suppressed based on visibility mode
                tee.Suppress(action.Content)
            case ActionPlanComplete:
                planDetected = true
                return errPlanComplete // sentinel to break out of SSE loop
            case ActionFlush:
                prePlanText.WriteString(action.Content)
                tee.Flush(action.Content)
            }
        }
        return nil
    })

    if !planDetected {
        sseWriter.Close()
        return // normal response, no plan
    }

    // 5. Execute the Loom plan
    planText := detector.PlanText()
    ctx, cancel := context.WithTimeout(r.Context(), h.config.PlanTimeout)
    defer cancel()

    result, err := h.loom.Run(ctx, strings.NewReader(planText))

    // 6. Collect all step results
    // (Run returns the final result; we need all step results for injection)
    // Use Stream() instead for full visibility:
    stepResults := collectAllResults(ctx, h.loom, planText)

    // 7. Build and execute second LLM call
    injector := &ResultInjector{
        originalMessages: req.Messages,
        prePlanText:      prePlanText.String(),
        results:          stepResults,
        model:            req.Model,
    }
    secondReq := injector.BuildRequest()

    secondResp, err := h.forwardToUpstream(secondReq)
    if err != nil { writeSSEError(sseWriter, err); return }
    defer secondResp.Body.Close()

    // 8. Forward second response to client
    ParseSSEStream(secondResp.Body, func(data []byte) error {
        return sseWriter.WriteEvent(data)
    })
    sseWriter.Close()
}
```

---

## 12. Open Questions

### Q1: Should the proxy support recursive plans?

If the second LLM call also generates a Loom plan, should the proxy execute it? Current design says no — the suppression instruction ("Do not output any Loom execution plans") prevents this. But a `LOOM_MAX_DEPTH` config could enable controlled recursion in the future.

**Recommendation:** No recursion in v1. Add `LOOM_MAX_DEPTH` (default 1) in v2 if needed.

### Q2: How to collect all step results when using `Run()`?

`loom.Run()` returns only the final return step's result. For the injection message, we want all step results. Options:
- Use `loom.Stream()` and drain the channel, collecting results into a slice.
- Add a `loom.RunAll()` method that returns `[]StepResult`.
- The `ResultInjector` could accept just the return step's result (simpler but less informative).

**Recommendation:** Use `loom.Stream()` and collect. Avoids modifying the Loom core API.

### Q3: Should Loom Proxy support tool_calls in addition to content?

Some models return `tool_calls` in the response delta instead of `content`. If a model uses native tool calling alongside Loom plans, the proxy needs to handle both. Current design ignores `tool_calls` deltas.

**Recommendation:** Pass through `tool_calls` unchanged in v1. They are orthogonal to Loom plans. If a model mixes tool calls and Loom fences, that is undefined behavior.

### Q4: Token counting and cost tracking?

Loom Proxy makes two LLM calls per plan-bearing request. Should it report combined token usage?

**Recommendation:** Sum the `usage` objects from both calls and return the combined total in the final response. Log both individually at `debug` level.

### Q5: Should the return step be required?

Currently, the plan detector requires a `return <id>` directive to trigger execution. But some plans might have all side effects (writes, async) with no meaningful return value.

**Recommendation:** Keep `return` required in v1. For side-effect-only plans, the LLM can write `return <last_write_step_id>` to signal completion. This keeps the state machine simple and unambiguous.

### Q6: Streaming results back during plan execution?

Instead of waiting for all steps to complete before making the second call, could we stream partial results? E.g., show "Fetched user profile..." as each step completes.

**Recommendation:** Defer to v2. This would require a more complex tee that interleaves Loom progress events with SSE chunks, and the second LLM call would need to handle partial results. The complexity is not justified for v1.

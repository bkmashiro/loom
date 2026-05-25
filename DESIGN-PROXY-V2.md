# Loom Proxy v2 — Stateful Session Injection

> Eliminates the second LLM call and `return` directive. Plan execution results
> are injected into the next conversational round, halving API cost and simplifying
> the proxy architecture.

---

## 1. Architecture Overview

Loom Proxy v2 sits between any OpenAI-compatible client and an upstream LLM API, same as v1. The fundamental change: **results from plan execution in round N are injected into the messages of round N+1**, rather than triggering a second LLM call within the same request.

### Block Diagram

```
┌──────────┐        ┌──────────────────────────────────────────────────┐        ┌──────────┐
│          │  POST   │                   LOOM PROXY v2                  │  POST   │          │
│  Client  │───────>│  ┌───────────┐  ┌───────────┐  ┌────────────┐  │───────>│ Upstream │
│ (any AI  │        │  │  Session   │  │  Stream   │  │   Plan     │  │        │  LLM API │
│  client) │<───────│  │  Injector  │  │   Tee     │  │  Detector  │  │<───────│          │
│          │  SSE   │  └─────┬─────┘  └───────────┘  └──────┬─────┘  │  SSE   │          │
└──────────┘        │        │                               │        │        └──────────┘
                    │        │                               │ plan   │
                    │   ┌────▼────┐                    ┌─────▼─────┐  │
                    │   │ Session │                    │   Loom    │  │
                    │   │  Store  │<───results─────────│  Runtime  │  │
                    │   └─────────┘                    └───────────┘  │
                    └──────────────────────────────────────────────────┘
```

### Sequence Diagram

```
Client            Loom Proxy           Upstream LLM          Loom Runtime
  │                   │                     │                     │
  │ ── Round 1 ────────────────────────────────────────────────── │
  │                   │                     │                     │
  │──POST /v1/chat───>│                     │                     │
  │  completions      │──POST /v1/chat────->│                     │
  │                   │  completions        │                     │
  │                   │<──SSE chunks────────│                     │
  │<──SSE chunks──────│  (plan text or      │                     │
  │  (per visibility  │   suppressed)       │                     │
  │   mode)           │                     │                     │
  │                   │<──[DONE]────────────│                     │
  │<──[DONE]──────────│                     │                     │
  │                   │                     │                     │
  │                   │   (stream ended,    │                     │
  │                   │    plan detected)   │                     │
  │                   │──────execute plan──────────────────────-->│
  │                   │<─────results─────────────────────────────│
  │                   │                     │                     │
  │                   │   [store results    │                     │
  │                   │    + assistant text  │                     │
  │                   │    in session]       │                     │
  │                   │                     │                     │
  │ ── Round 2 ────────────────────────────────────────────────── │
  │                   │                     │                     │
  │──POST /v1/chat───>│                     │                     │
  │  completions      │                     │                     │
  │                   │   [inject pending   │                     │
  │                   │    results into     │                     │
  │                   │    messages]        │                     │
  │                   │                     │                     │
  │                   │──POST /v1/chat────->│                     │
  │                   │  (messages now      │                     │
  │                   │   include results)  │                     │
  │                   │<──SSE chunks────────│                     │
  │<──SSE chunks──────│  (natural response  │                     │
  │                   │   incorporating     │                     │
  │                   │   results)          │                     │
  │                   │                     │                     │
```

### Why This Is Better Than v1

| Aspect | v1 | v2 |
|---|---|---|
| LLM calls per plan | 2 (original + summary) | 1 (results seen in next round) |
| `return` directive | Required — plan boundary marker | Eliminated — boundary = `[DONE]` |
| Conflict risk | `return` can confuse LLMs that use it as a language keyword | None |
| API cost | 2x input tokens on summary call | 0 extra calls |
| Proxy complexity | Mid-stream second request, complex tee | Background execution, simple injection |
| Result timing | Blocks the response until execution + summary complete | Execution overlaps with user think time |
| Conversational flow | Synthetic summary, may sound unnatural | LLM sees results in natural message context |

---

## 2. Session Management

### Session State

The proxy maintains lightweight per-session state:

```go
type SessionState struct {
    // PendingResults holds step results from the last plan execution.
    // nil means no pending results (pass-through mode for next request).
    PendingResults []StepResult

    // LastAssistantMessage is the full text the LLM produced in the round
    // that contained the plan. Needed to reconstruct the assistant message
    // for injection (the client may not include it correctly if plan text
    // was suppressed).
    LastAssistantMessage string

    // ExecutionDone is a channel that closes when background plan execution
    // completes. The next request for this session blocks on this before
    // forwarding to upstream.
    ExecutionDone <-chan struct{}

    // LastActivity is updated on every request for TTL expiration.
    LastActivity time.Time

    // Mu protects concurrent access (background executor writes, next
    // request reads).
    Mu sync.Mutex
}
```

### Session Identification

Sessions are identified by one of (in priority order):

1. **Explicit header**: `X-Loom-Session-ID` — client provides a stable session ID.
2. **Derived from messages**: hash the `messages` array up to (but not including) the last user message. This produces a stable identifier for the same conversation across rounds.

```go
func DeriveSessionID(messages []Message) string {
    // Find the last user message boundary
    lastUserIdx := -1
    for i := len(messages) - 1; i >= 0; i-- {
        if messages[i].Role == "user" {
            lastUserIdx = i
            break
        }
    }

    // Hash everything before the last user message
    prefix := messages
    if lastUserIdx > 0 {
        prefix = messages[:lastUserIdx]
    }

    h := sha256.New()
    enc := json.NewEncoder(h)
    enc.Encode(prefix)
    return hex.EncodeToString(h.Sum(nil))[:16]
}
```

### Session Store

```go
type SessionStore struct {
    mu       sync.Mutex
    sessions map[string]*SessionState
    ttl      time.Duration
}

func (s *SessionStore) Get(id string) *SessionState
func (s *SessionStore) Set(id string, state *SessionState)
func (s *SessionStore) cleanup()  // runs on a ticker, removes expired sessions
```

### Session TTL

Sessions expire after **5 minutes** of inactivity (configurable via `LOOM_SESSION_TTL`). The cleanup goroutine runs every 60 seconds.

Memory is bounded: each session stores only the pending results (typically < 10 KB) and the assistant text. Even with thousands of concurrent sessions, memory usage is negligible.

---

## 3. Simplified Plan Detector

### Key Change: No `return` Directive

In v2, plan boundary detection is drastically simplified. Since execution happens after `[DONE]`, the detector only needs to answer: **did this response contain at least one valid Loom fence?**

### State Machine

```
          ┌─────────────┐   fence opener     ┌──────────────┐
    ──────│    IDLE      │──────────────────>│   IN_FENCE   │
          │             │                    │              │
          │  (no Loom   │<───────────────────│  (accumulate │
          │   fences    │   fence closer     │   body)      │
          │   seen)     │                    └──────────────┘
          └──────┬──────┘
                 │  (after first fence closes)
                 ▼
          ┌─────────────┐   fence opener     ┌──────────────┐
          │  HAS_PLAN   │──────────────────>│   IN_FENCE   │
          │             │                    │  (same as    │
          │  (at least  │<───────────────────│   above)     │
          │   one valid │   fence closer     └──────────────┘
          │   fence)    │
          └─────────────┘
```

### States

```go
type DetectorState int

const (
    // StateIdle — no Loom fence seen yet. Forward everything.
    StateIdle DetectorState = iota

    // StateInFence — inside a Loom code fence. Accumulating body.
    StateInFence

    // StateHasPlan — at least one valid Loom fence has completed.
    // Looking for more fences or end of stream.
    StateHasPlan
)
```

### Detection Rules

1. **Fence opener**: a line matching `` ^```(io|write|pure|shell|async|escape) `` transitions to `StateInFence`.

2. **Fence closer**: a line matching `` ^``` `` (three backticks, no trailing word chars) closes the fence. Transitions to `StateHasPlan`.

3. **Stream end with `StateHasPlan`**: trigger plan execution.

4. **Stream end with `StateIdle`**: pass-through, no plan detected.

5. **No `return` detection needed.** The `return` directive is no longer part of the protocol. If present in LLM output, it is treated as part of a fence body or ignored as regular text.

### Simplified Detector

```go
type PlanDetector struct {
    state      DetectorState
    lineBuf    strings.Builder  // partial line accumulator
    planText   strings.Builder  // full plan text (all fences)
    prePlanText strings.Builder // text before the first fence
    stepCount  int              // number of completed fences
}

// Feed processes a content token. Returns actions for the tee.
func (d *PlanDetector) Feed(token string) []DetectorAction

// HasPlan returns true if at least one valid Loom fence was completed.
func (d *PlanDetector) HasPlan() bool {
    return d.state == StateHasPlan || d.state == StateInFence && d.stepCount > 0
}

// PlanText returns the accumulated plan text (all fences).
func (d *PlanDetector) PlanText() string

// PrePlanText returns any assistant text that preceded the first fence.
func (d *PlanDetector) PrePlanText() string
```

### Actions (Simplified)

```go
type ActionType int

const (
    ActionForward      ActionType = iota // send content to client
    ActionBuffer                         // hold, might be fence start
    ActionSuppress                       // plan content, don't show
    ActionFlush                          // false alarm, flush buffer
    // ActionPlanComplete is REMOVED — plan completion is now determined
    // by [DONE] + HasPlan(), not by a mid-stream action.
)
```

### Edge Cases

- **Incomplete fence at `[DONE]`**: if the stream ends while `StateInFence`, treat the partially accumulated fence as regular text. Flush it to the client. Only completed fences count.
- **No fences at all**: the common case. Zero overhead beyond per-token string comparison.
- **Non-Loom fences**: `` ```python `` does not match a Loom type keyword; forwarded as-is.
- **`return` in LLM output**: ignored by the detector. If an LLM still emits `return`, it is harmless — just extra text inside or between fences. The Loom runtime still uses it to determine which step's result to surface, but the proxy does not depend on it for plan boundary detection.

---

## 4. Result Injection Format

When a session has pending results and a new request arrives, the proxy modifies the `messages` array before forwarding to upstream.

### Injection Structure

Results are injected as **two additional messages** between the assistant's round-N response and the user's round-N+1 message:

```json
{
  "messages": [
    // ... original messages from the client up to and including round N-1 ...

    {"role": "assistant", "content": "<LLM's round-N text, including plan fences>"},

    {"role": "tool", "content": "Loom execution results:\n\n<step id=\"fetch_user\" type=\"io\" status=\"ok\">\n{\"name\": \"Alice\", \"id\": 42}\n</step>\n\n<step id=\"fetch_posts\" type=\"io\" status=\"ok\">\n[{\"title\": \"Hello World\"}]\n</step>\n\n<step id=\"build_feed\" type=\"pure\" status=\"ok\">\n[{\"title\": \"Hello World\", \"score\": 0.95}]\n</step>"},

    {"role": "user", "content": "<user's round N+1 message>"}
  ]
}
```

### Injection Role: `tool` vs `user`

**Default: `tool` role.** This is semantically correct (results from tool execution) and widely supported by OpenAI-compatible APIs.

**Fallback: `user` role.** Some upstream providers do not support the `tool` role without a preceding `tool_calls` assistant message. Configurable via `LOOM_INJECTION_ROLE`.

When using `user` role, the content is prefixed to distinguish it from actual user input:

```json
{
  "role": "user",
  "content": "[System: Loom execution results]\n\n<step id=\"fetch_user\" ...>...</step>\n..."
}
```

### Tool Role with `tool_call_id`

Some APIs require a `tool_call_id` on `tool` role messages. When `LOOM_INJECTION_ROLE=tool` (default), the proxy also patches the preceding assistant message to include a synthetic `tool_calls` entry:

```json
{
  "role": "assistant",
  "content": "<pre-plan text>",
  "tool_calls": [{
    "id": "loom_exec_001",
    "type": "function",
    "function": {
      "name": "loom_execute",
      "arguments": "{}"
    }
  }]
}
```

And the tool message includes:

```json
{
  "role": "tool",
  "tool_call_id": "loom_exec_001",
  "content": "Loom execution results:\n..."
}
```

This ensures compatibility with strict OpenAI API validation. The `tool_call_id` is deterministic per session round (e.g., `loom_exec_{round_number:03d}`).

### XML Result Format

Step results use the same XML format as v1 for consistency with the Loom ecosystem:

```xml
<step id="step_id" type="io" status="ok">
{"result": "data"}
</step>

<step id="other_step" type="shell" status="error">
exit code 1: command not found
</step>
```

Fields:
- `id`: the step identifier from the plan
- `type`: the step type (`io`, `write`, `pure`, `shell`, `async`, `escape`)
- `status`: `ok` or `error`
- Body: the result data (JSON for structured data, plain text for errors)

### Message Reconstruction

The client may not have the correct assistant message from round N if plan text was suppressed. The proxy must reconstruct it:

| Visibility Mode | Client saw | Proxy stores as `LastAssistantMessage` |
|---|---|---|
| `passthrough` | Full text including plan fences | Full text including plan fences |
| `suppress` | Only pre-plan prose | Full text including plan fences |
| `indicator` | Pre-plan prose + indicator text | Full text including plan fences |

The proxy always stores the **full** assistant text (including plan fences) because the LLM in round N+1 needs to see what it said to understand the results contextually.

When injecting, the proxy replaces or inserts the stored assistant message regardless of what the client sends, ensuring the LLM sees the complete picture.

---

## 5. Plan Visibility Modes

Same three modes as v1, but behavior is simpler since there is no second LLM call:

| Mode | Client Sees in Round N | Round N+1 Behavior |
|---|---|---|
| `passthrough` | Full plan text streams through, then `[DONE]` | Results injected; LLM responds naturally |
| `suppress` | Only pre-plan prose, then `[DONE]` | Results injected; LLM responds naturally |
| `indicator` | Pre-plan prose + indicator text, then `[DONE]` | Results injected; LLM responds naturally |

In all modes, the client receives `[DONE]` normally after round N. The plan executes in the background. The user types their next message. When that message arrives, results are injected.

### Indicator Text

When `plan_visibility=indicator`, the proxy emits a configurable text chunk after the pre-plan prose:

```
data: {"choices":[{"delta":{"content":"\n\n[Executing plan...]"}}]}
```

The indicator text is **not** included in the stored `LastAssistantMessage` (it's a proxy-only artifact).

---

## 6. Configuration

### Environment Variables

| Env Var | Flag | Default | Description |
|---|---|---|---|
| `LOOM_ADDR` | `--addr` | `:8081` | Listen address |
| `LOOM_UPSTREAM` | `--upstream` | `http://localhost:8080/v1` | Upstream LLM API base URL |
| `LOOM_API_KEY` | `--api-key` | (none) | Override auth for upstream calls |
| `LOOM_PLAN_VISIBILITY` | `--plan-visibility` | `suppress` | `passthrough`, `suppress`, or `indicator` |
| `LOOM_INDICATOR_TEXT` | `--indicator-text` | `Executing plan...` | Text shown in indicator mode |
| `LOOM_TIMEOUT` | `--timeout` | `120s` | Per-request timeout |
| `LOOM_SESSION_TTL` | `--session-ttl` | `5m` | Session expiration after inactivity |
| `LOOM_INJECTION_ROLE` | `--injection-role` | `tool` | Role for injected results: `tool` or `user` |
| `LOOM_SYSTEM_PROMPT_FILE` | `--system-prompt-file` | (none) | Path to Loom system prompt to inject |
| `LOOM_SYSTEM_PROMPT_MODE` | `--system-prompt-mode` | `prepend` | How to merge: `prepend`, `append`, `replace` |
| `LOOM_LOG_LEVEL` | `--log-level` | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `LOOM_MAX_PLAN_SIZE` | `--max-plan-size` | `65536` | Max bytes of plan text (safety limit) |

### Removed from v1

| Removed | Reason |
|---|---|
| `LOOM_PLAN_TIMEOUT` | No longer needed as a separate config. Execution happens in the background after `[DONE]`, overlapping with user think time. The general `LOOM_TIMEOUT` still applies as an upper bound on execution, and `LOOM_SESSION_TTL` ensures stale sessions are cleaned up. |

### New in v2

| New | Purpose |
|---|---|
| `LOOM_SESSION_TTL` | Controls how long session state (pending results) is kept |
| `LOOM_INJECTION_ROLE` | Controls whether results are injected as `tool` or `user` role |

---

## 7. Concurrency Model

### Background Execution After `[DONE]`

When the upstream stream completes and a plan is detected:

1. The proxy sends `[DONE]` to the client immediately (round N is complete from the client's perspective).
2. A background goroutine begins executing the plan via the Loom runtime.
3. Results are written to the session state when execution completes.

```go
func (h *Handler) executeInBackground(sessionID string, planText string, assistantText string) {
    session := h.sessions.GetOrCreate(sessionID)
    done := make(chan struct{})
    session.Mu.Lock()
    session.ExecutionDone = done
    session.LastAssistantMessage = assistantText
    session.Mu.Unlock()

    go func() {
        defer close(done)

        ctx, cancel := context.WithTimeout(context.Background(), h.config.Timeout)
        defer cancel()

        results := h.executePlan(ctx, planText)

        session.Mu.Lock()
        session.PendingResults = results
        session.LastActivity = time.Now()
        session.Mu.Unlock()
    }()
}
```

### Request Ordering Guarantee

When a new request arrives for a session with a pending execution:

1. The proxy checks if `ExecutionDone` channel exists and is not yet closed.
2. If execution is still running, the proxy **blocks** until it completes (with the request's context timeout as an upper bound).
3. Once execution is done, pending results are injected into the messages and the request is forwarded to upstream.
4. After injection, `PendingResults` is cleared.

```go
func (h *Handler) waitForPendingExecution(ctx context.Context, session *SessionState) error {
    session.Mu.Lock()
    done := session.ExecutionDone
    session.Mu.Unlock()

    if done == nil {
        return nil // no pending execution
    }

    select {
    case <-done:
        return nil
    case <-ctx.Done():
        return fmt.Errorf("timed out waiting for pending plan execution: %w", ctx.Err())
    }
}
```

### Concurrency Safety

- `SessionState.Mu` protects all fields from concurrent access.
- The background goroutine writes results; the next request's goroutine reads them.
- `ExecutionDone` channel provides a safe signal mechanism without polling.
- `SessionStore.mu` protects the sessions map itself.

### Non-Streaming Mode

When the client sends `stream: false`:
1. Proxy overrides to `stream: true` for upstream (same as v1, for plan detection).
2. Accumulates the full response.
3. If a plan is detected: sends the reassembled non-streaming response to the client, then executes the plan in the background (results will be injected in the next round).
4. If no plan: returns the upstream response as-is.

The client receives a complete response immediately. Plan results appear in the next round, same as streaming mode.

---

## 8. What Changes From v1

### Removed

| Component | v1 | v2 |
|---|---|---|
| Second LLM call | `ResultInjector.BuildRequest()` makes a second upstream call within the same request | Eliminated entirely |
| `return` directive detection | `StatePlanComplete` triggered by `return <id>` | Removed; `StateHasPlan` triggered by fence completion |
| `StateBetweenSteps` | Needed to look for `return` between fences | Removed; fences transition directly to `StateHasPlan` |
| `ActionPlanComplete` | Emitted when `return` detected | Removed; plan completion determined by `[DONE]` + `HasPlan()` |
| `LOOM_PLAN_TIMEOUT` | Separate timeout for plan execution within a request | Removed; execution is background, bounded by `LOOM_TIMEOUT` |

### Added

| Component | Purpose |
|---|---|
| `SessionState` | Holds pending results between rounds |
| `SessionStore` | In-memory session store with TTL expiration |
| Session injection logic | Modifies incoming `messages` to include pending results |
| Background execution goroutine | Executes plan after `[DONE]`, non-blocking |
| `X-Loom-Session-ID` header | Optional explicit session identification |
| `LOOM_SESSION_TTL` config | Session expiration control |
| `LOOM_INJECTION_ROLE` config | Injection message role control |

### Modified

| Component | Change |
|---|---|
| `PlanDetector` | Simplified: 3 states instead of 4, no `return` detection |
| `StreamTee` | Simpler: always forwards `[DONE]`, no mid-stream plan completion |
| `proxy.go` main handler | Restructured: injection happens on request ingress, execution happens after response egress |
| `injector.go` | Repurposed: no longer builds a second LLM request; instead modifies incoming messages |

### File Changes (Refactor From v1)

```
pkg/proxy/
    proxy.go         # Major refactor: add session injection on ingress,
                     #   background execution on egress
    detector.go      # Simplify: remove StateBetweenSteps, StatePlanComplete,
                     #   return detection
    detector_test.go # Update tests: remove return-based cases, add [DONE]-based
    injector.go      # Repurpose: inject results into incoming messages
                     #   (was: build second LLM request)
    injector_test.go # Rewrite tests for new injection logic
    tee.go           # Simplify: remove ActionPlanComplete handling
    tee_test.go      # Update tests
    session.go       # NEW: SessionState, SessionStore, DeriveSessionID
    session_test.go  # NEW: session management tests
    config.go        # Add LOOM_SESSION_TTL, LOOM_INJECTION_ROLE;
                     #   remove LOOM_PLAN_TIMEOUT
```

---

## 9. Implementation Plan

### Phase 1: Session Infrastructure (2-3 days)

**Goal:** Session store with identification, TTL, and cleanup. No functional changes to proxy behavior yet.

**Files:** `pkg/proxy/session.go`, `pkg/proxy/session_test.go`, `pkg/proxy/config.go`

**Work:**
- Implement `SessionState`, `SessionStore`, `DeriveSessionID`
- TTL-based cleanup goroutine
- Parse `X-Loom-Session-ID` header
- Add `LOOM_SESSION_TTL` and `LOOM_INJECTION_ROLE` to config
- Remove `LOOM_PLAN_TIMEOUT` from config

**Acceptance Criteria:**
- Unit tests: session create/get/expire
- Unit tests: `DeriveSessionID` produces stable hashes for same conversation prefix
- Unit tests: TTL cleanup removes expired sessions
- Existing proxy behavior unchanged (sessions exist but are not used yet)

### Phase 2: Simplified Plan Detector (2-3 days)

**Goal:** Remove `return` directive dependency from the detector. Plan completion is determined by `[DONE]` + `HasPlan()`.

**Files:** `pkg/proxy/detector.go`, `pkg/proxy/detector_test.go`, `pkg/proxy/tee.go`, `pkg/proxy/tee_test.go`

**Work:**
- Remove `StateBetweenSteps` and `StatePlanComplete` states
- Remove `ActionPlanComplete` action type
- Add `StateHasPlan` state
- Add `HasPlan()` and `PrePlanText()` methods
- Update `StreamTee` to check `HasPlan()` after `[DONE]` instead of reacting to `ActionPlanComplete`

**Acceptance Criteria:**
- Unit tests: plan with fences but no `return` is detected as a valid plan
- Unit tests: plan with `return` still works (return is just ignored text)
- Unit tests: no regressions on fence detection (non-Loom fences, split tokens)
- Fuzz tests updated

### Phase 3: Background Execution + Result Injection (3-4 days)

**Goal:** Wire everything together. Plans execute in background after `[DONE]`. Results are injected into the next request.

**Files:** `pkg/proxy/proxy.go`, `pkg/proxy/injector.go`, `pkg/proxy/injector_test.go`

**Work:**
- Refactor `handleStream`: after `[DONE]` with plan, launch background execution
- Add request ingress hook: check for pending session results, inject into messages
- Implement `waitForPendingExecution` blocking logic
- Implement message injection (assistant message reconstruction + tool/user result message)
- Handle `tool_call_id` synthesis for strict APIs

**Acceptance Criteria:**
- Integration test: round 1 with plan -> round 2 with follow-up -> LLM sees results in round 2
- Integration test: fast follow-up (execution still running when round 2 arrives) blocks correctly
- Integration test: session expires before round 2 -> no injection, normal pass-through
- Integration test: all three visibility modes work correctly
- Integration test: `injection_role=user` fallback works
- Error handling: failed plan steps appear as `status="error"` in injected results
- Non-streaming mode works with background execution

### Phase 4: Polish + Edge Cases (2-3 days)

**Goal:** Handle edge cases, clean up, update documentation.

**Files:** all proxy files, `cmd/loom-proxy/main.go`

**Work:**
- Handle client sending wrong/missing assistant message in round N+1 (proxy overrides with stored version)
- Handle multiple plans across sessions concurrently (stress test)
- Graceful shutdown: drain in-flight executions
- Structured logging for session lifecycle events
- Metrics: sessions active, pending executions, injection count
- Update system prompt template (remove instructions about `return` directive)

**Acceptance Criteria:**
- Load test: 100 concurrent sessions with plans, no races or deadlocks
- Graceful shutdown completes in-flight executions within timeout
- Logs show session lifecycle at debug level
- System prompt file updated to remove `return` references

---

## 10. Request Flow (Pseudocode)

```go
func (h *Handler) handleStream(w http.ResponseWriter, r *http.Request, req ChatCompletionRequest) {
    // === INGRESS: Inject pending results ===
    sessionID := h.resolveSessionID(r, req.Messages)
    session := h.sessions.Get(sessionID)

    if session != nil {
        // Block until any pending execution completes
        if err := h.waitForPendingExecution(r.Context(), session); err != nil {
            writeError(w, err)
            return
        }

        // Inject results into messages if available
        session.Mu.Lock()
        if session.PendingResults != nil {
            req.Messages = h.injectResults(req.Messages, session)
            session.PendingResults = nil
            session.ExecutionDone = nil
        }
        session.Mu.Unlock()
    }

    // === FORWARD: Standard proxy with plan detection ===
    if h.systemPrompt != "" {
        req.Messages = injectSystemPrompt(req.Messages, h.systemPrompt, h.config.SystemPromptMode)
    }

    upstreamResp, err := h.forwardToUpstream(req)
    if err != nil { writeError(w, err); return }
    defer upstreamResp.Body.Close()

    sseWriter := NewSSEWriter(w)
    detector := NewPlanDetector()
    tee := NewStreamTee(sseWriter, detector, h.config.PlanVisibility)

    // Process upstream SSE stream (all chunks, including [DONE])
    ParseSSEStream(upstreamResp.Body, func(data []byte) error {
        content, ok := ChunkContent(data)
        if !ok {
            return tee.ForwardRaw(data)
        }
        actions := detector.Feed(content)
        for _, action := range actions {
            switch action.Type {
            case ActionForward:
                tee.Forward(action.Content)
            case ActionSuppress:
                tee.Suppress(action.Content)
            case ActionFlush:
                tee.Flush(action.Content)
            case ActionBuffer:
                // hold
            }
        }
        return nil
    })

    // Send [DONE] to client
    sseWriter.Close()

    // === EGRESS: Launch background execution if plan detected ===
    if detector.HasPlan() {
        fullAssistantText := detector.PrePlanText() + detector.PlanText()
        h.executeInBackground(sessionID, detector.PlanText(), fullAssistantText)
    }
}
```

---

## 11. Open Questions

### Q1: What if the user never sends a follow-up?

If plan execution completes but the user never sends another message, the results sit in the session until TTL expires and are discarded. This is acceptable — the user abandoned the conversation. The plan's side effects (writes, shell commands) still executed.

**Mitigation for important results:** A future enhancement could support a webhook or push notification when results are ready, but this is out of scope for v2.

### Q2: Should the proxy support `tool_calls` natively?

If the upstream model uses native `tool_calls`, the proxy needs to decide whether to treat Loom fences in `content` and tool calls in `tool_calls` as orthogonal. Current recommendation: same as v1 — pass through `tool_calls` unchanged. They are orthogonal to Loom plans.

### Q3: What about the client's conversation history consistency?

When `plan_visibility=suppress`, the client's stored assistant message for round N will not include the plan fences. But the proxy injects the full text (with fences) into round N+1. This means the client's history and the LLM's view diverge slightly. This is intentional — the LLM needs the full context, and the client should not be burdened with raw plan syntax.

### Q4: Should we support multiple pending executions per session?

If round N has a plan, and the user sends round N+1 which also produces a plan (before or after injection), should both be tracked? Current design: only one pending execution per session. The round N+1 plan execution overwrites any remaining state. This is sufficient for conversational use patterns.

### Q5: Race condition — what if execution finishes after session TTL?

If plan execution takes longer than `LOOM_SESSION_TTL`, the session could be cleaned up before results are stored. **Mitigation:** The cleanup goroutine checks `ExecutionDone` — if the channel exists and is not yet closed, the session is not eligible for cleanup regardless of TTL.

### Q6: Token counting with injection?

Injected results add tokens to the round N+1 request. This is visible in the upstream API's `usage` response for that call. No special handling needed — the client sees accurate token counts for the call that was actually made.

### Q7: Should the Loom system prompt be updated?

Yes. The system prompt template should be updated to:
- Remove any mention of `return` as a plan termination directive
- Explain that the LLM should simply end its response after writing plan fences
- Note that results will appear in the next message

This is a documentation/prompt change, not a code change, but it is critical for correct LLM behavior.

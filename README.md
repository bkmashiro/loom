# loom

**LLM execution runtime — parallel plan execution via streaming notation and composable IO primitives.**

LLMs generate execution plans. Loom runs them in parallel.

```
Sequential tool calling:   LLM → A → LLM → B → LLM → C → answer   (3 LLM calls, serial)
Loom:                      LLM → [A ∥ B ∥ C → answer]              (1 LLM call, parallel)
```

→ [Design Document](./DESIGN.md) · [Detailed Spec](./DESIGN-DETAILED.md) · [Proxy v2 Design](./DESIGN-PROXY-V2.md)

---

## How it works

The LLM outputs an annotated plan using fenced code blocks:

````
```io fetch_user
GET /api/users/${user_id}
```

```io fetch_posts
GET /api/posts?user=${user_id}
```

```pure(fetch_user, fetch_posts) build_feed
merge(fetch_user, fetch_posts)[:10]
```
````

Loom streams this output token by token. As each fence closes, the step is immediately
dispatched — `fetch_user` and `fetch_posts` fire in parallel before `build_feed` is
even parsed. Steps without deps run immediately; steps with deps run as soon as their
inputs are ready.

---

## Step Types

| Type | Semantics |
|------|-----------|
| `io` | Idempotent read — parallel, retryable, LRU-cached |
| `write` | Side-effecting write — isolated, not auto-retried |
| `pure` | Deterministic computation — parallel, no IO |
| `shell` | Shell command — WASM sandbox |
| `async` | Fire-and-forget — non-blocking, does not gate result |
| `escape` | Raw tool call — external tool registry |

## Built-in Primitives

```
GET / POST / PUT / DELETE <url>   — HTTP
read / write / append / ls <path> — Filesystem (capability-gated)
kv.get / kv.set / kv.del <key>   — Per-plan KV store
python / js <code>                — Code execution (WASM sandbox)
@tool <name> <args>               — Escape hatch (custom tools)
```

---

## Architecture

```
LLM stream → pkg/parser → pkg/dag → pkg/exec → result
                              ↓
                         pkg/pool (WASM instances)
                         pkg/primitives (HTTP, FS, KV)
                         pkg/proxy (OpenAI-compatible proxy)
```

| Package | Role |
|---------|------|
| `pkg/parser` | Streaming fence parser — emits Steps as each block closes, no look-ahead |
| `pkg/dag` | DAG scheduler — parallel dispatch, cycle detection, forward-reference resolution |
| `pkg/pool` | Pre-warmed WASM instance pool (~1ms acquire latency) |
| `pkg/exec` | Routes steps to backends by type; singleflight dedup + LRU cache |
| `pkg/primitives` | HTTP, filesystem, KV, map/reduce built-ins |
| `pkg/loom` | Top-level API (`Run`, `Stream`, `RegisterTool`, `WithIOCache`) |
| `pkg/proxy` | Drop-in OpenAI proxy — injects Loom execution transparently |

---

## Loom Proxy

`loom-proxy` is an OpenAI-compatible reverse proxy. Point any client at it — it intercepts
responses and runs Loom plans in the background, injecting results into the next round.

```
Client → loom-proxy → upstream LLM
                ↓ (after [DONE])
         Background execution
                ↓ (next request)
         Results injected as tool/user message
```

### Key features

- **Zero client changes** — any OpenAI SDK works unmodified
- **`loom_describe` tool** — injected automatically; LLM can call it to learn the fence
  syntax on demand (no system prompt required)
- **Session state** — `X-Loom-Session-ID` header or SHA-256 prefix hash for continuity
- **Result injection** — tool-role or user-role injection, configurable
- **`/metrics`** — Prometheus-format endpoint: requests, plans, injections, pending executions

```bash
loom-proxy \
  --upstream https://api.openai.com \
  --addr :8080 \
  --injection-role tool \
  --session-ttl 30m
```

Environment variables: `LOOM_UPSTREAM`, `LOOM_ADDR`, `LOOM_API_KEY`, `LOOM_SESSION_TTL`,
`LOOM_INJECTION_ROLE`, `LOOM_PLAN_VISIBILITY` (`passthrough`/`suppress`/`indicator`),
`LOOM_DESCRIBE_ENABLED`.

---

## Application Scenarios

### 1. Research Assistant (read-heavy fanout)

A user asks "Compare pricing for product X across 5 vendors." The LLM emits 5 parallel
`io` fetches that all execute simultaneously. Serial tool calling would take 5 × LLM round
trips; Loom does it in one.

```
```io vendor_a
GET https://api.vendor-a.com/price?sku=X
```
```io vendor_b
GET https://api.vendor-b.com/price?sku=X
```
...
```pure(vendor_a,vendor_b,...) compare
cheapest = min(vendor_a, vendor_b, ...)
```
```

### 2. Data Pipeline (multi-stage DAG)

ETL with dependent stages: extract → transform → load. The DAG ensures each stage
waits for its inputs while independent branches run in parallel.

```
fetch_raw → clean → [enrich_geo, enrich_demo] → join → write_db
                                    ↑ parallel ↑
```

### 3. Swarm Calling

One coordinator LLM orchestrates N specialist sub-agents that each handle a domain.
Results are aggregated by a final step. Loom's DAG maps this directly:

```
coordinator → [agent_legal, agent_finance, agent_technical, agent_market]
                                     ↓ all parallel ↓
                              aggregator (synthesis)
```

Benchmark (pure harness overhead, no real IO):

| Pattern | Agents | Latency |
|---------|--------|---------|
| Swarm | 4 | ~14 µs |
| Swarm | 16 | ~50 µs |
| Swarm | 64 | ~185 µs |

With 50ms IO per agent: 64-agent swarm completes in ~50ms instead of 64 × 50ms = 3.2s.

### 4. Speculative Execution

An `async` step fires a slow background job immediately (e.g. triggering a report
generation); the plan continues without waiting. The job's completion is observable in a
later round via session injection.

### 5. Cached Repeated Lookups

Within a multi-turn conversation, the same `/api/user/123` endpoint is called each round.
The LRU+TTL IO cache (`WithIOCache(256, 30s)`) short-circuits repeated identical requests —
identical concurrent requests are additionally coalesced by singleflight.

### 6. Code Generation + Execution

```
```pure sandbox
js: fetch("https://jsonplaceholder.typicode.com/todos/1").then(r=>r.json())
```
```shell(sandbox) run
node -e "${sandbox}"
```
```

The `shell` step runs in a WASM sandbox with no host capabilities by default.

---

## Performance Model

Goal: harness overhead < 10% of LLM inference time.

```
LLM tokens:  ──[fence1]──[fence2]──[fence3]──[DONE]──
                  ↓           ↓          ↓
Dispatch:    [exec1]     [exec2]    [exec3]
             [running]   [running]  [running]
                                             ↓
                                    Results ready before next round
```

| Scenario | Sequential tool calling | Loom |
|----------|------------------------|------|
| 10 steps, 50ms IO | 10 × (2s LLM + 50ms) = **20.5s** | 1 × (2s LLM + 50ms) = **2.05s** |
| 20 steps, 3 parallel stages | 20 × 2s = **40s** | 3 × (2s + 50ms) ≈ **6.2s** |
| 64-agent swarm, 50ms IO | 64 × 50ms = **3.2s** | ~**50ms** |

### Benchmark Results (no real LLM or IO)

```
DAG Scheduler (pure harness overhead)
  Linear chain    4 steps:   ~9.4 µs
  Linear chain   64 steps: ~510 µs
  Parallel        4 steps:   ~4.4 µs
  Parallel       64 steps:  ~54 µs
  Diamond w=64:            ~183 µs
  Swarm   64 agents:       ~185 µs
  Forward-ref 32 steps:     ~84 µs

IO Cache
  Cache hit:     ~55 ns/op   (0 allocs)
  Cache miss:   ~100 ns/op   (2 allocs)
  Mixed (80/20): ~152 ns/op
  Parallel:     ~197 ns/op   (under contention)

Parser
  1-step plan:    ~6 µs
  10-step plan:  ~12 µs
  16-step plan:  ~12 µs
  100-step plan: ~43 µs
```

---

## Status

| Phase | Package | Status | Tests |
|-------|---------|--------|-------|
| 0 | `pkg/parser` | ✅ complete | 16 tests + benchmarks |
| 1 | `pkg/dag` | ✅ complete | 22 tests + benchmarks |
| 2 | `pkg/pool` | ✅ complete | 19 tests, wazero backend |
| 3 | `pkg/exec` + `pkg/primitives` | ✅ complete | 20 tests + cache benchmarks |
| 4 | `pkg/loom` | ✅ complete | integration tests |
| 5 | Proxy v2, session store, metrics | ✅ complete | 70+ tests |
| — | Singleflight + LRU cache | ✅ complete | integrated |
| — | Forward references in DAG | ✅ complete | 3 new tests |
| — | `loom_describe` built-in tool | ✅ complete | 8 tests |
| — | `/metrics` endpoint | ✅ complete | 2 tests |

**Total: 148 passing tests across 7 packages.**

---

## License

MIT

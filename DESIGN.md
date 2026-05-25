# Loom — Design Document

> *A loom weaves parallel threads into cloth. Loom weaves parallel IO and computation into efficient LLM execution plans.*

## Motivation

### The Tool Calling Problem

Current LLM agentic systems execute tools **sequentially**, with a full LLM inference round-trip between each step:

```
LLM → tool_A → LLM → tool_B → LLM → tool_C → LLM → answer
      ←result←      ←result←      ←result←

Cost:  N × LLM inference  +  N × tool latency  (all serial)
```

The bottleneck is not tool execution — it is **LLM inference between every step**. For a 10-step task, this means 10 API calls, 10 round-trip latencies, and strictly sequential execution even when most steps are independent.

### What Loom Does Instead

The LLM generates a **complete execution plan** in one pass. The Loom runtime executes it, respecting declared dependencies, parallelizing where possible:

```
LLM generates plan (once)
        ↓
Runtime executes:
  tool_A ──┐
  tool_B ──┼──→ tool_D(A,B) ──→ answer
  tool_C ──┘  (parallel)

Cost:  1 × LLM inference  +  max(parallel stage latency)
```

LLM round-trips drop from O(steps) to O(plan stages). A 10-step task with 3 parallel stages needs ~3 LLM calls instead of 10.

---

## Core Concepts

### 1. The Plan

A Loom plan is a **streaming DAG** of typed steps. The LLM outputs it token by token; the runtime starts executing as each step is parsed — it does not wait for the full plan.

### 2. Steps

Each step has:
- A **unique ID** (declared at the step, not later)
- An **effect type** (determines scheduling + security policy)
- Zero or more **input dependencies** (IDs of prior steps)
- A **body** (the actual operation or code)
- An optional **output name** (referenced by dependents)

### 3. Effect Types

| Type | Symbol | Meaning | Scheduling |
|------|--------|---------|------------|
| IO read | `!` | Idempotent network/file/DB read | Parallel, retryable, cacheable |
| IO write | `!!` | Side-effecting write | Isolated, not auto-retried |
| Pure | (none) | Deterministic computation, no IO | Parallel, free to re-execute |
| Async | `&` | Fire-and-forget side effect | Non-blocking, does not gate return |
| Escape | `@` | Raw/custom tool call | Treated as opaque IO write |

### 4. Notation Design Goals

- **LLM-friendly**: familiar syntax, low cognitive load, easy to generate correctly
- **Streaming-parseable**: each block can be acted on as soon as it arrives
- **No look-ahead (最小后相关)**: step N's semantics are fully determined when step N is parsed; no future step can change the meaning of a past step
- **Monotonic**: once declared, a step's type and dependencies are fixed
- **Fault-isolated**: a malformed step does not crash the pipeline; dependents are cancelled, independent branches continue
- **Minimal token overhead**: annotations are short; every token adds scheduling value
- **Separation of concerns**: LLM declares *what*, runtime decides *how*

---

## Notation

Steps are declared as annotated fences, one per block. The LLM outputs them top-to-bottom; the runtime streams them in.

### Syntax

````
```<type> <id>(<dep1>, <dep2>, ...)
<body>
```
````

- `<type>`: effect type keyword (see below)
- `<id>`: unique step name (snake_case)
- `(<dep1>, ...)`: optional explicit dependencies (usually inferred from variable references in body)
- `<body>`: any code or DSL expression

### Full Example

````
```io fetch_user
GET /api/users/${user_id}
```

```io fetch_posts
GET /api/posts?user=${user_id}
```

```io fetch_recs
POST /ml/recommend {"user": "${user_id}", "n": 5}
```

```pure(fetch_user, fetch_posts, fetch_recs) build_feed
import json
user  = json.loads(fetch_user)
posts = json.loads(fetch_posts)["items"]
recs  = json.loads(fetch_recs)["items"]
feed  = sorted(posts + recs, key=lambda x: x["score"], reverse=True)
return feed[:10]
```

```async
POST /api/analytics {"user": "${user_id}", "action": "feed_view"}
```

return build_feed
````

The runtime receives `fetch_user`, `fetch_posts`, `fetch_recs` and immediately fires all three in parallel. By the time `build_feed` is parsed, the IO is likely already in flight or complete.

### Step Types

**`io` — Idempotent IO (reads)**
```
```io <id>
GET /api/resource
```
```
- Parallelized with all other `io` steps that have no dependency on each other
- Safe to retry on transient failure
- Response may be cached if the same URL is fetched again in the same plan
- Executed in WASM sandbox with network capability granted

**`write` — Side-effecting IO**
```
```write <id>(<dep>)
POST /api/orders {"item": "${dep.item_id}"}
```
```
- Not auto-retried (non-idempotent)
- Isolated WASM sandbox
- Waits for all declared dependencies

**`pure` — Pure computation**
```
```pure(<dep1>, <dep2>) <id>
result = merge(dep1, dep2)
return result
```
```
- No network or filesystem access
- Can run without WASM isolation (fast path) or in WASM (configurable)
- Re-executable freely

**`async` — Fire-and-forget**
```
```async
POST /api/log {"event": "..."}
```
```
- Does not block `return`
- Failure does not fail the plan
- No output available to other steps

**`shell` — Shell command in WASM sandbox**
```
```shell <id>
pip install numpy && python compute.py
```
```
- Full WASM sandbox with restricted filesystem
- Stdout captured as output

**`escape` — Raw tool call (escape hatch)**
```
```escape <id>
@tool browser_screenshot {"url": "https://example.com"}
```
```
- Calls an externally-registered tool by name
- Treated as opaque IO write: not retried, not cached
- Allows integration with existing tool ecosystems (LangChain tools, MCP, etc.)
- Intentionally unconstrained — use sparingly

---

## Primitive Set

Loom provides a minimal set of built-in primitives that covers ~90% of common tool use cases. All are expressed in step bodies.

### Network
```
GET    <url> [headers: {...}]
POST   <url> <body> [headers: {...}]
PUT    <url> <body> [headers: {...}]
DELETE <url> [headers: {...}]
```

### Filesystem (WASI capability-gated)
```
read   <path>
write  <path> <content>
append <path> <content>
ls     <path>
```

### Key-Value Store (per-session)
```
kv.get <key>
kv.set <key> <value>
kv.del <key>
```

### Code Execution
```
python <code-string>   # Python in WASM
js     <code-string>   # JavaScript (QuickJS in WASM)
shell  <cmd>           # bash in WASM
```

### Composition
```
map    <list> <step-id>    # parallel map over list
reduce <list> <step-id>    # sequential fold
```

### Escape Hatch
```
@tool  <name> <args-json>  # call any registered external tool
```

**Coverage:**

| Common Tool | Loom Primitive |
|-------------|---------------|
| Web search  | `GET search_api_url` |
| Read file   | `read path` |
| Write file  | `write path content` |
| Run code    | `python / js / shell` |
| Call REST API | `GET / POST / PUT / DELETE` |
| Memory store | `kv.set` |
| Memory recall | `kv.get` |
| Browse web  | `GET` + `python scrape` |
| Send email  | `POST mail_api` |
| Query DB    | `POST db_api` or `shell psql` |
| Custom tool | `@tool name args` |

The primitive set is intentionally **not fixed**. New primitives can be registered by the runtime host. The `escape` step type ensures nothing is impossible.

---

## Runtime Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     LLM Stream                              │
│  (token by token, or chunk by chunk)                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  Stream Parser                               │
│  • Detects fence boundaries (``` type id(deps) ```)         │
│  • Emits Step events as each block completes                │
│  • Validates: no cycles, forward-references only            │
└────────────────────────┬────────────────────────────────────┘
                         │ Step events
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   DAG Scheduler                              │
│  • Maintains dependency graph                               │
│  • Dispatches steps whose dependencies are satisfied        │
│  • Tracks completed outputs, feeds to dependents            │
└──────┬─────────────┬──────────────┬──────────────┬──────────┘
       │             │              │              │
       ▼             ▼              ▼              ▼
  IO Executor   IO Executor   Pure Executor  Async Executor
  (WASM +       (WASM +       (goroutine or  (goroutine,
   network)      filesystem)   WASM)          detached)
```

### Key Properties

**Cold start hiding**: For serverless deployments, the WASM runtime is warm (shared pool). IO steps fire immediately; pure computation steps that depend on them start executing when IO completes. Cold start of the compute environment is overlapped with IO wait.

**Request coalescing**: If multiple concurrent plans request the same `io` step (same URL, same params), the runtime deduplicates — one fetch, shared result. (singleflight semantics)

**Backpressure**: The scheduler can throttle step dispatch based on resource availability (open connections, WASM instance pool size).

---

## Execution Isolation

| Step Type | Isolation | Rationale |
|-----------|-----------|-----------|
| `io` (read) | WASM + net capability | Network access, no filesystem |
| `write` | WASM + net + scoped fs | Write to allowed paths only |
| `pure` | None (fast) or WASM | Configurable per deployment |
| `shell` | WASM full sandbox | Arbitrary code, maximum isolation |
| `async` | WASM + net | Same as write, fire-and-forget |
| `escape` | Host-defined | External tool owns its isolation |

WASM provides:
- **Memory isolation**: linear memory, no host memory access
- **Syscall restriction**: only explicitly imported host functions available
- **Capability-based FS**: WASI preopened directories only
- **Fast startup**: ~1-5ms vs Docker's 500ms-2s

---

## Comparison

| | Tool Calling | Loom |
|--|-------------|------|
| Execution model | Sequential, one call per LLM round-trip | Parallel DAG, one plan per LLM call |
| LLM round-trips | O(steps) | O(plan stages) |
| Parallelism | None | Automatic from data dependencies |
| Tool definition | Per-platform schemas | Unified primitive set + escape hatch |
| Isolation | Platform-specific | WASM, uniform model |
| Streaming | N/A | Parse-and-execute as LLM outputs |
| Cost | High (N inference calls) | Low (1 inference call per plan) |

---

## Performance Model

### The Fundamental Bound

Every agentic loop iteration costs:

```
iteration time = LLM inference time + Harness execution time
```

**Goal: Harness execution time < 10% of LLM inference time.**

A typical LLM generates a 20-step plan in 2–5 seconds. The harness must complete those 20 steps in under 200ms. With parallel IO, this is achievable: 20 independent HTTP calls at 50ms each take 50ms total, not 1000ms.

If the harness is fast enough, the LLM barely waits between thinking and acting — which means each inference call can include richer context (more completed results) and generate a larger, more comprehensive plan.

### Pipelined Execution

The ideal execution model overlaps LLM generation with Harness execution:

```
LLM output:    ──[step1]──[step2]──[step3]──[step4]──[done]──
                    ↓         ↓         ↓         ↓
Harness:       [exec1]   [exec2]   [exec3]   [exec4]
               [running] [running] [running] [running]
                                                      ↓
                                             All results ready.
                                             LLM starts next plan immediately.
```

By the time the LLM finishes generating the plan, most steps are already executing or complete. The LLM receives all results with near-zero additional wait.

### What This Enables

- **Larger plans per LLM call**: if execution is free, the LLM can generate 30-step plans as easily as 5-step ones
- **Richer context per inference**: completed step results can be packed into the next prompt, giving the LLM more information per reasoning step
- **Fewer total LLM calls**: more work per call means fewer round-trips to complete the overall task
- **Result streaming back**: completed steps are appended to the next LLM context as they finish — no need to wait for the slowest step

### Quantified Comparison

| Scenario | Sequential tool calling | Loom |
|---|---|---|
| 10 steps, 50ms IO each | 10 × (2s LLM + 50ms) = 20.5s | 1 × (2s LLM + 50ms) = 2.05s |
| 20 steps, 3 parallel stages | 20 × 2s = 40s | 3 × (2s + 50ms) ≈ 6.2s |
| 5 independent steps | 5 × 2s = 10s | 1 × (2s + 50ms) = 2.05s |

The gain is primarily from **reducing LLM round-trips**, with parallel IO execution as a secondary benefit.

### Implementation Targets

| Component | Target | Rationale |
|---|---|---|
| Per-step scheduling overhead | < 2ms | Goroutine spawn + DAG update |
| WASM instance acquisition | < 1ms | Pre-warmed instance pool |
| HTTP connection setup | ~0ms | Per-host connection pool, keep-alive |
| Result delivery to next step | < 0.1ms | Zero-copy in-process pass |
| Total harness overhead (20 steps) | < 50ms | Well under 10% of LLM inference |

### Why WASM over Docker

The performance model explains why isolation technology matters:

| | Docker cold start | WASM cold start |
|---|---|---|
| Time | 500ms – 2s | 1 – 5ms |
| Effect on harness | Destroys the performance model | Negligible |

Docker cold start alone exceeds the entire harness execution budget. WASM is not just a security choice — it is a **correctness requirement** for the performance model to hold.

---

## What Loom Is Not

- Not a replacement for the LLM itself
- Not an agent framework (no memory management, no planning loop)
- Not a workflow engine (no persistent state, no human-in-the-loop)
- Not tied to any specific LLM provider

Loom is the **execution layer** between an LLM's output and the real world. It handles parallelism, isolation, and scheduling so the LLM can focus on reasoning.

---

## Status

Early design phase. Contributions and feedback welcome.

See [issues](https://github.com/bkmashiro/loom/issues) for the implementation roadmap.

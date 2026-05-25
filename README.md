# loom

**LLM execution runtime — parallel plan execution via streaming notation and composable IO primitives.**

LLMs generate execution plans. Loom runs them in parallel.

```
Current tool calling:    LLM → A → LLM → B → LLM → C → answer   (3 LLM calls, serial)
Loom:                    LLM → [A ∥ B ∥ C → answer]              (1 LLM call, parallel)
```

→ [Design Document](./DESIGN.md) · [Detailed Spec](./DESIGN-DETAILED.md)

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
posts = merge(fetch_user, fetch_posts)
return posts[:10]
```

return build_feed
````

Loom streams this output token by token. As each fence closes, the step is immediately dispatched — `fetch_user` and `fetch_posts` fire in parallel before `build_feed` is even parsed.

## Step Types

| Type | Symbol | Semantics |
|------|--------|-----------|
| `io` | idempotent read | Parallel, retryable, cacheable |
| `write` | side-effecting write | Isolated, not auto-retried |
| `pure` | deterministic computation | Parallel, no IO |
| `shell` | shell command | WASM sandbox |
| `async` | fire-and-forget | Non-blocking, does not gate return |
| `escape` | raw tool call | External tool registry |

## Built-in Primitives

```
GET / POST / PUT / DELETE <url>   — HTTP
read / write / append / ls <path> — Filesystem (capability-gated)
kv.get / kv.set / kv.del <key>   — Per-plan KV store
python / js <code>                — Code execution (WASM sandbox)
@tool <name> <args>               — Escape hatch (custom tools)
```

## Architecture

```
LLM stream → pkg/parser → pkg/dag → pkg/exec → result
                              ↓
                         pkg/pool (WASM instances)
                         pkg/primitives (HTTP, FS, KV)
```

- **`pkg/parser`** — Streaming fence parser. Emits Steps as each block closes. No look-ahead.
- **`pkg/dag`** — DAG scheduler. Dispatches steps in parallel when dependencies are satisfied.
- **`pkg/pool`** — Pre-warmed WASM instance pool (adapted from [shimmy](https://github.com/lambda-feedback/shimmy)). ~1ms acquire latency.
- **`pkg/exec`** — Routes steps to backends by type and body prefix.
- **`pkg/primitives`** — HTTP, filesystem, KV, and map/reduce built-ins.
- **`pkg/loom`** — Top-level API (`Run`, `Stream`, `RegisterTool`).

## Status

| Phase | Package | Status |
|-------|---------|--------|
| 0 | `pkg/parser` | ✅ complete (12 tests) |
| 1 | `pkg/dag` | ✅ complete (13 tests) |
| 2 | `pkg/pool` | ✅ complete (19 tests, wazero backend) |
| 3 | `pkg/exec` + `pkg/primitives` | 🔲 next |
| 4 | `pkg/loom` | 🔲 planned |
| 5 | singleflight, backpressure, benchmarks | 🔲 planned |

## Performance Model

Goal: harness execution time < 10% of LLM inference time.

```
LLM output:    ──[step1]──[step2]──[step3]──[done]──
                    ↓         ↓         ↓
Harness:       [exec1]   [exec2]   [exec3]
               [running] [running] [running]
                                            ↓
                                   All results ready.
```

| Scenario | Sequential tool calling | Loom |
|---|---|---|
| 10 steps, 50ms IO each | 10 × (2s LLM + 50ms) = 20.5s | 1 × (2s LLM + 50ms) = 2.05s |
| 20 steps, 3 parallel stages | 20 × 2s = 40s | 3 × (2s + 50ms) ≈ 6.2s |

## License

MIT

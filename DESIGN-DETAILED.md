# Loom — Detailed Implementation Specification

> Implementation-grade specification for the Loom LLM execution runtime.
> Each component is specified with Go interfaces, data structures, error contracts,
> and acceptance criteria sufficient for independent parallel implementation.

---

## 1. System Overview

Loom is a streaming DAG execution runtime that sits between an LLM's text output and the outside world. It parses annotated code fences from a streaming LLM response, builds a dependency graph on the fly, and dispatches steps for parallel execution — achieving O(plan-stages) LLM round-trips instead of O(steps). Compute isolation is provided by WebAssembly (wazero), with an instance pool adapted from the shimmy-wasm-go project.

### Component Diagram

```
                         ┌──────────────────────┐
                         │    LLM Text Stream    │
                         │     (io.Reader)       │
                         └──────────┬─────────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │    pkg/parser         │
                         │  Streaming fence      │
                         │  parser               │
                         │  Emits: chan Step      │
                         └──────────┬─────────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │    pkg/dag            │
                         │  DAG scheduler        │
                         │  Dependency tracking  │
                         │  Dispatch on ready    │
                         └───┬──────┬──────┬─────┘
                             │      │      │
                    ┌────────┘      │      └────────┐
                    ▼               ▼               ▼
             ┌────────────┐ ┌────────────┐ ┌────────────┐
             │ IO / HTTP  │ │ Pure /     │ │ Shell /    │
             │ executor   │ │ goroutine  │ │ WASM pool  │
             │            │ │ or WASM    │ │ (shimmy)   │
             └────────────┘ └────────────┘ └────────────┘
                    │               │               │
                    └───────────────┼───────────────┘
                                    ▼
                         ┌──────────────────────┐
                         │    pkg/exec           │
                         │  Step executor        │
                         │  Routes by StepType   │
                         └──────────┬─────────────┘
                                    │
                         ┌──────────────────────┐
                         │    pkg/pool           │
                         │  WASM instance pool   │
                         │  (extracted/adapted   │
                         │   from shimmy)        │
                         └──────────────────────┘

                         ┌──────────────────────┐
                         │    pkg/primitives     │
                         │  HTTP, FS, KV, Map/   │
                         │  Reduce built-ins     │
                         └──────────────────────┘

                         ┌──────────────────────┐
                         │    pkg/loom           │
                         │  Top-level API        │
                         │  Run / Stream /       │
                         │  RegisterTool         │
                         └──────────────────────┘
```

### Relationship to shimmy-wasm-go

Shimmy is an existing WASM execution service built on **wazero** (pure-Go WebAssembly runtime). It provides:

- A compiled-module + instance-pool architecture (`internal/execution/wasm/`)
- Memory snapshot/restore for clean per-request state
- A `Dispatcher` interface with `Send(ctx, method, data) (data, error)` semantics
- A `puddle`-based resource pool for process-based supervisors
- WASI support for guest modules

Loom will **import shimmy as a Go module** for its WASM pool and adapt shimmy's `Dispatcher` interface to Loom's `pool.Pool` / `pool.Instance` interfaces. See Section 4 for the detailed integration plan.

---

## 2. Component Specifications

### A. `pkg/parser` — Streaming Fence Parser

**Responsibility:** Read streaming LLM output from an `io.Reader` and emit parsed `Step` values on a channel as each fenced code block completes.

#### Public Interface

```go
package parser

import "io"

// StepType enumerates the effect classification of a step.
type StepType int

const (
    IO     StepType = iota // Idempotent read (GET, read file)
    Write                  // Side-effecting write (POST, write file)
    Pure                   // Deterministic computation, no IO
    Shell                  // Shell command in WASM sandbox
    Async                  // Fire-and-forget, does not gate return
    Escape                 // Raw tool call (escape hatch)
)

// Step is a single unit of work parsed from the LLM output.
type Step struct {
    ID   string   // Unique identifier (snake_case), e.g. "fetch_user"
    Type StepType // Effect type
    Deps []string // IDs of steps this step depends on
    Body string   // The code/DSL body between the fences
    Lang string   // Optional sub-language hint (e.g. "python", "js")
}

// ReturnDirective represents a `return <id>` statement parsed from the stream.
type ReturnDirective struct {
    StepID string
}

// Event is either a parsed Step or a ReturnDirective.
type Event struct {
    Step   *Step            // Non-nil if this event is a step
    Return *ReturnDirective // Non-nil if this event is a return directive
}

// Parser reads from an io.Reader and emits Events as fences complete.
type Parser struct {
    // unexported fields
    r      io.Reader
    events chan Event
    err    error
    done   chan struct{}
}

// NewParser creates a parser that reads from r.
// Parsing begins immediately in a background goroutine.
func NewParser(r io.Reader) *Parser

// Events returns a channel that emits Events as they are parsed.
// The channel is closed when the reader is exhausted or an unrecoverable
// error occurs. Malformed individual fences emit an error via Err() but
// do NOT close the channel — parsing continues.
func (p *Parser) Events() <-chan Event

// Err returns the first unrecoverable error encountered during parsing,
// or nil if parsing completed successfully. Only valid after Events()
// channel is closed.
func (p *Parser) Err() error

// Close cancels parsing and releases resources. Safe to call multiple times.
func (p *Parser) Close()
```

#### Key Data Structures

```go
// Internal parser state machine
type parserState int

const (
    stateText      parserState = iota // Outside any fence
    stateFenceOpen                     // Just saw ```, parsing header
    stateBody                          // Inside fence body
)

// fenceHeader holds the parsed components of a fence opening line.
type fenceHeader struct {
    Type StepType
    ID   string
    Deps []string
    Lang string
}
```

#### Parsing Rules

1. **Fence detection:** A line matching `` ^```(\w+)(.*)$ `` opens a fence. The closing `` ^``` `` (alone on a line) closes it.
2. **Header parsing:** The text after `` ``` `` is parsed as: `<type>[.<lang>] [<id>][(dep1, dep2, ...)]`
   - Type keywords: `io`, `write`, `pure`, `shell`, `async`, `escape`
   - Language suffix: `pure.python`, `shell.js` — the part after `.` is stored in `Step.Lang`
   - ID is optional for `async` steps (auto-generated as `_async_N`)
   - Deps in parentheses are comma-separated step IDs
3. **Return directive:** A line matching `^return\s+(\w+)\s*$` outside any fence emits a `ReturnDirective`.
4. **Streaming:** The parser reads byte-by-byte (buffered) and emits events as soon as a fence closes or a return directive is seen. It never buffers more than one complete fence.

#### Error Handling Contract

| Condition | Behavior |
|---|---|
| Malformed fence header (bad type keyword) | Skip the fence body, log warning, continue parsing |
| Unclosed fence at EOF | Emit error on `Err()`, do not emit partial step |
| Duplicate step ID | Emit the step; DAG scheduler rejects it downstream |
| IO error on reader | Close events channel, set `Err()` |
| Context cancellation | Close events channel, set `Err()` to context error |

#### Dependencies

- Standard library only (`io`, `bufio`, `strings`, `sync`)
- No dependency on other Loom packages

---

### B. `pkg/dag` — DAG Scheduler

**Responsibility:** Accept steps as they arrive from the parser, build a dependency graph, dispatch steps for execution when all dependencies are satisfied, and collect results.

#### Public Interface

```go
package dag

import (
    "context"

    "github.com/bkmashiro/loom/pkg/parser"
)

// Result holds the output of a completed step.
type Result struct {
    StepID string
    Data   any    // The return value from execution
    Err    error  // Non-nil if execution failed
}

// StepResult is a Result plus metadata for streaming consumers.
type StepResult struct {
    Result
    Pending   int // Number of steps still pending
    Completed int // Number of steps completed so far
}

// Executor is the interface the scheduler uses to run steps.
type Executor interface {
    Execute(ctx context.Context, step parser.Step, inputs map[string]Result) (Result, error)
}

// Scheduler manages the dependency graph and dispatches ready steps.
type Scheduler struct {
    // unexported
    exec      Executor
    graph     map[string]*node
    results   map[string]Result
    pending   map[string]struct{}
    mu        sync.Mutex
    returnID  string
    returnCh  chan Result
    streamCh  chan StepResult
    ctx       context.Context
    cancel    context.CancelFunc
    wg        sync.WaitGroup
}

// NewScheduler creates a scheduler that dispatches to the given executor.
func NewScheduler(ctx context.Context, exec Executor) *Scheduler

// Submit adds a step to the DAG. If all dependencies are already satisfied,
// the step is immediately dispatched for execution. Called as the parser
// emits steps — safe to call from a single goroutine concurrently with
// execution completing in other goroutines.
//
// Returns error if:
//   - Step ID is duplicate
//   - A dependency cycle would be created
//   - A dependency references a step not yet seen (forward-ref)
//     NOTE: forward-refs are NOT allowed; deps must reference
//     already-submitted steps
func (s *Scheduler) Submit(step parser.Step) error

// SetReturn declares which step's result is the final output.
// Must be called before Wait.
func (s *Scheduler) SetReturn(stepID string)

// Wait blocks until the return step completes (or all steps if no return
// is set) and returns the final result. Returns error if the return step
// failed or was never submitted.
func (s *Scheduler) Wait() (Result, error)

// Stream returns a channel of StepResults emitted as each step completes.
// The channel is closed when all steps are done or the context is cancelled.
func (s *Scheduler) Stream() <-chan StepResult

// Cancel aborts all in-flight executions and prevents new dispatches.
func (s *Scheduler) Cancel()
```

#### Key Data Structures

```go
// node represents a step in the dependency graph.
type node struct {
    step       parser.Step
    deps       map[string]struct{} // set of dependency step IDs
    dependents []string            // step IDs that depend on this node
    state      nodeState
}

type nodeState int

const (
    nodeQueued    nodeState = iota // Waiting for dependencies
    nodeRunning                    // Currently executing
    nodeComplete                   // Finished (success or failure)
    nodeCancelled                  // Cancelled due to upstream failure
)
```

#### Dispatch Algorithm

1. When `Submit(step)` is called, insert the step into the graph.
2. Check that all listed deps exist in the graph (reject forward-refs).
3. Check for cycles via a simple DFS from the new node back through deps.
4. If all deps are in `nodeComplete` state, immediately dispatch.
5. Otherwise, mark as `nodeQueued`.
6. When a step completes (`nodeComplete`), iterate its `dependents` list. For each dependent, check if all its deps are now complete. If so, dispatch it.
7. If a step fails, cancel all transitive dependents (mark `nodeCancelled`).
8. `async` steps are dispatched but never waited on for the return path.

#### Error Handling Contract

| Condition | Behavior |
|---|---|
| Duplicate step ID | Return `ErrDuplicateStep` from `Submit` |
| Forward reference (dep not yet submitted) | Return `ErrForwardRef` from `Submit` |
| Cycle detected | Return `ErrCycle` from `Submit` |
| Step execution fails | Mark step as failed, cancel transitive dependents, continue independent branches |
| Return step not submitted | `Wait()` returns `ErrReturnStepNotFound` |
| Return step cancelled (upstream failure) | `Wait()` returns the upstream error wrapped in `ErrReturnCancelled` |
| Context cancelled | Cancel all pending/running steps, `Wait()` returns context error |

#### Sentinel Errors

```go
var (
    ErrDuplicateStep      = errors.New("dag: duplicate step ID")
    ErrForwardRef         = errors.New("dag: dependency references unknown step (forward references not allowed)")
    ErrCycle              = errors.New("dag: dependency cycle detected")
    ErrReturnStepNotFound = errors.New("dag: return step was never submitted")
    ErrReturnCancelled    = errors.New("dag: return step was cancelled due to upstream failure")
)
```

#### Dependencies

- `pkg/parser` (for `Step` and `StepType` types)
- Standard library (`sync`, `context`, `errors`)

---

### C. `pkg/exec` — Step Executor

**Responsibility:** Route each step to the appropriate execution backend based on its `StepType`, execute it, and return the result.

#### Public Interface

```go
package exec

import (
    "context"
    "net/http"

    "github.com/bkmashiro/loom/pkg/dag"
    "github.com/bkmashiro/loom/pkg/parser"
    "github.com/bkmashiro/loom/pkg/pool"
    "github.com/bkmashiro/loom/pkg/primitives"
)

// ToolFunc is the signature for externally-registered escape tools.
type ToolFunc func(ctx context.Context, args map[string]any) (any, error)

// ToolRegistry holds registered escape-hatch tools.
type ToolRegistry struct {
    tools map[string]ToolFunc
    mu    sync.RWMutex
}

// Register adds a tool to the registry.
func (r *ToolRegistry) Register(name string, fn ToolFunc)

// Get retrieves a tool by name.
func (r *ToolRegistry) Get(name string) (ToolFunc, bool)

// StepExecutor implements dag.Executor by dispatching to typed backends.
type StepExecutor struct {
    pool       pool.Pool         // WASM instance pool (from shimmy)
    httpClient *http.Client      // Connection-pooled HTTP client
    tools      *ToolRegistry     // Registered escape-hatch tools
    kv         primitives.KVStore // Per-plan key-value store
    httpPrims  *primitives.HTTPPrimitives
    fsPrims    *primitives.FSPrimitives
}

// Ensure StepExecutor satisfies dag.Executor.
var _ dag.Executor = (*StepExecutor)(nil)

// StepExecutorConfig holds configuration for the executor.
type StepExecutorConfig struct {
    Pool       pool.Pool
    HTTPClient *http.Client
    Tools      *ToolRegistry
    KV         primitives.KVStore
}

// NewStepExecutor creates a new executor with the given backends.
func NewStepExecutor(cfg StepExecutorConfig) *StepExecutor

// Execute runs a single step with its resolved dependency inputs.
// Dispatches to the appropriate backend based on step.Type:
//
//   IO     → HTTP primitives (GET/POST/PUT/DELETE parsed from body)
//   Write  → HTTP primitives with write semantics
//   Pure   → In-process goroutine (fast path) or WASM (sandboxed path)
//   Shell  → WASM instance from pool
//   Async  → Same as IO/Write but non-blocking wrapper
//   Escape → ToolRegistry lookup + call
//
func (e *StepExecutor) Execute(
    ctx context.Context,
    step parser.Step,
    inputs map[string]dag.Result,
) (dag.Result, error)
```

#### Execution Routing

```go
// Internal dispatch table (conceptual)
func (e *StepExecutor) Execute(ctx context.Context, step parser.Step, inputs map[string]dag.Result) (dag.Result, error) {
    // Interpolate dependency results into step body
    body := interpolate(step.Body, inputs)

    switch step.Type {
    case parser.IO:
        return e.executeHTTP(ctx, step, body, true)  // retryable
    case parser.Write:
        return e.executeHTTP(ctx, step, body, false) // not retryable
    case parser.Pure:
        return e.executePure(ctx, step, body, inputs)
    case parser.Shell:
        return e.executeWASM(ctx, step, body)
    case parser.Async:
        return e.executeAsync(ctx, step, body)
    case parser.Escape:
        return e.executeEscape(ctx, step, body)
    default:
        return dag.Result{}, fmt.Errorf("exec: unknown step type: %d", step.Type)
    }
}
```

#### Variable Interpolation

Step bodies may reference dependency outputs using `${dep_id}` syntax. The executor replaces these references with the JSON-serialized result from the named dependency before execution.

```go
// interpolate replaces ${ref} tokens in body with JSON-encoded results
// from the inputs map.
func interpolate(body string, inputs map[string]dag.Result) string
```

#### IO Step Retry Policy

- IO steps (idempotent reads) are retried up to 3 times with exponential backoff (100ms, 200ms, 400ms).
- Write steps are NOT retried.
- HTTP 429 (rate limit) responses are retried with the `Retry-After` header value.

#### Error Handling Contract

| Condition | Behavior |
|---|---|
| Unknown step type | Return `ErrUnknownStepType` |
| HTTP error (5xx) on IO step | Retry up to 3 times, then return error |
| HTTP error on Write step | Return error immediately |
| WASM pool exhausted (timeout) | Return `ErrPoolTimeout` |
| WASM execution panic | Return error, instance is recycled by pool |
| Escape tool not found | Return `ErrToolNotFound` |
| Variable interpolation failure (missing dep) | Return `ErrMissingInput` |

#### Dependencies

- `pkg/parser` (Step types)
- `pkg/dag` (Result type, Executor interface)
- `pkg/pool` (WASM pool)
- `pkg/primitives` (HTTP, FS, KV implementations)
- `net/http` (standard library)

---

### D. `pkg/pool` — WASM Instance Pool

**Responsibility:** Manage a pool of pre-warmed WASM instances that can execute code in sandboxed environments, with per-language isolation.

#### Public Interface

```go
package pool

import "context"

// Language identifies a WASM guest environment.
type Language string

const (
    LangPython Language = "python" // MicroPython WASM module
    LangJS     Language = "js"     // QuickJS WASM module
    LangShell  Language = "shell"  // Busybox/restricted shell WASM module
)

// Instance represents an acquired WASM sandbox ready to execute code.
type Instance interface {
    // Run executes code in the sandbox. Inputs are made available to the
    // guest as a JSON-encoded payload. Returns the guest's output (stdout
    // capture or return value).
    //
    // The instance's linear memory is restored to its snapshot after Run
    // returns, ensuring clean state for the next caller.
    Run(ctx context.Context, code string, inputs map[string]any) (any, error)

    // Language returns which language this instance supports.
    Language() Language
}

// Pool manages pre-warmed WASM instances.
type Pool interface {
    // Acquire obtains a ready instance for the given language.
    // Blocks until an instance is available or ctx is cancelled.
    // The caller MUST call Release when done.
    Acquire(ctx context.Context, lang Language) (Instance, error)

    // Release returns an instance to the pool. If the instance is in a
    // bad state (detected via health check), it is discarded and a fresh
    // one is created in the background.
    Release(inst Instance)

    // Start initializes the pool, compiling modules and pre-warming instances.
    Start(ctx context.Context) error

    // Shutdown closes all instances and releases resources.
    Shutdown(ctx context.Context) error

    // Stats returns pool utilization metrics.
    Stats() PoolStats
}

// PoolStats reports pool utilization.
type PoolStats struct {
    ByLanguage map[Language]LanguageStats
}

// LanguageStats reports per-language pool state.
type LanguageStats struct {
    Total     int // Total instances (idle + in-use)
    Idle      int // Instances available for acquisition
    InUse     int // Instances currently acquired
    WaitCount int // Number of goroutines waiting to acquire
}

// PoolConfig configures the instance pool.
type PoolConfig struct {
    // Modules maps languages to their .wasm file paths.
    Modules map[Language]string

    // InstancesPerLang is the number of pre-warmed instances per language.
    // Defaults to runtime.NumCPU() if <= 0.
    InstancesPerLang int

    // AcquireTimeout is the maximum time to wait for an available instance.
    // Defaults to 5s.
    AcquireTimeout time.Duration
}

// NewPool creates a new WASM instance pool with the given configuration.
func NewPool(cfg PoolConfig) Pool
```

#### Shimmy-Adapted Implementation

```go
// shimmy_adapter.go — bridges shimmy's wasm.Dispatcher to Loom's Pool interface

package pool

import (
    "context"

    shimmy "github.com/lambda-feedback/shimmy/internal/execution/wasm"
)

// ShimmyPool adapts shimmy's WASM dispatcher into Loom's Pool interface.
// It maintains one shimmy Dispatcher per Language, each with its own
// compiled module and instance pool.
type ShimmyPool struct {
    dispatchers map[Language]*shimmy.Dispatcher
    cfg         PoolConfig
}

// shimmy's Dispatcher already provides:
//   - Module compilation (once at Start)
//   - Channel-based instance pool (chan *wasmSupervisor)
//   - Memory snapshot/restore (clean state per request)
//   - WASI support
//
// ShimmyPool maps Acquire/Release to shimmy's channel-based pool
// and wraps the supervisor's Send in Loom's Instance interface.

type shimInstance struct {
    lang Language
    // Internal: holds a borrowed *wasmSupervisor from shimmy's pool channel
}

func (si *shimInstance) Run(ctx context.Context, code string, inputs map[string]any) (any, error) {
    // Encodes (code, inputs) into shimmy's requestEnvelope format:
    //   {"method": "execute", "params": {"code": code, "inputs": inputs}}
    // Calls supervisor.Send(ctx, "execute", params)
    // Decodes the response
}

func (si *shimInstance) Language() Language {
    return si.lang
}
```

#### Health Check on Release

When an instance is released, the pool checks:
1. Did the last `Run` return an error indicating corrupted state?
2. Is the instance's memory restorable? (shimmy already handles this via snapshot restore)

If unhealthy, the instance is discarded and a new one is created in the background.

#### Error Handling Contract

| Condition | Behavior |
|---|---|
| No module configured for language | Return `ErrLanguageNotSupported` |
| Pool exhausted + acquire timeout | Return `ErrPoolTimeout` |
| WASM module fails to compile | Return error from `Start()`, pool is not usable |
| Guest panic during Run | Return error, instance memory is restored (clean) |
| Pool shutdown while instances in use | Wait for in-use instances to be released (with timeout) |

#### Dependencies

- `github.com/tetratelabs/wazero` (WASM runtime — same as shimmy)
- `github.com/lambda-feedback/shimmy` (adapted pool infrastructure)
- Standard library (`context`, `sync`, `time`)

---

### E. `pkg/primitives` — Built-in Primitive Implementations

**Responsibility:** Provide the standard library of operations that step bodies can invoke — HTTP, filesystem, KV store, and map/reduce combinators.

#### HTTP Primitives

```go
package primitives

import (
    "context"
    "net/http"
)

// HTTPPrimitives handles HTTP request parsing and execution.
type HTTPPrimitives struct {
    client *http.Client
}

func NewHTTPPrimitives(client *http.Client) *HTTPPrimitives

// ParseAndExecute parses an HTTP primitive from a step body string
// and executes it. The body format is:
//   METHOD URL [BODY] [headers: {JSON}]
//
// Examples:
//   GET https://api.example.com/users/1
//   POST https://api.example.com/users {"name": "Alice"} headers: {"Authorization": "Bearer tok"}
//
// Returns the response body as a string (for text) or base64 (for binary).
func (h *HTTPPrimitives) ParseAndExecute(ctx context.Context, body string) (string, error)

// HTTPRequest is the parsed form of an HTTP primitive.
type HTTPRequest struct {
    Method  string
    URL     string
    Body    string
    Headers map[string]string
}

// ParseHTTPRequest extracts an HTTPRequest from a step body line.
func ParseHTTPRequest(line string) (HTTPRequest, error)
```

#### Filesystem Primitives

```go
// FSPrimitives handles filesystem operations, gated by an allowlist of paths.
type FSPrimitives struct {
    allowedPaths []string // WASI-style preopened directory list
}

func NewFSPrimitives(allowedPaths []string) *FSPrimitives

// Execute runs a filesystem primitive. Commands:
//   read <path>
//   write <path> <content>
//   append <path> <content>
//   ls <path>
//
// All paths are validated against the allowedPaths list.
func (f *FSPrimitives) Execute(ctx context.Context, body string) (string, error)
```

#### KV Store

```go
// KVStore is a per-plan in-memory key-value store.
type KVStore interface {
    Get(key string) (any, bool)
    Set(key string, value any)
    Del(key string)
}

// NewMemoryKV creates an in-memory KV store.
func NewMemoryKV() KVStore

// memoryKV is the default in-memory implementation.
type memoryKV struct {
    data map[string]any
    mu   sync.RWMutex
}
```

#### Map/Reduce Combinators

```go
// MapReduce provides parallel map and sequential reduce over lists.
type MapReduce struct {
    executor dag.Executor
}

// Map applies a step template to each element of a list in parallel.
// Returns a list of results in the same order as the input.
func (mr *MapReduce) Map(ctx context.Context, list []any, stepTemplate parser.Step) ([]any, error)

// Reduce applies a step template sequentially, folding left.
// The accumulator is passed as input named "acc", the current element as "item".
func (mr *MapReduce) Reduce(ctx context.Context, list []any, stepTemplate parser.Step, initial any) (any, error)
```

#### Dependencies

- `pkg/parser` (Step type for map/reduce templates)
- `pkg/dag` (Executor interface for map/reduce)
- `net/http` (standard library)

---

### F. `pkg/loom` — Top-Level API

**Responsibility:** Provide the user-facing API that wires together parser, DAG scheduler, executor, and pool into a single cohesive runtime.

#### Public Interface

```go
package loom

import (
    "context"
    "io"
    "net/http"

    "github.com/bkmashiro/loom/pkg/dag"
    "github.com/bkmashiro/loom/pkg/exec"
    "github.com/bkmashiro/loom/pkg/parser"
    "github.com/bkmashiro/loom/pkg/pool"
    "github.com/bkmashiro/loom/pkg/primitives"
)

// Result is the final output of a plan execution.
type Result = dag.Result

// StepResult is a per-step result emitted during streaming execution.
type StepResult = dag.StepResult

// ToolFunc is an externally-registered tool function.
type ToolFunc = exec.ToolFunc

// Loom is the top-level execution runtime.
type Loom struct {
    pool       pool.Pool
    httpClient *http.Client
    tools      *exec.ToolRegistry
    kv         primitives.KVStore
}

// New creates a new Loom runtime with the given options.
func New(opts ...Option) *Loom

// Run parses a plan from the reader and executes it to completion.
// Blocks until the return step produces a result or all steps finish.
func (l *Loom) Run(ctx context.Context, r io.Reader) (Result, error)

// Stream parses a plan from the reader and returns a channel that emits
// StepResults as each step completes. The channel is closed when all
// steps are done.
func (l *Loom) Stream(ctx context.Context, r io.Reader) <-chan StepResult

// RegisterTool registers an escape-hatch tool by name.
func (l *Loom) RegisterTool(name string, fn ToolFunc)

// Start initializes the runtime (pre-warms WASM pool, etc.).
// Must be called before Run or Stream.
func (l *Loom) Start(ctx context.Context) error

// Shutdown releases all resources (WASM instances, connections, etc.).
func (l *Loom) Shutdown(ctx context.Context) error

// --- Options ---

// Option configures a Loom runtime.
type Option func(*Loom)

// WithWASMPool sets the WASM instance pool.
func WithWASMPool(p pool.Pool) Option {
    return func(l *Loom) { l.pool = p }
}

// WithHTTPClient sets the HTTP client for IO/Write steps.
func WithHTTPClient(c *http.Client) Option {
    return func(l *Loom) { l.httpClient = c }
}

// WithKV sets the key-value store for per-plan memory.
func WithKV(store primitives.KVStore) Option {
    return func(l *Loom) { l.kv = store }
}

// WithPoolConfig configures the WASM pool from a PoolConfig.
// Mutually exclusive with WithWASMPool.
func WithPoolConfig(cfg pool.PoolConfig) Option
```

#### Orchestration Flow (Run)

```go
func (l *Loom) Run(ctx context.Context, r io.Reader) (Result, error) {
    // 1. Create executor
    executor := exec.NewStepExecutor(exec.StepExecutorConfig{
        Pool:       l.pool,
        HTTPClient: l.httpClient,
        Tools:      l.tools,
        KV:         l.kv,
    })

    // 2. Create scheduler
    sched := dag.NewScheduler(ctx, executor)

    // 3. Create parser
    p := parser.NewParser(r)
    defer p.Close()

    // 4. Feed parser events into scheduler
    for event := range p.Events() {
        if event.Step != nil {
            if err := sched.Submit(*event.Step); err != nil {
                // Log and continue — malformed steps don't crash the plan
                continue
            }
        }
        if event.Return != nil {
            sched.SetReturn(event.Return.StepID)
        }
    }

    // 5. Check parser error
    if err := p.Err(); err != nil {
        // Parser had an unrecoverable error, but some steps may have
        // already executed. Still try to Wait for results.
    }

    // 6. Wait for result
    return sched.Wait()
}
```

#### Dependencies

- All `pkg/*` packages
- Standard library

---

## 3. Notation Reference (Formal Grammar)

### EBNF Grammar

```ebnf
plan          = { step | return_stmt | text } ;

step          = fence_open , body , fence_close ;
fence_open    = "```" , step_header , newline ;
fence_close   = "```" , newline ;

step_header   = step_type , [ "." , lang ] , [ ws , step_id ] , [ dep_list ] ;
step_type     = "io" | "write" | "pure" | "shell" | "async" | "escape" ;
lang          = identifier ;           (* e.g., "python", "js" *)
step_id       = identifier ;           (* snake_case *)
dep_list      = "(" , identifier , { "," , identifier } , ")" ;

body          = { any_line } ;         (* everything between fences *)

return_stmt   = "return" , ws , identifier , newline ;

text          = any_line ;             (* non-fence, non-return text — ignored *)

identifier    = letter , { letter | digit | "_" } ;
ws            = " " , { " " } ;
newline       = "\n" ;
```

### Header Syntax Variants

All of the following are valid fence headers:

```
```io fetch_user                         — IO step, no deps
```io fetch_user(auth_token)             — IO step, one dep
```pure(fetch_user, fetch_posts) merge   — Pure step, two deps
```pure.python(a, b) transform           — Pure step, Python lang, two deps
```shell build                           — Shell step, no deps
```shell.js compute                      — Shell step, JavaScript
```async                                 — Async step, auto-ID
```async log_event                       — Async step, explicit ID
```escape(input) browser_shot            — Escape step, one dep
```write(order_data) create_order        — Write step, one dep
```

### Dependency Declaration

Dependencies can appear in two positions:
1. **Explicit:** In the parenthesized list in the header — `(dep1, dep2)`
2. **Implicit:** Via `${dep_id}` references in the body (resolved by the executor)

The scheduler uses ONLY explicit deps for ordering. Implicit deps (`${...}`) are a convenience — if a body references `${foo}` but `foo` is not in the explicit dep list, the executor will fail with `ErrMissingInput` at runtime.

### Return Statement

```
return <step_id>
```

- Must appear outside any fence.
- Declares which step's result is the plan's final output.
- If omitted, the last step's result is used.
- Only one return per plan; if multiple appear, the last one wins.

### Error Cases

| Input | Parser Behavior |
|---|---|
| Unknown type keyword: `` ```foo bar `` | Skip fence, emit warning |
| Missing step ID where required | Auto-generate ID (`_step_N`) |
| Empty body | Valid — step executes with empty body |
| Nested fences: `` ``` `` inside a body | The FIRST `` ``` `` on its own line closes the fence |
| Unclosed fence at EOF | Discard partial fence, set parser error |
| Multiple `return` statements | Last one wins |
| `return` referencing unknown step | Scheduler returns `ErrReturnStepNotFound` from `Wait()` |

---

## 4. Shimmy Integration Plan

### What shimmy provides (usable directly)

| Component | Shimmy Location | What It Does | Loom Use |
|---|---|---|---|
| wazero runtime init | `wasm/dispatcher.go:Start()` | Creates `wazero.Runtime`, instantiates WASI | Reuse directly — same wazero setup |
| Module compilation | `wasm/dispatcher.go:Start()` | `rt.CompileModule()` — compile once, share | Reuse directly |
| Instance pool | `wasm/dispatcher.go` | `chan *wasmSupervisor` — channel-based pool | Adapt to `pool.Pool` interface |
| Memory snapshot/restore | `wasm/supervisor.go` | `takeSnapshot()` / `restoreSnapshot()` | Reuse directly — critical for clean per-request state |
| Guest ABI (alloc/evaluate) | `wasm/adapter.go` | JSON envelope → alloc → evaluate → read response | Reuse, but extend for Loom's code-execution use case |
| Dispatcher interface | `dispatcher/dispatcher.go` | `Send(ctx, method, data) (data, error)` | Wrap in `pool.Pool` adapter |

### What needs extraction / adaptation

1. **Pool interface mismatch:** Shimmy's WASM pool is channel-based (`chan *wasmSupervisor`) with acquire/release baked into `Dispatcher.Send()`. Loom needs explicit `Acquire()` / `Release()` to support long-running steps and streaming. The `ShimmyPool` adapter (Section 2D) bridges this gap.

2. **Multi-language support:** Shimmy loads a single `.wasm` module. Loom needs multiple modules (Python, JS, Shell). Solution: create one shimmy `Dispatcher` per language, each with its own compiled module and pool channel.

3. **Guest ABI extension:** Shimmy's guest ABI uses `alloc` + `evaluate` with a `{"method": "...", "params": {...}}` envelope. Loom's code-execution use case needs a slightly different contract:
   - Method: `"execute"` (always)
   - Params: `{"code": "<source>", "inputs": {<dep results>}, "lang": "<language>"}`
   - This is a data-level change, not an ABI change — the same `alloc`/`evaluate` functions work.

4. **WASM modules for each language:** Shimmy does not ship language-specific WASM modules. Loom must provide or build:
   - `python.wasm` — MicroPython compiled to WASI
   - `js.wasm` — QuickJS compiled to WASI
   - `shell.wasm` — Busybox or a restricted shell compiled to WASI

### What is missing (must be built)

| Component | Why It Is Not in Shimmy | What to Build |
|---|---|---|
| Streaming fence parser | Shimmy is an HTTP request handler, not a plan executor | `pkg/parser` — entirely new |
| DAG scheduler | Not applicable to shimmy's use case | `pkg/dag` — entirely new |
| Step executor routing | Shimmy dispatches to a single module type | `pkg/exec` — entirely new |
| HTTP primitives | Shimmy has no HTTP client | `pkg/primitives` — entirely new |
| KV store | Shimmy has no state management | `pkg/primitives` — entirely new |
| Top-level API | Shimmy is a service, not a library | `pkg/loom` — entirely new |

### Proposed shimmy usage strategy

**Do not fork shimmy.** Instead:

1. Import `github.com/lambda-feedback/shimmy` as a Go module dependency.
2. Use `shimmy/internal/execution/wasm` package types. If `internal` visibility is a blocker, use a Go workspace `replace` directive to remap shimmy into `./vendor/shimmy` and remove the `internal` restriction.
3. Alternatively, extract the ~300 lines of WASM pool code (`dispatcher.go`, `supervisor.go`, `adapter.go`, `config.go`) into `pkg/pool/wazero.go` as a standalone implementation. This is the recommended approach since shimmy's pool code is self-contained and Loom needs slightly different semantics (explicit acquire/release vs. shimmy's implicit acquire-send-release).

**Recommended: Option 3 (extract and adapt).** The shimmy WASM pool code is ~400 lines across 4 files with a clean dependency on wazero only. Extracting it avoids the `internal` package visibility issue and allows Loom to customize the pool behavior (multi-language, explicit acquire/release, health checks) without upstream coordination.

---

## 5. Implementation Timeline

### Phase 0: Foundation

**Builds:** `pkg/parser`
**Depends on:** Nothing
**What to build:**
- Streaming fence parser with state machine
- Step and Event types
- Comprehensive test suite with edge cases

**Acceptance criteria:**
- [ ] Test: Parse a single IO step from a string reader → correct Step emitted
- [ ] Test: Parse multiple steps with dependencies → correct dep lists
- [ ] Test: Parse all 6 step types (io, write, pure, shell, async, escape)
- [ ] Test: Language suffix parsing (`pure.python`) → `Step.Lang == "python"`
- [ ] Test: Malformed fence header → fence skipped, parsing continues
- [ ] Test: Unclosed fence at EOF → `Err()` returns error
- [ ] Test: Return directive → ReturnDirective emitted
- [ ] Test: Mixed text and fences → text ignored, fences parsed
- [ ] Test: Streaming behavior — steps emitted before reader is exhausted (use `io.Pipe`)
- [ ] Test: Auto-generated ID for async steps without ID
- [ ] Benchmark: Parse 100-step plan in < 1ms

**Estimated complexity:** Medium

---

### Phase 1: DAG Scheduler

**Builds:** `pkg/dag`
**Depends on:** Phase 0 (`pkg/parser` types)
**What to build:**
- Dependency graph with submit-as-you-go semantics
- Concurrent dispatch of ready steps
- Result collection and propagation
- Cycle detection, forward-ref rejection
- Error propagation and cancellation

**Acceptance criteria:**
- [ ] Test: Linear chain A→B→C — executes sequentially
- [ ] Test: Independent steps A, B, C — executes in parallel (verify with timing)
- [ ] Test: Diamond pattern A→C, B→C — C starts only after both A and B complete
- [ ] Test: Cycle detection — Submit returns `ErrCycle`
- [ ] Test: Forward reference — Submit returns `ErrForwardRef`
- [ ] Test: Duplicate ID — Submit returns `ErrDuplicateStep`
- [ ] Test: Step failure cancels transitive dependents, independent branches continue
- [ ] Test: SetReturn + Wait — returns correct step's result
- [ ] Test: Stream — emits StepResults in completion order
- [ ] Test: Context cancellation — all pending steps cancelled
- [ ] Benchmark: Submit 100 steps, scheduling overhead < 2ms total

**Estimated complexity:** High

---

### Phase 2: WASM Pool

**Builds:** `pkg/pool`
**Depends on:** Nothing (can run in parallel with Phase 0 and 1)
**What to build:**
- Extract and adapt shimmy's WASM pool code
- Multi-language support (one compiled module per language)
- Explicit Acquire/Release API
- Memory snapshot/restore (from shimmy's supervisor)
- Health check on release
- Pool statistics

**Acceptance criteria:**
- [ ] Test: Acquire and Release a Python instance — no error
- [ ] Test: Run simple Python code (`1 + 1`) → returns `2`
- [ ] Test: Run simple JS code (`1 + 1`) → returns `2`
- [ ] Test: Pool exhaustion — Acquire blocks until Release or timeout
- [ ] Test: Memory isolation — state from Run A is not visible in Run B
- [ ] Test: Concurrent Acquire — up to N instances acquired simultaneously
- [ ] Test: Start with invalid module path → returns error
- [ ] Test: Shutdown while instances in use → waits then closes
- [ ] Benchmark: Acquire latency < 1ms (pre-warmed)
- [ ] Benchmark: Run simple code < 5ms

**Estimated complexity:** High

**Note:** This phase requires .wasm modules for each language. As a starting point, use publicly available MicroPython and QuickJS WASI builds. Shell support can be deferred to Phase 4.

---

### Phase 3: Primitives and Executor

**Builds:** `pkg/primitives`, `pkg/exec`
**Depends on:** Phase 0, Phase 1, Phase 2
**What to build:**
- HTTP primitives (parse and execute GET/POST/PUT/DELETE from step body)
- Filesystem primitives (WASI-gated read/write/ls)
- KV store (in-memory per-plan)
- Variable interpolation (`${dep_id}` replacement)
- Step executor routing by type
- Retry logic for IO steps

**Acceptance criteria:**
- [ ] Test: Parse `GET https://example.com` → correct HTTPRequest
- [ ] Test: Parse `POST https://example.com {"key": "val"}` with headers
- [ ] Test: KV set/get/del round-trip
- [ ] Test: FS read/write with allowed path → success
- [ ] Test: FS write to disallowed path → error
- [ ] Test: Variable interpolation `${foo}` with foo in inputs → replaced
- [ ] Test: Variable interpolation `${missing}` → `ErrMissingInput`
- [ ] Test: IO step retry on 500 → retries 3 times
- [ ] Test: Write step on 500 → no retry
- [ ] Test: Escape step with registered tool → tool called
- [ ] Test: Escape step with unknown tool → `ErrToolNotFound`
- [ ] Test: Execute routes IO step to HTTP, Shell step to WASM

**Estimated complexity:** Medium

---

### Phase 4: Top-Level API and Integration

**Builds:** `pkg/loom`, integration tests
**Depends on:** Phase 3
**What to build:**
- Loom struct with Options pattern
- Run (blocking) and Stream (channel) execution paths
- Tool registration
- Start/Shutdown lifecycle
- End-to-end integration tests

**Acceptance criteria:**
- [ ] Test (integration): Full plan — 3 parallel IO steps → pure merge → return
- [ ] Test (integration): Plan with async step — async does not block return
- [ ] Test (integration): Plan with escape step — registered tool is called
- [ ] Test (integration): Streaming — StepResults emitted as steps complete
- [ ] Test (integration): Error in one branch — other branches continue
- [ ] Test (integration): Empty plan (no fences) — returns empty result, no error
- [ ] Test (integration): Plan with only text (no fences) — returns empty result
- [ ] Test: RegisterTool before Run — tool available during execution
- [ ] Test: WithHTTPClient option — custom client used for IO steps
- [ ] Benchmark: 20-step plan with mock IO (< 50ms harness overhead)

**Estimated complexity:** Medium

---

### Phase 5: Polish and Performance

**Builds:** Benchmarks, request coalescing, singleflight, documentation
**Depends on:** Phase 4
**What to build:**
- Request coalescing for identical IO steps (singleflight)
- Connection pooling tuning for HTTP client
- Backpressure support (throttle dispatch when pool is saturated)
- Shell language support in WASM pool (busybox or restricted shell)
- Map/Reduce combinator implementation
- Performance benchmarks matching the targets from DESIGN.md

**Acceptance criteria:**
- [ ] Test: Two identical `GET /api/x` steps in same plan → single HTTP call
- [ ] Test: Backpressure — when pool is full, scheduler slows dispatch rate
- [ ] Test: Map combinator — parallel map over 10-element list
- [ ] Test: Reduce combinator — sequential fold over list
- [ ] Benchmark: Per-step scheduling overhead < 2ms
- [ ] Benchmark: WASM instance acquisition < 1ms
- [ ] Benchmark: 20-step parallel IO plan total harness overhead < 50ms
- [ ] Benchmark: Result delivery between steps < 0.1ms

**Estimated complexity:** Medium

---

## 6. Testing Strategy

### Unit Tests (per package)

Each package has its own `_test.go` files testing public interfaces in isolation.

| Package | Test Focus | Mock Strategy |
|---|---|---|
| `pkg/parser` | Fence parsing edge cases, streaming behavior | Use `io.Pipe` and `strings.Reader` |
| `pkg/dag` | Graph operations, dispatch ordering, error propagation | Mock `Executor` that records calls and returns canned results |
| `pkg/exec` | Routing correctness, retry logic, interpolation | Mock `Pool`, mock HTTP server (`httptest`) |
| `pkg/pool` | Acquire/release, concurrency, timeout, health check | Use a real tiny `.wasm` module (e.g., 10-line C → WASI) |
| `pkg/primitives` | HTTP parsing, FS gating, KV correctness | `httptest.Server`, temp directories |
| `pkg/loom` | Options wiring, lifecycle | Mock all sub-components |

### Integration Tests

Located in `test/integration/`:

```go
func TestFullPlan_ParallelIOToPureMerge(t *testing.T) {
    // Set up a mock HTTP server that returns known data
    // Construct a plan string with 3 IO steps + 1 pure merge + return
    // Create a Loom instance with the mock HTTP server
    // Call Run and verify the result
}

func TestFullPlan_StreamingExecution(t *testing.T) {
    // Use io.Pipe to simulate streaming LLM output
    // Write fence bytes with delays between them
    // Verify that steps start executing before the full plan is written
}

func TestFullPlan_WASMExecution(t *testing.T) {
    // Load real MicroPython and QuickJS WASM modules
    // Execute a plan with pure.python and pure.js steps
    // Verify correct results
}
```

### Benchmark Targets

| Metric | Target | How to Measure |
|---|---|---|
| Per-step scheduling overhead | < 2ms | `b.N` loop: Submit + dispatch with no-op executor |
| WASM instance acquisition | < 1ms | `b.N` loop: Acquire + Release on pre-warmed pool |
| HTTP connection setup | ~0ms | Verify keep-alive reuse via `httptest` |
| Result delivery to next step | < 0.1ms | Measure time from step completion to dependent dispatch |
| Total harness overhead (20 steps) | < 50ms | End-to-end with mock IO (0ms latency) |
| Parser throughput | > 100 plans/ms | Parse 1000 plans of 20 steps each |

### Test Data

Create `testdata/` directory with:
- `simple_plan.txt` — 3 IO + 1 pure + return
- `complex_plan.txt` — 20 steps, diamond dependencies
- `malformed_plan.txt` — various error cases
- `streaming_plan.txt` — plan with interleaved text and fences
- `python.wasm`, `js.wasm` — test WASM modules (or download in CI)

---

## 7. Open Questions

1. **WASM module sourcing:** Where do the MicroPython, QuickJS, and shell WASM modules come from? Options:
   - Pre-built binaries checked into the repo (large, ~5MB each)
   - Downloaded in CI from a known URL
   - Built from source in a separate build step
   - **Recommendation:** Download pre-built WASI binaries in CI, cache them

2. **Guest ABI for code execution:** Shimmy's ABI expects `alloc` + `evaluate` exports. MicroPython-wasm and QuickJS-wasm have their own APIs. Do we:
   - Write a thin C wrapper that conforms to shimmy's ABI and delegates to the language runtime?
   - Adapt Loom's pool to call language-specific exports directly?
   - **Recommendation:** Thin C wrapper per language, keeping the ABI uniform

3. **Pure step fast path:** Should `pure` steps always run in WASM, or can they run as in-process Go code? If in-process, how do we handle untrusted code?
   - **Recommendation:** Default to WASM; offer a `WithUnsafePure()` option for trusted environments that runs pure steps as goroutines (eval via Yaegi or similar Go interpreter)

4. **Streaming parse + execution overlap:** If the parser emits a step while the scheduler is dispatching another, what happens to backpressure? Should the events channel be buffered?
   - **Recommendation:** Buffered channel (capacity 64). Parser produces faster than scheduler consumes in practice, but a large buffer prevents blocking the parser goroutine.

5. **Error granularity in results:** Should `Result.Data` contain structured error information (error code, message, partial data) or just the raw output?
   - **Recommendation:** `Result.Data` is always the raw output (or nil on error). `Result.Err` carries the error. Callers can inspect error types via `errors.Is` / `errors.As`.

6. **Plan size limits:** Should there be a maximum number of steps per plan to prevent resource exhaustion?
   - **Recommendation:** Default limit of 1000 steps, configurable via `WithMaxSteps(n)` option.

7. **HTTP base URL:** IO steps use relative URLs in the design doc examples (`GET /api/users`). How is the base URL resolved?
   - **Recommendation:** Require a `WithBaseURL(url)` option. Relative URLs are resolved against it. Absolute URLs are used as-is.

8. **Concurrency limit for IO steps:** Should there be a global limit on concurrent outbound HTTP requests, separate from the WASM pool?
   - **Recommendation:** Yes. Use `http.Client.Transport` with `MaxConnsPerHost` (default: 10) and a global semaphore (default: 50 concurrent requests), configurable via `WithMaxConcurrentIO(n)`.

9. **Shimmy dependency strategy:** Import shimmy as a module, vendor it, or extract the relevant code?
   - shimmy's WASM code is in `internal/` (unexportable). Forking or extracting is necessary.
   - **Recommendation:** Extract the 4 WASM files (~400 lines) into `pkg/pool/wazero/`, with attribution. This avoids a hard dependency on shimmy's release cycle.

10. **Map/Reduce step syntax:** How are `map` and `reduce` expressed in the fence notation? Are they their own step types, or special body keywords within `pure` steps?
    - **Recommendation:** Body keywords within pure steps. `map` and `reduce` are parsed by the executor, not the fence parser. This keeps the step type taxonomy simple.

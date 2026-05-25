package loom

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/bkmashiro/loom/pkg/dag"
	"github.com/bkmashiro/loom/pkg/dispatch"
	"github.com/bkmashiro/loom/pkg/exec"
	"github.com/bkmashiro/loom/pkg/fn"
	"github.com/bkmashiro/loom/pkg/parser"
	"github.com/bkmashiro/loom/pkg/pool"
	"github.com/bkmashiro/loom/pkg/primitives"
	"github.com/bkmashiro/loom/pkg/sandbox"
)

// Re-export key types so callers don't need to import sub-packages.
type Result = dag.Result
type StepResult = dag.StepResult
type ToolFunc = exec.ToolFunc

// Loom is the top-level runtime that wires parser, DAG scheduler, and executor together.
type Loom struct {
	pool            pool.Pool
	httpClient      *http.Client
	tools           *exec.ToolRegistry
	kv              primitives.KVStore
	ioCacheCap      int
	ioCacheTTL      time.Duration
	sandboxCfg      *sandbox.Config
	agentUpstream   string
	agentAPIKey     string
	agentModel      string
	dispatchWorkers []dispatch.Worker
}

// Option configures a Loom runtime.
type Option func(*Loom)

// WithWASMPool sets the WASM pool used for pure/shell steps.
func WithWASMPool(p pool.Pool) Option {
	return func(l *Loom) {
		l.pool = p
	}
}

// WithHTTPClient sets the HTTP client used for IO/Write steps.
func WithHTTPClient(c *http.Client) Option {
	return func(l *Loom) {
		l.httpClient = c
	}
}

// WithKV sets the KV store for the runtime.
func WithKV(store primitives.KVStore) Option {
	return func(l *Loom) {
		l.kv = store
	}
}

// WithIOCache enables the per-executor LRU+TTL cache for IO step results.
// cap is the maximum number of entries; ttl is how long each entry lives.
// Both must be > 0 for the cache to be enabled.
func WithIOCache(cap int, ttl time.Duration) Option {
	return func(l *Loom) {
		l.ioCacheCap = cap
		l.ioCacheTTL = ttl
	}
}

// WithSandbox configures the filesystem sandbox for each plan execution.
// Each Run/Stream call creates an independent Sandbox instance from cfg.
func WithSandbox(cfg sandbox.Config) Option {
	return func(l *Loom) { l.sandboxCfg = &cfg }
}

// WithAgentUpstream configures the LLM upstream used for agent steps.
// url is the base URL (e.g. "https://api.openai.com"), apiKey is the Bearer token,
// and defaultModel is used when the step body does not specify a model.
func WithAgentUpstream(url, apiKey, defaultModel string) Option {
	return func(l *Loom) {
		l.agentUpstream = url
		l.agentAPIKey = apiKey
		l.agentModel = defaultModel
	}
}

// WithDispatch registers remote or custom workers. Steps are routed to the first
// matching worker; a DefaultLocalWorker wrapping the built-in executor is appended
// last as the catch-all fallback.
func WithDispatch(workers ...dispatch.Worker) Option {
	return func(l *Loom) {
		l.dispatchWorkers = workers
	}
}

// New creates a Loom with defaults (http.DefaultClient, in-memory KV, no WASM pool).
func New(opts ...Option) *Loom {
	l := &Loom{
		httpClient: http.DefaultClient,
		tools:      exec.NewToolRegistry(),
		kv:         primitives.NewMemoryKV(),
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// RegisterTool registers an escape-hatch tool by name.
func (l *Loom) RegisterTool(name string, toolFn ToolFunc) {
	l.tools.Register(name, toolFn)
}

// openSandbox creates a new Sandbox from l.sandboxCfg, or returns nil if unconfigured.
func (l *Loom) openSandbox() (*sandbox.Sandbox, error) {
	if l.sandboxCfg == nil {
		return nil, nil
	}
	return sandbox.New(*l.sandboxCfg)
}

// newExecutorWithSandbox builds a StepExecutor using the given sandbox (may be nil).
func (l *Loom) newExecutorWithSandbox(sb *sandbox.Sandbox) *exec.StepExecutor {
	return exec.NewStepExecutor(exec.StepExecutorConfig{
		Pool:          l.pool,
		HTTPClient:    l.httpClient,
		Tools:         l.tools,
		KV:            l.kv,
		IOCacheCap:    l.ioCacheCap,
		IOCacheTTL:    l.ioCacheTTL,
		Sandbox:       sb,
		AgentUpstream: l.agentUpstream,
		AgentAPIKey:   l.agentAPIKey,
		AgentModel:    l.agentModel,
	})
}

// newExecutor builds a StepExecutor from the current Loom configuration.
func (l *Loom) newExecutor() *exec.StepExecutor {
	return l.newExecutorWithSandbox(nil)
}

// buildExecutor returns the dag.Executor to use for a plan run.
// If dispatchWorkers are configured, it wraps the local executor in a Dispatcher
// with the local executor registered last as the catch-all fallback.
func (l *Loom) buildExecutor(sb *sandbox.Sandbox) dag.Executor {
	local := l.newExecutorWithSandbox(sb)
	if len(l.dispatchWorkers) == 0 {
		return local
	}
	d := dispatch.New()
	for _, w := range l.dispatchWorkers {
		d.Register(w)
	}
	// Local executor as the last-resort fallback (handles all types/langs).
	d.Register(dispatch.DefaultLocalWorker(local))
	return d
}

// processEvent handles a single parser event, dispatching FuncDef/FuncCall
// steps through the fn.Registry and submitting all others directly to the scheduler.
func (l *Loom) processEvent(ev parser.Event, sched *dag.Scheduler, reg *fn.Registry) error {
	if ev.Step == nil {
		return nil
	}
	switch ev.Step.Type {
	case parser.FuncDef:
		return reg.Register(*ev.Step)
	case parser.FuncCall:
		expanded, returnID, err := reg.Expand(*ev.Step)
		if err != nil {
			return err
		}
		for _, s := range expanded {
			if err := sched.Submit(s); err != nil {
				return err
			}
		}
		// The call step's ID maps to the function's return step via a passthrough pure step.
		passthrough := parser.Step{
			ID:   ev.Step.ID,
			Type: parser.Pure,
			Deps: []string{returnID},
			Body: returnID, // identity: return the return step's value
		}
		return sched.Submit(passthrough)
	default:
		return sched.Submit(*ev.Step)
	}
}

// Run parses a complete plan from r and executes it, returning the final result.
// Blocks until the return step (or all steps) complete.
func (l *Loom) Run(ctx context.Context, r io.Reader) (Result, error) {
	sb, err := l.openSandbox()
	if err != nil {
		return Result{}, err
	}
	if sb != nil {
		defer sb.Close()
	}
	executor := l.buildExecutor(sb)
	sched := dag.NewScheduler(ctx, executor)
	reg := fn.NewRegistry()
	p := parser.NewParser(r)
	defer p.Close()

	for event := range p.Events() {
		if event.Step != nil {
			if err := l.processEvent(event, sched, reg); err != nil {
				// log and continue — malformed steps don't crash the plan
				_ = err
			}
		}
		if event.Return != nil {
			sched.SetReturn(event.Return.StepID)
		}
	}

	sched.Seal() // signal no more steps coming

	if err := p.Err(); err != nil {
		// parser error — but some steps may have executed; still wait
		_ = err
	}

	return sched.Wait()
}

// Stream parses a plan from r and returns a channel emitting StepResults as
// each step completes. Channel is closed when all steps are done.
func (l *Loom) Stream(ctx context.Context, r io.Reader) <-chan StepResult {
	sb, err := l.openSandbox()
	if err != nil {
		// Return a channel that emits nothing — sandbox creation failure.
		ch := make(chan StepResult)
		close(ch)
		return ch
	}
	executor := l.buildExecutor(sb)
	sched := dag.NewScheduler(ctx, executor)
	reg := fn.NewRegistry()

	ch := sched.Stream() // subscribe before parsing starts

	go func() {
		if sb != nil {
			defer sb.Close()
		}
		p := parser.NewParser(r)
		defer p.Close()
		for event := range p.Events() {
			if event.Step != nil {
				l.processEvent(event, sched, reg) //nolint
			}
			if event.Return != nil {
				sched.SetReturn(event.Return.StepID)
			}
		}
		sched.Seal()
	}()

	return ch
}

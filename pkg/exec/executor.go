package exec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bkmashiro/loom/pkg/dag"
	"github.com/bkmashiro/loom/pkg/parser"
	"github.com/bkmashiro/loom/pkg/pool"
	"github.com/bkmashiro/loom/pkg/primitives"
	"golang.org/x/sync/singleflight"
)

// Sentinel errors.
var (
	ErrUnknownPrimitive = errors.New("exec: unknown primitive in step body")
	ErrToolNotFound     = errors.New("exec: escape tool not found")
	ErrMissingInput     = errors.New("exec: missing interpolation variable")
	ErrNoPool           = errors.New("exec: WASM pool not configured")
)

// ToolFunc is a callable registered tool.
type ToolFunc func(ctx context.Context, args map[string]any) (any, error)

// ToolRegistry holds named tool functions.
type ToolRegistry struct {
	mu    sync.RWMutex
	tools map[string]ToolFunc
}

// NewToolRegistry creates an empty ToolRegistry.
func NewToolRegistry() *ToolRegistry {
	return &ToolRegistry{tools: make(map[string]ToolFunc)}
}

// Register adds a tool under name.
func (r *ToolRegistry) Register(name string, fn ToolFunc) {
	r.mu.Lock()
	r.tools[name] = fn
	r.mu.Unlock()
}

// Get looks up a tool by name.
func (r *ToolRegistry) Get(name string) (ToolFunc, bool) {
	r.mu.RLock()
	fn, ok := r.tools[name]
	r.mu.RUnlock()
	return fn, ok
}

// StepExecutorConfig holds configuration for StepExecutor.
type StepExecutorConfig struct {
	Pool        pool.Pool          // may be nil (WASM steps will fail if nil)
	HTTPClient  *http.Client       // if nil, use http.DefaultClient
	Tools       *ToolRegistry      // if nil, escape steps always return ErrToolNotFound
	KV          primitives.KVStore // if nil, kv steps are not supported
	IOCacheCap  int                // 0 = disabled
	IOCacheTTL  time.Duration      // 0 = disabled
}

// StepExecutor implements dag.Executor, routing each step by type.
type StepExecutor struct {
	pool       pool.Pool
	httpClient *http.Client
	tools      *ToolRegistry
	kv         primitives.KVStore
	sf         singleflight.Group
	cache      *IOCache // nil if caching disabled
}

// Ensure interface is satisfied.
var _ dag.Executor = (*StepExecutor)(nil)

// NewStepExecutor creates a StepExecutor from config.
func NewStepExecutor(cfg StepExecutorConfig) *StepExecutor {
	client := cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	e := &StepExecutor{
		pool:       cfg.Pool,
		httpClient: client,
		tools:      cfg.Tools,
		kv:         cfg.KV,
	}
	if cfg.IOCacheCap > 0 && cfg.IOCacheTTL > 0 {
		e.cache = NewIOCache(cfg.IOCacheCap, cfg.IOCacheTTL)
	}
	return e
}

// Execute runs a single step, routing by StepType.
func (e *StepExecutor) Execute(ctx context.Context, step parser.Step, inputs map[string]dag.Result) (dag.Result, error) {
	// 1. Interpolate ${dep_id} in body.
	body, err := interpolate(step.Body, inputs)
	if err != nil {
		return dag.Result{StepID: step.ID, Err: err}, err
	}

	var data any
	switch step.Type {
	case parser.IO:
		data, err = e.executeIO(ctx, body, true) // retryable
	case parser.Write:
		data, err = e.executeIO(ctx, body, false) // not retryable
	case parser.Pure:
		data, err = e.executePure(ctx, step, body, inputs)
	case parser.Shell:
		data, err = e.executeWASM(ctx, pool.LangShell, body, inputs)
	case parser.Async:
		// Fire-and-forget; always return immediately.
		go e.executeIO(ctx, body, false) //nolint
		data = nil
	case parser.Escape:
		data, err = e.executeEscape(ctx, body, inputs)
	default:
		err = fmt.Errorf("%w: type=%d", ErrUnknownPrimitive, step.Type)
	}

	return dag.Result{StepID: step.ID, Data: data, Err: err}, err
}

// interpolate replaces ${key} tokens in body with JSON-encoded values from inputs.
func interpolate(body string, inputs map[string]dag.Result) (string, error) {
	var sb strings.Builder
	var firstErr error
	rest := body
	for {
		start := strings.Index(rest, "${")
		if start == -1 {
			sb.WriteString(rest)
			break
		}
		sb.WriteString(rest[:start])
		rest = rest[start+2:] // skip "${"
		end := strings.Index(rest, "}")
		if end == -1 {
			// No closing brace — write the literal and stop.
			sb.WriteString("${")
			sb.WriteString(rest)
			break
		}
		key := rest[:end]
		rest = rest[end+1:]
		result, ok := inputs[key]
		if !ok {
			if firstErr == nil {
				firstErr = fmt.Errorf("%w: %s", ErrMissingInput, key)
			}
			// Write placeholder so interpolation continues.
			sb.WriteString("${")
			sb.WriteString(key)
			sb.WriteString("}")
			continue
		}
		encoded, err := json.Marshal(result.Data)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("exec: marshal input %s: %w", key, err)
			}
			sb.WriteString("${")
			sb.WriteString(key)
			sb.WriteString("}")
			continue
		}
		sb.Write(encoded)
	}
	return sb.String(), firstErr
}

// firstNonEmpty returns the first non-empty, trimmed line from s.
func firstNonEmpty(s string) string {
	for _, l := range strings.Split(s, "\n") {
		l = strings.TrimSpace(l)
		if l != "" {
			return l
		}
	}
	return ""
}

// executeIO parses the first non-empty line as an HTTP request and executes it.
// If retryable, retries on 5xx or network errors up to 3 times with exponential backoff.
// Identical concurrent requests (same method+url+body) are coalesced via singleflight.
// For retryable (IO) steps, results are stored in the LRU+TTL cache (if configured).
func (e *StepExecutor) executeIO(ctx context.Context, body string, retryable bool) (any, error) {
	line := firstNonEmpty(body)
	if line == "" {
		return nil, fmt.Errorf("exec: IO step body is empty")
	}

	req, err := primitives.ParseHTTPRequest(line)
	if err != nil {
		return nil, fmt.Errorf("exec: parse HTTP request: %w", err)
	}

	// Cache lookup: only for retryable (idempotent) IO steps.
	var ck string
	if e.cache != nil && retryable {
		ck = cacheKey(req.Method, req.URL, req.Body)
		if cached, ok := e.cache.Get(ck); ok {
			return cached, nil
		}
	}

	// Singleflight key: method + URL + body to coalesce identical concurrent requests.
	sfKey := req.Method + " " + req.URL + " " + req.Body

	result, err, _ := e.sf.Do(sfKey, func() (any, error) {
		if !retryable {
			return e.doHTTP(ctx, req)
		}

		// Retry with exponential backoff: 100ms, 200ms, 400ms.
		backoffs := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
		var lastErr error
		for attempt := 0; attempt <= len(backoffs); attempt++ {
			var res string
			res, lastErr = e.doHTTP(ctx, req)
			if lastErr == nil {
				return res, nil
			}
			// Check if it's a 5xx error wrapped in our sentinel.
			if !isRetryable(lastErr) {
				return nil, lastErr
			}
			if attempt < len(backoffs) {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(backoffs[attempt]):
				}
			}
		}
		return nil, lastErr
	})

	// Store successful result in cache for retryable steps.
	if err == nil && e.cache != nil && retryable {
		e.cache.Set(ck, result)
	}

	return result, err
}

// retryableHTTPError is used to distinguish 5xx errors from other errors.
type retryableHTTPError struct {
	statusCode int
	body       string
}

func (e *retryableHTTPError) Error() string {
	return fmt.Sprintf("exec: HTTP %d: %s", e.statusCode, e.body)
}

func isRetryable(err error) bool {
	var rhe *retryableHTTPError
	return errors.As(err, &rhe)
}

// doHTTP builds and executes an *http.Request from a primitives.HTTPRequest.
func (e *StepExecutor) doHTTP(ctx context.Context, req primitives.HTTPRequest) (string, error) {
	var bodyReader io.Reader
	if req.Body != "" {
		bodyReader = strings.NewReader(req.Body)
	}

	httpReq, err := http.NewRequestWithContext(ctx, req.Method, req.URL, bodyReader)
	if err != nil {
		return "", fmt.Errorf("exec: build HTTP request: %w", err)
	}

	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}
	if req.Body != "" && httpReq.Header.Get("Content-Type") == "" {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	resp, err := e.httpClient.Do(httpReq)
	if err != nil {
		return "", &retryableHTTPError{statusCode: 0, body: err.Error()}
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("exec: read HTTP response: %w", err)
	}

	if resp.StatusCode >= 500 {
		return "", &retryableHTTPError{statusCode: resp.StatusCode, body: string(respBody)}
	}

	return string(respBody), nil
}

// executePure routes by Lang to WASM or returns body as-is for in-process eval.
func (e *StepExecutor) executePure(ctx context.Context, step parser.Step, body string, inputs map[string]dag.Result) (any, error) {
	switch step.Lang {
	case "python":
		return e.executeWASM(ctx, pool.LangPython, body, inputs)
	case "js", "javascript":
		return e.executeWASM(ctx, pool.LangJS, body, inputs)
	default:
		// In-process placeholder: return body as-is.
		return body, nil
	}
}

// executeWASM acquires a pool instance and runs code in it.
func (e *StepExecutor) executeWASM(ctx context.Context, lang pool.Language, code string, inputs map[string]dag.Result) (any, error) {
	if e.pool == nil {
		return nil, ErrNoPool
	}
	inst, err := e.pool.Acquire(ctx, lang)
	if err != nil {
		return nil, fmt.Errorf("exec: acquire WASM instance: %w", err)
	}
	defer e.pool.Release(inst)

	rawInputs := make(map[string]any, len(inputs))
	for k, v := range inputs {
		rawInputs[k] = v.Data
	}

	return inst.Run(ctx, code, rawInputs)
}

// executeEscape dispatches a "@tool <name> <json-args>" step to a registered tool.
func (e *StepExecutor) executeEscape(ctx context.Context, body string, inputs map[string]dag.Result) (any, error) {
	if e.tools == nil {
		return nil, ErrToolNotFound
	}

	// Find first non-empty line.
	line := ""
	for _, l := range strings.Split(body, "\n") {
		l = strings.TrimSpace(l)
		if l != "" {
			line = l
			break
		}
	}

	// Strip "@tool " prefix.
	line = strings.TrimPrefix(line, "@tool ")
	line = strings.TrimSpace(line)

	// Split on first space to get name and args.
	spaceIdx := strings.Index(line, " ")
	var name, argsStr string
	if spaceIdx == -1 {
		name = line
		argsStr = "{}"
	} else {
		name = line[:spaceIdx]
		argsStr = strings.TrimSpace(line[spaceIdx+1:])
	}

	fn, ok := e.tools.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrToolNotFound, name)
	}

	var args map[string]any
	if err := json.Unmarshal([]byte(argsStr), &args); err != nil {
		return nil, fmt.Errorf("exec: unmarshal tool args for %q: %w", name, err)
	}

	return fn(ctx, args)
}

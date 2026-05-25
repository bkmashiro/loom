package exec

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bkmashiro/loom/pkg/dag"
	"github.com/bkmashiro/loom/pkg/parser"
	"github.com/bkmashiro/loom/pkg/sandbox"
)

// --- Helpers ---

func makeExecutor(opts ...func(*StepExecutorConfig)) *StepExecutor {
	cfg := StepExecutorConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	return NewStepExecutor(cfg)
}

func withCache(cap int, ttl time.Duration) func(*StepExecutorConfig) {
	return func(cfg *StepExecutorConfig) {
		cfg.IOCacheCap = cap
		cfg.IOCacheTTL = ttl
	}
}

func withClient(c *http.Client) func(*StepExecutorConfig) {
	return func(cfg *StepExecutorConfig) { cfg.HTTPClient = c }
}

func withTools(r *ToolRegistry) func(*StepExecutorConfig) {
	return func(cfg *StepExecutorConfig) { cfg.Tools = r }
}

func withSandbox(sb *sandbox.Sandbox) func(*StepExecutorConfig) {
	return func(cfg *StepExecutorConfig) { cfg.Sandbox = sb }
}

func mustSandbox(t *testing.T, cfg sandbox.Config) *sandbox.Sandbox {
	t.Helper()
	sb, err := sandbox.New(cfg)
	if err != nil {
		t.Fatalf("sandbox.New: %v", err)
	}
	t.Cleanup(func() { sb.Close() })
	return sb
}

// --- Test 1: interpolate ---

func TestInterpolate(t *testing.T) {
	inputs := map[string]dag.Result{
		"foo": {StepID: "foo", Data: "hello"},
		"num": {StepID: "num", Data: 42},
	}

	t.Run("replaces token with JSON", func(t *testing.T) {
		out, err := interpolate("prefix ${foo} suffix", inputs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if out != `prefix "hello" suffix` {
			t.Errorf("got %q", out)
		}
	})

	t.Run("replaces numeric token", func(t *testing.T) {
		out, err := interpolate("value=${num}", inputs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if out != "value=42" {
			t.Errorf("got %q", out)
		}
	})

	t.Run("missing key returns ErrMissingInput", func(t *testing.T) {
		_, err := interpolate("${missing}", inputs)
		if !errors.Is(err, ErrMissingInput) {
			t.Errorf("expected ErrMissingInput, got %v", err)
		}
	})

	t.Run("no tokens returns body unchanged", func(t *testing.T) {
		out, err := interpolate("no tokens here", inputs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if out != "no tokens here" {
			t.Errorf("got %q", out)
		}
	})
}

// --- Test 2: executeIO GET ---

func TestExecuteIO_GET(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/hello" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, "world")
	}))
	defer srv.Close()

	e := makeExecutor(withClient(srv.Client()))

	step := parser.Step{
		ID:   "fetch",
		Type: parser.IO,
		Body: "GET " + srv.URL + "/hello",
	}

	res, err := e.Execute(context.Background(), step, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Data != "world" {
		t.Errorf("expected 'world', got %v", res.Data)
	}
}

// --- Test 3: executeIO retry ---

func TestExecuteIO_Retry(t *testing.T) {
	var attempts int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&attempts, 1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "error")
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	t.Run("IO retries on 5xx", func(t *testing.T) {
		atomic.StoreInt32(&attempts, 0)
		e := makeExecutor(withClient(srv.Client()))
		step := parser.Step{
			ID:   "fetch",
			Type: parser.IO,
			Body: "GET " + srv.URL + "/retry",
		}
		res, err := e.Execute(context.Background(), step, nil)
		if err != nil {
			t.Fatalf("expected success after retries, got %v", err)
		}
		if res.Data != "ok" {
			t.Errorf("expected 'ok', got %v", res.Data)
		}
		if atomic.LoadInt32(&attempts) < 3 {
			t.Errorf("expected at least 3 attempts, got %d", atomic.LoadInt32(&attempts))
		}
	})

	t.Run("Write does not retry on 5xx", func(t *testing.T) {
		atomic.StoreInt32(&attempts, 0)
		e := makeExecutor(withClient(srv.Client()))
		step := parser.Step{
			ID:   "write",
			Type: parser.Write,
			Body: "GET " + srv.URL + "/retry",
		}
		_, err := e.Execute(context.Background(), step, nil)
		if err == nil {
			t.Fatal("expected error for Write step on 5xx without retry")
		}
		if atomic.LoadInt32(&attempts) != 1 {
			t.Errorf("expected exactly 1 attempt for Write, got %d", atomic.LoadInt32(&attempts))
		}
	})
}

// --- Test 4: executePure default (no lang) ---

func TestExecutePure_Default(t *testing.T) {
	e := makeExecutor()
	step := parser.Step{
		ID:   "pure",
		Type: parser.Pure,
		Lang: "", // no lang
		Body: "some computation body",
	}
	res, err := e.Execute(context.Background(), step, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Data != "some computation body" {
		t.Errorf("expected body as-is, got %v", res.Data)
	}
}

// --- Test 5: executeEscape found ---

func TestExecuteEscape_Found(t *testing.T) {
	reg := NewToolRegistry()
	var gotArgs map[string]any
	reg.Register("mytool", func(ctx context.Context, args map[string]any) (any, error) {
		gotArgs = args
		return "tool-result", nil
	})

	e := makeExecutor(withTools(reg))
	step := parser.Step{
		ID:   "esc",
		Type: parser.Escape,
		Body: `@tool mytool {"key": "value", "n": 3}`,
	}

	res, err := e.Execute(context.Background(), step, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Data != "tool-result" {
		t.Errorf("expected 'tool-result', got %v", res.Data)
	}
	if gotArgs["key"] != "value" {
		t.Errorf("expected key=value in args, got %v", gotArgs)
	}
	if gotArgs["n"] != float64(3) {
		t.Errorf("expected n=3 in args, got %v", gotArgs["n"])
	}
}

// --- Test 6: executeEscape not found ---

func TestExecuteEscape_NotFound(t *testing.T) {
	reg := NewToolRegistry()
	e := makeExecutor(withTools(reg))

	step := parser.Step{
		ID:   "esc",
		Type: parser.Escape,
		Body: `@tool notregistered {}`,
	}

	_, err := e.Execute(context.Background(), step, nil)
	if !errors.Is(err, ErrToolNotFound) {
		t.Errorf("expected ErrToolNotFound, got %v", err)
	}
}

// --- Test 7: executeAsync ---

func TestExecuteAsync(t *testing.T) {
	var called int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.StoreInt32(&called, 1)
		fmt.Fprint(w, "async-ok")
	}))
	defer srv.Close()

	e := makeExecutor(withClient(srv.Client()))
	step := parser.Step{
		ID:   "async",
		Type: parser.Async,
		Body: "GET " + srv.URL + "/async",
	}

	start := time.Now()
	res, err := e.Execute(context.Background(), step, nil)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Async Execute: %v", err)
	}
	if res.Data != nil {
		t.Errorf("expected nil data for async, got %v", res.Data)
	}
	// Should return very quickly (fire and forget).
	if elapsed > 100*time.Millisecond {
		t.Errorf("async step took too long: %v", elapsed)
	}
}

// --- Test 8: unknown step type ---

func TestExecuteUnknown(t *testing.T) {
	e := makeExecutor()
	step := parser.Step{
		ID:   "unk",
		Type: parser.StepType(99),
		Body: "something",
	}

	_, err := e.Execute(context.Background(), step, nil)
	if !errors.Is(err, ErrUnknownPrimitive) {
		t.Errorf("expected ErrUnknownPrimitive, got %v", err)
	}
}

// --- Test 9: singleflight coalesces identical concurrent IO steps ---

func TestSingleflight(t *testing.T) {
	var callCount int32

	// The server adds a small delay so concurrent requests overlap and singleflight can coalesce them.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		time.Sleep(30 * time.Millisecond)
		fmt.Fprint(w, "coalesced-result")
	}))
	defer srv.Close()

	url := srv.URL + "/data"
	body := "GET " + url

	// Build executor and scheduler with 3 independent identical IO steps.
	e := makeExecutor(withClient(srv.Client()))
	sched := dag.NewScheduler(context.Background(), e)

	// Submit 3 steps with identical method+URL — no deps so they dispatch in parallel.
	for i := 0; i < 3; i++ {
		step := parser.Step{
			ID:   fmt.Sprintf("io%d", i),
			Type: parser.IO,
			Body: body,
		}
		if err := sched.Submit(step); err != nil {
			t.Fatalf("Submit step io%d: %v", i, err)
		}
	}
	sched.Seal()

	// Collect all results via Stream.
	var results []dag.Result
	for sr := range sched.Stream() {
		results = append(results, sr.Result)
	}

	// All 3 steps should have succeeded.
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Err != nil {
			t.Errorf("step %s errored: %v", r.StepID, r.Err)
		}
		if r.Data != "coalesced-result" {
			t.Errorf("step %s: expected 'coalesced-result', got %v", r.StepID, r.Data)
		}
	}

	// Singleflight should have resulted in only 1 actual HTTP call.
	n := atomic.LoadInt32(&callCount)
	if n != 1 {
		t.Errorf("expected 1 HTTP call (singleflight coalescing), got %d", n)
	}
}

// --- Test 10: IO cache hit avoids second HTTP request ---

func TestExecutor_IOCacheHit(t *testing.T) {
	var callCount int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		fmt.Fprint(w, "cached-result")
	}))
	defer srv.Close()

	e := makeExecutor(
		withClient(srv.Client()),
		withCache(128, time.Minute),
	)

	step := parser.Step{
		ID:   "fetch",
		Type: parser.IO,
		Body: "GET " + srv.URL + "/data",
	}

	// First call — should hit the server.
	res1, err := e.Execute(context.Background(), step, nil)
	if err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	if res1.Data != "cached-result" {
		t.Errorf("expected 'cached-result', got %v", res1.Data)
	}

	// Second identical call — should be served from cache.
	res2, err := e.Execute(context.Background(), step, nil)
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if res2.Data != "cached-result" {
		t.Errorf("expected 'cached-result', got %v", res2.Data)
	}

	// Only one HTTP call should have been made.
	n := atomic.LoadInt32(&callCount)
	if n != 1 {
		t.Errorf("expected 1 HTTP call with cache enabled, got %d", n)
	}
}

// --- Test 11: Write steps are not cached ---

func TestExecutor_WriteNotCached(t *testing.T) {
	var callCount int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		fmt.Fprint(w, "write-result")
	}))
	defer srv.Close()

	e := makeExecutor(
		withClient(srv.Client()),
		withCache(128, time.Minute),
	)

	step := parser.Step{
		ID:   "write",
		Type: parser.Write,
		Body: "POST " + srv.URL + "/data",
	}

	// First Write call.
	_, err := e.Execute(context.Background(), step, nil)
	if err != nil {
		t.Fatalf("first Execute: %v", err)
	}

	// Second identical Write call — must NOT be served from cache.
	_, err = e.Execute(context.Background(), step, nil)
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}

	// Both calls must reach the server (Write steps are never cached).
	n := atomic.LoadInt32(&callCount)
	if n != 2 {
		t.Errorf("expected 2 HTTP calls for Write steps (no cache), got %d", n)
	}
}

// --- Test 12: FS write via write step with ephemeral sandbox ---

func TestExecutor_FSWrite_Ephemeral(t *testing.T) {
	sb := mustSandbox(t, sandbox.EphemeralSandbox())
	e := makeExecutor(withSandbox(sb))

	step := parser.Step{
		ID:   "fswrite",
		Type: parser.Write,
		Body: "write /tmp/out.txt hello",
	}

	res, err := e.Execute(context.Background(), step, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Data != nil {
		t.Errorf("expected nil data for write, got %v", res.Data)
	}

	got, err := sb.ReadFile("/tmp/out.txt")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("expected 'hello', got %q", string(got))
	}
}

// --- Test 13: FS read via io step with pre-populated ephemeral sandbox ---

func TestExecutor_FSRead_Ephemeral(t *testing.T) {
	sb := mustSandbox(t, sandbox.EphemeralSandbox())
	if err := sb.WriteFile("/tmp/data.txt", []byte("file-content"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	e := makeExecutor(withSandbox(sb))

	// Use a write step (disambiguated to FS because body starts with "read").
	step := parser.Step{
		ID:   "fsread",
		Type: parser.Write,
		Body: "read /tmp/data.txt",
	}

	res, err := e.Execute(context.Background(), step, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	data, ok := res.Data.([]byte)
	if !ok {
		t.Fatalf("expected []byte data, got %T: %v", res.Data, res.Data)
	}
	if string(data) != "file-content" {
		t.Errorf("expected 'file-content', got %q", string(data))
	}
}

// --- Test 14: FS write with nil sandbox returns ErrNoSandbox ---

func TestExecutor_FSWrite_NoSandbox(t *testing.T) {
	e := makeExecutor() // no sandbox

	step := parser.Step{
		ID:   "fswrite",
		Type: parser.Write,
		Body: "write /tmp/out.txt hello",
	}

	_, err := e.Execute(context.Background(), step, nil)
	if !errors.Is(err, sandbox.ErrNoSandbox) {
		t.Errorf("expected ErrNoSandbox, got %v", err)
	}
}

// --- Test 15: FS write against read-only sandbox returns ErrReadOnly ---

func TestExecutor_FSWrite_ReadOnly(t *testing.T) {
	// Use a temp dir to back the read-only mount.
	t.TempDir() // ensure temp infra is set up
	sb := mustSandbox(t, sandbox.ReadOnlySandbox(t.TempDir()))
	e := makeExecutor(withSandbox(sb))

	step := parser.Step{
		ID:   "fswrite",
		Type: parser.Write,
		Body: "write /out.txt hello",
	}

	_, err := e.Execute(context.Background(), step, nil)
	if !errors.Is(err, sandbox.ErrReadOnly) {
		t.Errorf("expected ErrReadOnly, got %v", err)
	}
}

// --- Test 16: HTTP write step still routes to HTTP (regression) ---

func TestExecutor_HTTP_StillWorks(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "bad method", http.StatusMethodNotAllowed)
			return
		}
		fmt.Fprint(w, "posted")
	}))
	defer srv.Close()

	e := makeExecutor(withClient(srv.Client()))

	step := parser.Step{
		ID:   "httpwrite",
		Type: parser.Write,
		Body: "POST " + srv.URL + "/data",
	}

	res, err := e.Execute(context.Background(), step, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Data != "posted" {
		t.Errorf("expected 'posted', got %v", res.Data)
	}
}

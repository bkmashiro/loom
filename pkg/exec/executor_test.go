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
)

// --- Helpers ---

func makeExecutor(opts ...func(*StepExecutorConfig)) *StepExecutor {
	cfg := StepExecutorConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	return NewStepExecutor(cfg)
}

func withClient(c *http.Client) func(*StepExecutorConfig) {
	return func(cfg *StepExecutorConfig) { cfg.HTTPClient = c }
}

func withTools(r *ToolRegistry) func(*StepExecutorConfig) {
	return func(cfg *StepExecutorConfig) { cfg.Tools = r }
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

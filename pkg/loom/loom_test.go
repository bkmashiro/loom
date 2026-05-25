package loom_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bkmashiro/loom/pkg/loom"
	"github.com/bkmashiro/loom/pkg/sandbox"
)

// buildPlan is a helper to create an io.Reader from a plan string.
func planReader(s string) *strings.Reader {
	return strings.NewReader(s)
}

// TestRun_SingleIOStep: one IO step GETs from a mock server.
func TestRun_SingleIOStep(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hello from server")
	}))
	defer srv.Close()

	plan := fmt.Sprintf("```io fetch\nGET %s\n```\nreturn fetch\n", srv.URL)

	l := loom.New(WithTestHTTPClient(srv))
	result, err := l.Run(context.Background(), planReader(plan))
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.StepID != "fetch" {
		t.Errorf("expected StepID=fetch, got %q", result.StepID)
	}
	body, ok := result.Data.(string)
	if !ok {
		t.Fatalf("expected string data, got %T", result.Data)
	}
	if !strings.Contains(body, "hello from server") {
		t.Errorf("expected body to contain 'hello from server', got %q", body)
	}
}

// WithTestHTTPClient creates a loom.Option using the test server's client.
func WithTestHTTPClient(srv *httptest.Server) loom.Option {
	return loom.WithHTTPClient(srv.Client())
}

// TestRun_ParallelIO_PureMerge: 3 parallel IO steps → 1 pure merge step.
func TestRun_ParallelIO_PureMerge(t *testing.T) {
	startTimes := make([]time.Time, 3)
	var idx int
	mu := make(chan struct{}, 1)
	mu <- struct{}{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-mu
		i := idx
		idx++
		mu <- struct{}{}
		startTimes[i] = time.Now()
		time.Sleep(20 * time.Millisecond)
		fmt.Fprintf(w, "resp%d", i)
	}))
	defer srv.Close()

	plan := fmt.Sprintf(`
`+"```"+`io a
GET %s
`+"```"+`

`+"```"+`io b
GET %s
`+"```"+`

`+"```"+`io c
GET %s
`+"```"+`

`+"```"+`pure(a, b, c) merge
${a} ${b} ${c}
`+"```"+`

return merge
`, srv.URL, srv.URL, srv.URL)

	l := loom.New(WithTestHTTPClient(srv))
	result, err := l.Run(context.Background(), planReader(plan))
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
	if result.StepID != "merge" {
		t.Errorf("expected StepID=merge, got %q", result.StepID)
	}
	// The pure step result should be a string containing interpolated values
	data, ok := result.Data.(string)
	if !ok {
		t.Fatalf("expected string data from pure step, got %T", result.Data)
	}
	if data == "" {
		t.Error("expected non-empty data from pure merge step")
	}
}

// TestRun_AsyncDoesNotBlockReturn: async step sleeps 500ms but return should not wait.
func TestRun_AsyncDoesNotBlockReturn(t *testing.T) {
	// The async server sleeps 500ms
	asyncSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		fmt.Fprint(w, "async done")
	}))
	defer asyncSrv.Close()

	// Fast server returns immediately
	fastSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "fast")
	}))
	defer fastSrv.Close()

	plan := fmt.Sprintf(`
`+"```"+`io fast_step
GET %s
`+"```"+`

`+"```"+`async
GET %s
`+"```"+`

return fast_step
`, fastSrv.URL, asyncSrv.URL)

	// Use a combined client that routes both. For simplicity, each server
	// has its own URL so we use the default client (both are httptest servers
	// on localhost).
	l := loom.New()
	start := time.Now()
	result, err := l.Run(context.Background(), planReader(plan))
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
	if result.StepID != "fast_step" {
		t.Errorf("expected StepID=fast_step, got %q", result.StepID)
	}
	// Should return well before the 500ms async sleep completes
	if elapsed >= 400*time.Millisecond {
		t.Errorf("Run took %v, expected < 400ms (async should not block return)", elapsed)
	}
}

// TestRun_EscapeTool: escape step calls a registered tool.
func TestRun_EscapeTool(t *testing.T) {
	plan := `
` + "```" + `escape my_step
@tool my_tool {"key": "val"}
` + "```" + `

return my_step
`
	l := loom.New()
	l.RegisterTool("my_tool", func(ctx context.Context, args map[string]any) (any, error) {
		return "called", nil
	})

	result, err := l.Run(context.Background(), planReader(plan))
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
	if result.Data != "called" {
		t.Errorf("expected result data 'called', got %v", result.Data)
	}
}

// TestRun_StepFailure_IndependentContinues: step A fails, step B (depends on A)
// is cancelled, but step C (independent) succeeds.
func TestRun_StepFailure_IndependentContinues(t *testing.T) {
	// Server that returns 500 for /fail, 200 for /ok
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fail" {
			w.WriteHeader(500)
			fmt.Fprint(w, "error")
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	// step_a: fails (500 → retryable error, will retry 3 times)
	// step_b: depends on step_a → will be cancelled
	// step_c: independent → should succeed
	plan := fmt.Sprintf(`
`+"```"+`io step_a
GET %s/fail
`+"```"+`

`+"```"+`pure(step_a) step_b
result of a: ${step_a}
`+"```"+`

`+"```"+`io step_c
GET %s/ok
`+"```"+`
`, srv.URL, srv.URL)

	l := loom.New(WithTestHTTPClient(srv))

	var results []loom.StepResult
	ch := l.Stream(context.Background(), planReader(plan))
	for sr := range ch {
		results = append(results, sr)
	}

	// Find step_c result — it should have succeeded
	var foundC bool
	var foundBCancelled bool
	for _, r := range results {
		if r.StepID == "step_c" && r.Err == nil {
			foundC = true
		}
		if r.StepID == "step_b" && r.Err != nil {
			foundBCancelled = true
		}
	}

	if !foundC {
		t.Error("expected step_c to succeed independently")
	}
	if !foundBCancelled {
		t.Error("expected step_b to be cancelled due to upstream failure")
	}
}

// TestStream_EmitsInOrder: 3-step linear plan, all results emitted, channel closes.
func TestStream_EmitsInOrder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "data")
	}))
	defer srv.Close()

	plan := fmt.Sprintf(`
`+"```"+`io step1
GET %s
`+"```"+`

`+"```"+`pure(step1) step2
body after step1
`+"```"+`

`+"```"+`pure(step2) step3
body after step2
`+"```"+`
`, srv.URL)

	l := loom.New(WithTestHTTPClient(srv))
	ch := l.Stream(context.Background(), planReader(plan))

	var results []loom.StepResult
	for sr := range ch {
		results = append(results, sr)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 StepResults, got %d", len(results))
	}

	// Check that all 3 step IDs are present
	stepIDs := map[string]bool{}
	for _, r := range results {
		stepIDs[r.StepID] = true
	}
	for _, id := range []string{"step1", "step2", "step3"} {
		if !stepIDs[id] {
			t.Errorf("missing step %q in stream results", id)
		}
	}
}

// TestRun_EmptyPlan: empty plan returns Result{} with no error.
func TestRun_EmptyPlan(t *testing.T) {
	l := loom.New()
	result, err := l.Run(context.Background(), planReader(""))
	if err != nil {
		t.Fatalf("expected no error for empty plan, got %v", err)
	}
	if result.StepID != "" || result.Data != nil || result.Err != nil {
		t.Errorf("expected zero Result for empty plan, got %+v", result)
	}
}

// TestRun_ReturnDirective: explicit return directive returns the correct step.
func TestRun_ReturnDirective(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "step-data")
	}))
	defer srv.Close()

	plan := fmt.Sprintf(`
`+"```"+`io step_a
GET %s
`+"```"+`

`+"```"+`io step_b
GET %s
`+"```"+`

return step_a
`, srv.URL, srv.URL)

	l := loom.New(WithTestHTTPClient(srv))
	result, err := l.Run(context.Background(), planReader(plan))
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
	if result.StepID != "step_a" {
		t.Errorf("expected return StepID=step_a, got %q", result.StepID)
	}
}

// TestRun_VariableInterpolation: IO step result is interpolated into pure step body.
func TestRun_VariableInterpolation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "hello")
	}))
	defer srv.Close()

	// The pure step body uses ${io_step}. Since pure with no lang returns body as-is
	// (after interpolation), the result should contain the JSON-encoded "hello".
	// io_step result is string "hello" → JSON encoded as "hello" (with quotes).
	plan := fmt.Sprintf(`
`+"```"+`io io_step
GET %s
`+"```"+`

`+"```"+`pure(io_step) pure_step
prefix:${io_step}:suffix
`+"```"+`

return pure_step
`, srv.URL)

	l := loom.New(WithTestHTTPClient(srv))
	result, err := l.Run(context.Background(), planReader(plan))
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}

	body, ok := result.Data.(string)
	if !ok {
		t.Fatalf("expected string data from pure step, got %T: %v", result.Data, result.Data)
	}

	// The interpolated body: "hello" is JSON-encoded as `"hello"` (with quotes)
	if !strings.Contains(body, "hello") {
		t.Errorf("expected interpolated body to contain 'hello', got %q", body)
	}
}

// TestRegisterTool_BeforeRun: register a tool, then run a plan using it.
func TestRegisterTool_BeforeRun(t *testing.T) {
	called := false

	plan := `
` + "```" + `escape tool_step
@tool capture_tool {"x": 1}
` + "```" + `

return tool_step
`

	l := loom.New()
	l.RegisterTool("capture_tool", func(ctx context.Context, args map[string]any) (any, error) {
		called = true
		return fmt.Sprintf("x=%v", args["x"]), nil
	})

	result, err := l.Run(context.Background(), planReader(plan))
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
	if !called {
		t.Error("expected tool to be called")
	}
	if result.Data != "x=1" {
		t.Errorf("expected result 'x=1', got %v", result.Data)
	}
}

// TestLoom_WithSandbox_EphemeralRoundTrip: write a file then read it back via sandbox.
func TestLoom_WithSandbox_EphemeralRoundTrip(t *testing.T) {
	// Plan: write step writes a file; io step (disambiguated to FS) reads it back.
	// We need to read from the same sandbox instance, so we'll write and then verify
	// using the executor — but through loom we cannot directly inspect the sandbox.
	// Instead, run a plan where a write step writes and a separate write step reads.
	// Since FS read returns []byte but Loom's return expects the last step result,
	// we run the write, then read back and return.
	plan := `
` + "```" + `write writeStep
write /hello.txt world
` + "```" + `

` + "```" + `write(writeStep) readStep
read /hello.txt
` + "```" + `

return readStep
`

	l := loom.New(loom.WithSandbox(sandbox.EphemeralSandbox()))
	result, err := l.Run(context.Background(), planReader(plan))
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
	if result.StepID != "readStep" {
		t.Errorf("expected StepID=readStep, got %q", result.StepID)
	}
	data, ok := result.Data.([]byte)
	if !ok {
		t.Fatalf("expected []byte from read step, got %T: %v", result.Data, result.Data)
	}
	if string(data) != "world" {
		t.Errorf("expected 'world', got %q", string(data))
	}
}

// TestLoom_NoSandbox_Default: running without WithSandbox executes normally without errors.
func TestLoom_NoSandbox_Default(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	plan := fmt.Sprintf(`
`+"```"+`io fetchStep
GET %s
`+"```"+`

return fetchStep
`, srv.URL)

	l := loom.New(WithTestHTTPClient(srv)) // no WithSandbox
	result, err := l.Run(context.Background(), planReader(plan))
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}
	if result.Data != "ok" {
		t.Errorf("expected 'ok', got %v", result.Data)
	}
}

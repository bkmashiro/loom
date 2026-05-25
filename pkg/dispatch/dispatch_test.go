package dispatch_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bkmashiro/loom/pkg/dag"
	"github.com/bkmashiro/loom/pkg/dispatch"
	"github.com/bkmashiro/loom/pkg/parser"
)

// mockExecutor is a simple dag.Executor for testing.
type mockExecutor struct {
	fn func(ctx context.Context, step parser.Step, inputs map[string]dag.Result) (dag.Result, error)
}

func (m *mockExecutor) Execute(ctx context.Context, step parser.Step, inputs map[string]dag.Result) (dag.Result, error) {
	return m.fn(ctx, step, inputs)
}

// fixedResult returns a dag.Executor that always returns the given data.
func fixedResult(data any) *mockExecutor {
	return &mockExecutor{fn: func(_ context.Context, step parser.Step, _ map[string]dag.Result) (dag.Result, error) {
		return dag.Result{StepID: step.ID, Data: data}, nil
	}}
}

// TestDispatcher_LocalWorker — dispatcher with one local worker; routes step; result returned.
func TestDispatcher_LocalWorker(t *testing.T) {
	exec := fixedResult("hello")
	w := dispatch.DefaultLocalWorker(exec)

	d := dispatch.New()
	d.Register(w)

	step := parser.Step{ID: "s1", Type: parser.IO}
	result, err := d.Execute(context.Background(), step, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Data != "hello" {
		t.Errorf("expected 'hello', got %v", result.Data)
	}
	if result.StepID != "s1" {
		t.Errorf("expected StepID 's1', got %q", result.StepID)
	}
}

// TestDispatcher_NoWorker — dispatcher with no workers; returns ErrNoWorker.
func TestDispatcher_NoWorker(t *testing.T) {
	d := dispatch.New()
	step := parser.Step{ID: "s1", Type: parser.IO}
	_, err := d.Execute(context.Background(), step, nil)
	if !errors.Is(err, dispatch.ErrNoWorker) {
		t.Errorf("expected ErrNoWorker, got %v", err)
	}
}

// TestDispatcher_TypeFilter — two workers: one handles IO only, one handles Pure only.
func TestDispatcher_TypeFilter(t *testing.T) {
	ioExec := fixedResult("io-result")
	pureExec := fixedResult("pure-result")

	ioWorker := dispatch.NewLocalWorker(ioExec, dispatch.Capability{
		Types: []parser.StepType{parser.IO},
	})
	pureWorker := dispatch.NewLocalWorker(pureExec, dispatch.Capability{
		Types: []parser.StepType{parser.Pure},
	})

	d := dispatch.New()
	d.Register(ioWorker)
	d.Register(pureWorker)

	// IO step should go to ioWorker.
	ioStep := parser.Step{ID: "io1", Type: parser.IO}
	res, err := d.Execute(context.Background(), ioStep, nil)
	if err != nil {
		t.Fatalf("io step: unexpected error: %v", err)
	}
	if res.Data != "io-result" {
		t.Errorf("io step: expected 'io-result', got %v", res.Data)
	}

	// Pure step should go to pureWorker.
	pureStep := parser.Step{ID: "pure1", Type: parser.Pure}
	res, err = d.Execute(context.Background(), pureStep, nil)
	if err != nil {
		t.Fatalf("pure step: unexpected error: %v", err)
	}
	if res.Data != "pure-result" {
		t.Errorf("pure step: expected 'pure-result', got %v", res.Data)
	}
}

// TestDispatcher_LangFilter — worker with Langs: ["torch"]; pure.torch routes to it; pure.python falls through to fallback.
func TestDispatcher_LangFilter(t *testing.T) {
	torchExec := fixedResult("torch-result")
	fallbackExec := fixedResult("fallback-result")

	torchWorker := dispatch.NewLocalWorker(torchExec, dispatch.Capability{
		Types: []parser.StepType{parser.Pure},
		Langs: []string{"torch"},
	})
	fallbackWorker := dispatch.DefaultLocalWorker(fallbackExec)

	d := dispatch.New()
	d.Register(torchWorker)
	d.Register(fallbackWorker)

	// pure.torch should go to torchWorker.
	torchStep := parser.Step{ID: "t1", Type: parser.Pure, Lang: "torch"}
	res, err := d.Execute(context.Background(), torchStep, nil)
	if err != nil {
		t.Fatalf("torch step: unexpected error: %v", err)
	}
	if res.Data != "torch-result" {
		t.Errorf("torch step: expected 'torch-result', got %v", res.Data)
	}

	// pure.python should fall through to fallbackWorker (torchWorker won't match).
	pythonStep := parser.Step{ID: "p1", Type: parser.Pure, Lang: "python"}
	res, err = d.Execute(context.Background(), pythonStep, nil)
	if err != nil {
		t.Fatalf("python step: unexpected error: %v", err)
	}
	if res.Data != "fallback-result" {
		t.Errorf("python step: expected 'fallback-result', got %v", res.Data)
	}
}

// TestDispatcher_RemoteWorker — use httptest.NewServer as a fake remote;
// marshal a step, verify request received and result returned.
func TestDispatcher_RemoteWorker(t *testing.T) {
	var receivedStepID string
	var receivedBody string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/execute" || r.Method != http.MethodPost {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		var req struct {
			Step struct {
				ID   string `json:"id"`
				Body string `json:"body"`
			} `json:"step"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		receivedStepID = req.Step.ID
		receivedBody = req.Step.Body

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"step_id": req.Step.ID,
			"data":    "remote-data",
			"err":     nil,
		})
	}))
	defer ts.Close()

	rw := dispatch.NewRemoteWorker(ts.URL, dispatch.Capability{})
	d := dispatch.New()
	d.Register(rw)

	step := parser.Step{ID: "fetch1", Type: parser.IO, Deps: []string{}, Body: "GET /api/test", Lang: ""}
	res, err := d.Execute(context.Background(), step, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.StepID != "fetch1" {
		t.Errorf("expected StepID 'fetch1', got %q", res.StepID)
	}
	if receivedStepID != "fetch1" {
		t.Errorf("server received step ID %q, want 'fetch1'", receivedStepID)
	}
	if receivedBody != "GET /api/test" {
		t.Errorf("server received body %q, want 'GET /api/test'", receivedBody)
	}
}

// TestServer_Execute — create dispatch.NewServer(mockExec); POST /execute with a step; verify response.
func TestServer_Execute(t *testing.T) {
	exec := fixedResult("server-result")
	srv := dispatch.NewServer(exec)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	reqPayload, _ := json.Marshal(map[string]any{
		"step": map[string]any{
			"id":   "step1",
			"type": int(parser.Pure),
			"deps": []string{},
			"body": "x + 1",
			"lang": "python",
		},
		"inputs": map[string]any{},
	})

	resp, err := http.Post(ts.URL+"/execute", "application/json", bytes.NewReader(reqPayload))
	if err != nil {
		t.Fatalf("POST /execute failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		StepID string          `json:"step_id"`
		Data   json.RawMessage `json:"data"`
		Err    *string         `json:"err"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if result.StepID != "step1" {
		t.Errorf("expected StepID 'step1', got %q", result.StepID)
	}
	if result.Err != nil {
		t.Errorf("expected no error, got %q", *result.Err)
	}
	// Data should be the JSON-marshalled "server-result".
	var dataStr string
	if err := json.Unmarshal(result.Data, &dataStr); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	if dataStr != "server-result" {
		t.Errorf("expected data 'server-result', got %q", dataStr)
	}
}

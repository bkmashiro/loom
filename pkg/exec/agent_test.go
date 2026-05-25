package exec

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bkmashiro/loom/pkg/dag"
	"github.com/bkmashiro/loom/pkg/parser"
)

// makeAgentResponse builds a minimal chat completion JSON response.
func makeAgentResponse(content string) string {
	resp := map[string]any{
		"choices": []map[string]any{
			{
				"message": map[string]any{
					"content": content,
				},
			},
		},
	}
	b, _ := json.Marshal(resp)
	return string(b)
}

func withAgentUpstream(url, key, model string) func(*StepExecutorConfig) {
	return func(cfg *StepExecutorConfig) {
		cfg.AgentUpstream = url
		cfg.AgentAPIKey = key
		cfg.AgentModel = model
	}
}

// --- Test 1: Basic agent call ---

func TestExecuteAgent_Basic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, makeAgentResponse("Legal risk: high"))
	}))
	defer srv.Close()

	e := makeExecutor(
		withClient(srv.Client()),
		withAgentUpstream(srv.URL, "test-key", "gpt-4o"),
	)

	step := parser.Step{
		ID:   "agent1",
		Type: parser.Agent,
		Body: "task: Review the following for legal risks",
	}

	res, err := e.Execute(context.Background(), step, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Data != "Legal risk: high" {
		t.Errorf("expected 'Legal risk: high', got %v", res.Data)
	}
}

// --- Test 2: Dependency substitution ---

func TestExecuteAgent_DepSubstitution(t *testing.T) {
	var receivedBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, makeAgentResponse("ok"))
	}))
	defer srv.Close()

	e := makeExecutor(
		withClient(srv.Client()),
		withAgentUpstream(srv.URL, "", "gpt-4o"),
	)

	step := parser.Step{
		ID:   "agent2",
		Type: parser.Agent,
		Body: "task: Review this: ${briefing}",
	}

	inputs := map[string]dag.Result{
		"briefing": {StepID: "briefing", Data: "contract text"},
	}

	_, err := e.Execute(context.Background(), step, inputs)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Parse received body to verify the task was substituted.
	var req agentRequest
	if err := json.Unmarshal(receivedBody, &req); err != nil {
		t.Fatalf("unmarshal received body: %v", err)
	}

	var userContent string
	for _, msg := range req.Messages {
		if msg.Role == "user" {
			userContent = msg.Content
		}
	}
	if !strings.Contains(userContent, "contract text") {
		t.Errorf("expected task to contain 'contract text', got %q", userContent)
	}
}

// --- Test 3: Model override ---

func TestExecuteAgent_ModelOverride(t *testing.T) {
	var receivedBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, makeAgentResponse("done"))
	}))
	defer srv.Close()

	e := makeExecutor(
		withClient(srv.Client()),
		withAgentUpstream(srv.URL, "", "gpt-4o"),
	)

	step := parser.Step{
		ID:   "agent3",
		Type: parser.Agent,
		Body: "model: claude-3-5\ntask: Do something",
	}

	_, err := e.Execute(context.Background(), step, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	var req agentRequest
	if err := json.Unmarshal(receivedBody, &req); err != nil {
		t.Fatalf("unmarshal received body: %v", err)
	}
	if req.Model != "claude-3-5" {
		t.Errorf("expected model 'claude-3-5', got %q", req.Model)
	}
}

// --- Test 4: No upstream configured ---

func TestExecuteAgent_NoUpstream(t *testing.T) {
	e := makeExecutor() // no agent upstream

	step := parser.Step{
		ID:   "agent4",
		Type: parser.Agent,
		Body: "task: Do something",
	}

	_, err := e.Execute(context.Background(), step, nil)
	if !errors.Is(err, ErrNoAgentUpstream) {
		t.Errorf("expected ErrNoAgentUpstream, got %v", err)
	}
}

// --- Test 5: System prompt ---

func TestExecuteAgent_SystemPrompt(t *testing.T) {
	var receivedBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, makeAgentResponse("answer"))
	}))
	defer srv.Close()

	e := makeExecutor(
		withClient(srv.Client()),
		withAgentUpstream(srv.URL, "key", "gpt-4o"),
	)

	step := parser.Step{
		ID:   "agent5",
		Type: parser.Agent,
		Body: "system: You are a legal expert\ntask: Review this contract",
	}

	_, err := e.Execute(context.Background(), step, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	var req agentRequest
	if err := json.Unmarshal(receivedBody, &req); err != nil {
		t.Fatalf("unmarshal received body: %v", err)
	}

	var hasSystem bool
	for _, msg := range req.Messages {
		if msg.Role == "system" && msg.Content == "You are a legal expert" {
			hasSystem = true
		}
	}
	if !hasSystem {
		t.Errorf("expected system message 'You are a legal expert' in messages, got %v", req.Messages)
	}
}

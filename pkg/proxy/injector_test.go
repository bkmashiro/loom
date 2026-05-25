package proxy

import (
	"errors"
	"strings"
	"testing"

	"github.com/bkmashiro/loom/pkg/dag"
)

// TestFormatResults_Success — successful steps render with status="ok".
func TestFormatResults_Success(t *testing.T) {
	results := []dag.StepResult{
		{Result: dag.Result{StepID: "fetch_user", Data: map[string]string{"name": "Alice"}}},
		{Result: dag.Result{StepID: "fetch_posts", Data: []string{"Hello World"}}},
	}
	got := FormatResults(results)

	if !strings.Contains(got, `<step id="fetch_user" status="ok">`) {
		t.Errorf("missing fetch_user ok tag: %s", got)
	}
	if !strings.Contains(got, `<step id="fetch_posts" status="ok">`) {
		t.Errorf("missing fetch_posts ok tag: %s", got)
	}
	if !strings.Contains(got, `Alice`) {
		t.Errorf("missing Alice in results: %s", got)
	}
	if !strings.Contains(got, `</step>`) {
		t.Errorf("missing closing step tag: %s", got)
	}
}

// TestFormatResults_WithError — error steps render with status="error".
func TestFormatResults_WithError(t *testing.T) {
	results := []dag.StepResult{
		{Result: dag.Result{StepID: "fetch_user", Data: map[string]string{"name": "Alice"}}},
		{Result: dag.Result{StepID: "fetch_posts", Err: errors.New("connection refused")}},
	}
	got := FormatResults(results)

	if !strings.Contains(got, `<step id="fetch_user" status="ok">`) {
		t.Errorf("missing fetch_user ok tag: %s", got)
	}
	if !strings.Contains(got, `<step id="fetch_posts" status="error">`) {
		t.Errorf("missing fetch_posts error tag: %s", got)
	}
	if !strings.Contains(got, "connection refused") {
		t.Errorf("missing error message: %s", got)
	}
}

// TestInjectResults_ToolRole_WithAssistantMessage — replaces existing assistant message,
// inserts tool message with tool_call_id.
func TestInjectResults_ToolRole_WithAssistantMessage(t *testing.T) {
	messages := []Message{
		{Role: "user", Content: "What are my posts?"},
		{Role: "assistant", Content: "Let me check..."},  // this should be replaced
		{Role: "user", Content: "follow-up question"},
	}
	results := []dag.StepResult{
		{Result: dag.Result{StepID: "fetch_posts", Data: []string{"Post 1", "Post 2"}}},
	}
	fullAssistant := "Let me check...\n```io fetch_posts\nGET /posts\n```\n"

	out := InjectResults(messages, fullAssistant, results, "tool")

	// Should be: user, assistant(full), tool, user
	if len(out) != 4 {
		t.Fatalf("expected 4 messages, got %d: %+v", len(out), out)
	}
	if out[0].Role != "user" || out[0].Content != "What are my posts?" {
		t.Errorf("message[0] unexpected: %+v", out[0])
	}
	if out[1].Role != "assistant" {
		t.Errorf("message[1] should be assistant, got %s", out[1].Role)
	}
	if out[1].Content != fullAssistant {
		t.Errorf("message[1] content should be fullAssistant, got %q", out[1].Content)
	}
	if len(out[1].ToolCalls) != 1 {
		t.Errorf("message[1] should have tool_calls, got %d", len(out[1].ToolCalls))
	}
	if out[2].Role != "tool" {
		t.Errorf("message[2] should be tool, got %s", out[2].Role)
	}
	if out[2].ToolCallID != loomToolCallID {
		t.Errorf("message[2] tool_call_id should be %s, got %s", loomToolCallID, out[2].ToolCallID)
	}
	if !strings.Contains(out[2].Content, "fetch_posts") {
		t.Errorf("message[2] should contain step results, got %q", out[2].Content)
	}
	if out[3].Role != "user" || out[3].Content != "follow-up question" {
		t.Errorf("message[3] unexpected: %+v", out[3])
	}
}

// TestInjectResults_UserRole — injection role "user" uses user role instead of tool.
func TestInjectResults_UserRole(t *testing.T) {
	messages := []Message{
		{Role: "user", Content: "hi"},
		{Role: "assistant", Content: "plan text"},
		{Role: "user", Content: "what happened?"},
	}
	results := []dag.StepResult{
		{Result: dag.Result{StepID: "step1", Data: "done"}},
	}

	out := InjectResults(messages, "full plan text", results, "user")

	// Should have: user, assistant(no tool_calls), user(results), user
	if len(out) != 4 {
		t.Fatalf("expected 4 messages, got %d", len(out))
	}
	if out[1].Role != "assistant" {
		t.Errorf("message[1] should be assistant, got %s", out[1].Role)
	}
	if len(out[1].ToolCalls) > 0 {
		t.Error("user role injection should not add tool_calls to assistant message")
	}
	if out[2].Role != "user" {
		t.Errorf("message[2] should be user (fallback), got %s", out[2].Role)
	}
	if !strings.Contains(out[2].Content, "[System: Loom execution results]") {
		t.Errorf("user role results should have system prefix: %q", out[2].Content)
	}
}

// TestInjectResults_NoAssistantBefore — no assistant message before last user: inserts both.
func TestInjectResults_NoAssistantBefore(t *testing.T) {
	messages := []Message{
		{Role: "user", Content: "original"},
		{Role: "user", Content: "follow-up"},
	}
	results := []dag.StepResult{
		{Result: dag.Result{StepID: "step1", Data: "result"}},
	}

	out := InjectResults(messages, "assistant text", results, "tool")

	// user, user → user, assistant, tool, user
	if len(out) != 4 {
		t.Fatalf("expected 4 messages, got %d: %+v", len(out), out)
	}
	if out[0].Role != "user" {
		t.Errorf("message[0] should be user, got %s", out[0].Role)
	}
	if out[1].Role != "assistant" {
		t.Errorf("message[1] should be assistant, got %s", out[1].Role)
	}
	if out[2].Role != "tool" {
		t.Errorf("message[2] should be tool, got %s", out[2].Role)
	}
	if out[3].Role != "user" || out[3].Content != "follow-up" {
		t.Errorf("message[3] should be follow-up user, got %+v", out[3])
	}
}

// TestInjectResults_NoUserMessage — returns messages unchanged if no user message found.
func TestInjectResults_NoUserMessage(t *testing.T) {
	messages := []Message{
		{Role: "system", Content: "You are helpful."},
	}
	results := []dag.StepResult{}

	out := InjectResults(messages, "text", results, "tool")

	if len(out) != 1 {
		t.Errorf("expected messages unchanged, got %d messages", len(out))
	}
}

// TestInjectResults_EmptyAssistantMessage — empty lastAssistantMsg still inserts the message.
func TestInjectResults_EmptyAssistantMessage(t *testing.T) {
	messages := []Message{
		{Role: "user", Content: "hi"},
		{Role: "user", Content: "follow-up"},
	}
	results := []dag.StepResult{
		{Result: dag.Result{StepID: "s", Data: nil}},
	}

	out := InjectResults(messages, "", results, "tool")

	// Should still inject (empty assistant message is valid).
	if len(out) != 4 {
		t.Fatalf("expected 4 messages, got %d", len(out))
	}
	if out[1].Content != "" {
		t.Errorf("expected empty assistant content, got %q", out[1].Content)
	}
}

// TestInjectResults_PreservesFirstMessages — earlier messages are untouched.
func TestInjectResults_PreservesFirstMessages(t *testing.T) {
	messages := []Message{
		{Role: "system", Content: "sys"},
		{Role: "user", Content: "q1"},
		{Role: "assistant", Content: "a1"},
		{Role: "user", Content: "q2"},
		{Role: "assistant", Content: "a2 with plan"},
		{Role: "user", Content: "q3"},
	}
	results := []dag.StepResult{
		{Result: dag.Result{StepID: "s", Data: "x"}},
	}

	out := InjectResults(messages, "full a2", results, "tool")

	// Expected: sys, user(q1), assistant(a1), user(q2), assistant(full a2 with tools), tool, user(q3)
	if len(out) != 7 {
		t.Fatalf("expected 7 messages, got %d: %+v", len(out), out)
	}
	if out[0].Content != "sys" {
		t.Errorf("system message altered: %q", out[0].Content)
	}
	if out[1].Content != "q1" {
		t.Errorf("first user message altered: %q", out[1].Content)
	}
	if out[2].Content != "a1" {
		t.Errorf("first assistant message altered: %q", out[2].Content)
	}
	if out[3].Content != "q2" {
		t.Errorf("second user message altered: %q", out[3].Content)
	}
	if out[4].Content != "full a2" {
		t.Errorf("replaced assistant message wrong: %q", out[4].Content)
	}
	if out[6].Content != "q3" {
		t.Errorf("final user message wrong: %q", out[6].Content)
	}
}

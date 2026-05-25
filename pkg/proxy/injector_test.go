package proxy

import (
	"errors"
	"strings"
	"testing"

	"github.com/bkmashiro/loom/pkg/dag"
)

func TestInjector_FormatResults_Success(t *testing.T) {
	ri := &ResultInjector{
		Results: []dag.StepResult{
			{Result: dag.Result{StepID: "fetch_user", Data: map[string]string{"name": "Alice"}}},
			{Result: dag.Result{StepID: "fetch_posts", Data: []string{"Hello World"}}},
		},
	}
	got := ri.FormatResults()

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
	if !strings.Contains(got, "<results>") || !strings.Contains(got, "</results>") {
		t.Errorf("missing results wrapper: %s", got)
	}
}

func TestInjector_FormatResults_WithError(t *testing.T) {
	ri := &ResultInjector{
		Results: []dag.StepResult{
			{Result: dag.Result{StepID: "fetch_user", Data: map[string]string{"name": "Alice"}}},
			{Result: dag.Result{StepID: "fetch_posts", Err: errors.New("connection refused")}},
		},
	}
	got := ri.FormatResults()

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

func TestInjector_BuildRequest_NoPrePlanText(t *testing.T) {
	ri := &ResultInjector{
		OriginalMessages: []Message{
			{Role: "user", Content: "What is 2+2?"},
		},
		PrePlanText: "",
		Results:     []dag.StepResult{},
		Model:       "gpt-4",
	}
	req := ri.BuildRequest()

	// original + user results message = 2 total
	if len(req.Messages) != 2 {
		t.Errorf("expected 2 messages, got %d", len(req.Messages))
	}
	if req.Messages[0].Role != "user" {
		t.Errorf("first message should be user, got %s", req.Messages[0].Role)
	}
	if req.Messages[1].Role != "user" {
		t.Errorf("second message should be user (results), got %s", req.Messages[1].Role)
	}
	if req.Model != "gpt-4" {
		t.Errorf("unexpected model: %s", req.Model)
	}
	if !req.Stream {
		t.Errorf("expected stream:true")
	}
}

func TestInjector_BuildRequest_WithPrePlanText(t *testing.T) {
	ri := &ResultInjector{
		OriginalMessages: []Message{
			{Role: "user", Content: "What are my posts?"},
		},
		PrePlanText: "Let me look that up for you.",
		Results:     []dag.StepResult{},
		Model:       "gpt-3.5-turbo",
	}
	req := ri.BuildRequest()

	// original + assistant (pre-plan) + user results = 3 total
	if len(req.Messages) != 3 {
		t.Errorf("expected 3 messages, got %d", len(req.Messages))
	}
	if req.Messages[1].Role != "assistant" {
		t.Errorf("second message should be assistant, got %s", req.Messages[1].Role)
	}
	if req.Messages[1].Content != "Let me look that up for you." {
		t.Errorf("pre-plan text not preserved: %s", req.Messages[1].Content)
	}
	if req.Messages[2].Role != "user" {
		t.Errorf("third message should be user, got %s", req.Messages[2].Role)
	}
}

func TestInjector_BuildRequest_SuppressInstruction(t *testing.T) {
	ri := &ResultInjector{
		OriginalMessages: []Message{{Role: "user", Content: "hi"}},
		Results:          []dag.StepResult{},
		Model:            "gpt-4",
	}
	req := ri.BuildRequest()

	lastMsg := req.Messages[len(req.Messages)-1]
	if !strings.Contains(lastMsg.Content, "Do not output any Loom") {
		t.Errorf("suppression instruction missing from last message: %s", lastMsg.Content)
	}
}

package parser

import (
	"io"
	"strings"
	"testing"
	"time"
)

// helper to collect all events from a string input
func collectEvents(input string) ([]Event, error) {
	p := NewParser(strings.NewReader(input))
	var events []Event
	for ev := range p.Events() {
		events = append(events, ev)
	}
	return events, p.Err()
}

// Test 1: Parse single IO step → correct Step emitted
func TestSingleIOStep(t *testing.T) {
	input := "```io fetch_user\nGET /api/users/1\n```\n"
	events, err := collectEvents(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	step := events[0].Step
	if step == nil {
		t.Fatal("expected Step event, got nil")
	}
	if step.ID != "fetch_user" {
		t.Errorf("expected ID=fetch_user, got %q", step.ID)
	}
	if step.Type != IO {
		t.Errorf("expected Type=IO, got %v", step.Type)
	}
	if step.Body != "GET /api/users/1" {
		t.Errorf("expected body %q, got %q", "GET /api/users/1", step.Body)
	}
	if len(step.Deps) != 0 {
		t.Errorf("expected no deps, got %v", step.Deps)
	}
}

// Test 2: Parse multiple steps with different dep syntaxes → correct dep lists
func TestMultipleStepsDepSyntaxes(t *testing.T) {
	input := `
` + "```" + `io fetch_user
GET /api/users
` + "```" + `
` + "```" + `io fetch_posts(auth_token)
GET /api/posts
` + "```" + `
` + "```" + `pure(fetch_user, fetch_posts) build_feed
result = merge(fetch_user, fetch_posts)
` + "```" + `
`
	events, err := collectEvents(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}

	// fetch_user: no deps
	s0 := events[0].Step
	if s0.ID != "fetch_user" || s0.Type != IO || len(s0.Deps) != 0 {
		t.Errorf("step0 mismatch: %+v", s0)
	}

	// fetch_posts: deps=[auth_token], id via suffix syntax
	s1 := events[1].Step
	if s1.ID != "fetch_posts" || s1.Type != IO {
		t.Errorf("step1 mismatch: %+v", s1)
	}
	if len(s1.Deps) != 1 || s1.Deps[0] != "auth_token" {
		t.Errorf("step1 deps mismatch: %v", s1.Deps)
	}

	// build_feed: deps=[fetch_user, fetch_posts], deps-first syntax
	s2 := events[2].Step
	if s2.ID != "build_feed" || s2.Type != Pure {
		t.Errorf("step2 mismatch: %+v", s2)
	}
	if len(s2.Deps) != 2 || s2.Deps[0] != "fetch_user" || s2.Deps[1] != "fetch_posts" {
		t.Errorf("step2 deps mismatch: %v", s2.Deps)
	}
}

// Test 3: Parse all 6 step types
func TestAllStepTypes(t *testing.T) {
	input := "```io step_io\nbody\n```\n" +
		"```write step_write\nbody\n```\n" +
		"```pure step_pure\nbody\n```\n" +
		"```shell step_shell\nbody\n```\n" +
		"```async step_async\nbody\n```\n" +
		"```escape step_escape\nbody\n```\n"

	events, err := collectEvents(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 6 {
		t.Fatalf("expected 6 events, got %d", len(events))
	}

	expected := []struct {
		id  string
		typ StepType
	}{
		{"step_io", IO},
		{"step_write", Write},
		{"step_pure", Pure},
		{"step_shell", Shell},
		{"step_async", Async},
		{"step_escape", Escape},
	}

	for i, e := range expected {
		s := events[i].Step
		if s == nil {
			t.Fatalf("event[%d] is not a Step event", i)
		}
		if s.ID != e.id {
			t.Errorf("event[%d]: expected ID=%q, got %q", i, e.id, s.ID)
		}
		if s.Type != e.typ {
			t.Errorf("event[%d]: expected Type=%v, got %v", i, e.typ, s.Type)
		}
	}
}

// Test 4: Language suffix parsing (`pure.python`) → Step.Lang == "python"
func TestLanguageSuffix(t *testing.T) {
	input := "```pure.python transform\nimport json\nreturn json.loads(a)\n```\n"
	events, err := collectEvents(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	s := events[0].Step
	if s.Lang != "python" {
		t.Errorf("expected Lang=python, got %q", s.Lang)
	}
	if s.Type != Pure {
		t.Errorf("expected Type=Pure, got %v", s.Type)
	}
	if s.ID != "transform" {
		t.Errorf("expected ID=transform, got %q", s.ID)
	}
}

// Test 5: Malformed fence header → fence skipped, parsing continues
func TestMalformedFenceSkipped(t *testing.T) {
	input := "```unknown_type foo\nbad body\n```\n" +
		"```io good_step\nGET /ok\n```\n"
	events, err := collectEvents(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Only the good step should be emitted
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	s := events[0].Step
	if s.ID != "good_step" {
		t.Errorf("expected good_step, got %q", s.ID)
	}
}

// Test 6: Unclosed fence at EOF → Err() returns non-nil
func TestUnclosedFenceAtEOF(t *testing.T) {
	input := "```io unclosed_step\nGET /api/users\n"
	events, err := collectEvents(input)
	if err == nil {
		t.Fatal("expected error for unclosed fence, got nil")
	}
	// No events should be emitted for the partial step
	for _, ev := range events {
		if ev.Step != nil && ev.Step.ID == "unclosed_step" {
			t.Error("partial step should not be emitted")
		}
	}
}

// Test 7: Return directive → ReturnDirective emitted with correct StepID
func TestReturnDirective(t *testing.T) {
	input := "```io fetch_user\nGET /api/users\n```\nreturn fetch_user\n"
	events, err := collectEvents(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].Step == nil {
		t.Fatal("first event should be a Step")
	}
	ret := events[1].Return
	if ret == nil {
		t.Fatal("second event should be a ReturnDirective")
	}
	if ret.StepID != "fetch_user" {
		t.Errorf("expected StepID=fetch_user, got %q", ret.StepID)
	}
}

// Test 8: Mixed text and fences → text ignored, fences parsed correctly
func TestMixedTextAndFences(t *testing.T) {
	input := "This is some plain text that should be ignored.\n" +
		"More text here.\n" +
		"```io fetch_data\nGET /api/data\n```\n" +
		"Some text between fences.\n" +
		"```pure process\nresult = transform(fetch_data)\n```\n" +
		"Final text.\n"

	events, err := collectEvents(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].Step.ID != "fetch_data" {
		t.Errorf("expected fetch_data, got %q", events[0].Step.ID)
	}
	if events[1].Step.ID != "process" {
		t.Errorf("expected process, got %q", events[1].Step.ID)
	}
}

// Test 9: Streaming behavior — steps emitted before reader exhausted
func TestStreamingBehavior(t *testing.T) {
	pr, pw := io.Pipe()
	p := NewParser(pr)

	// Write first fence
	firstStep := make(chan struct{})
	go func() {
		pw.Write([]byte("```io first_step\nGET /api/first\n```\n"))
		// Signal that first fence is written, then wait before writing second
		close(firstStep)
		time.Sleep(50 * time.Millisecond)
		pw.Write([]byte("```io second_step\nGET /api/second\n```\n"))
		pw.Close()
	}()

	// Wait for first step to be available
	<-firstStep
	// Give parser a moment to process
	time.Sleep(20 * time.Millisecond)

	// Try to receive first event without waiting for reader to close
	select {
	case ev, ok := <-p.Events():
		if !ok {
			t.Fatal("events channel closed too early")
		}
		if ev.Step == nil || ev.Step.ID != "first_step" {
			t.Errorf("expected first_step, got %+v", ev)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timeout waiting for first step — streaming not working")
	}

	// Drain remaining events
	for range p.Events() {
	}
	if err := p.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Test 10: Auto-generated ID for async steps without explicit ID
func TestAsyncAutoID(t *testing.T) {
	input := "```async\nPOST /api/log\n```\n" +
		"```async\nPOST /api/metrics\n```\n" +
		"```async named_async\nPOST /api/named\n```\n"

	events, err := collectEvents(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}

	// First unnamed async should get _async_0
	s0 := events[0].Step
	if s0.ID != "_async_0" {
		t.Errorf("expected _async_0, got %q", s0.ID)
	}
	// Second unnamed async should get _async_1
	s1 := events[1].Step
	if s1.ID != "_async_1" {
		t.Errorf("expected _async_1, got %q", s1.ID)
	}
	// Named async keeps its name
	s2 := events[2].Step
	if s2.ID != "named_async" {
		t.Errorf("expected named_async, got %q", s2.ID)
	}
}

// Test: full example from spec
func TestFullExample(t *testing.T) {
	input := "```io fetch_user\nGET /api/users/${user_id}\n```\n\n" +
		"```io fetch_posts(auth_token)\nGET /api/posts\n```\n\n" +
		"```pure(fetch_user, fetch_posts) build_feed\nresult = merge(fetch_user, fetch_posts)\nreturn result\n```\n\n" +
		"```pure.python(a, b) transform\nimport json\nreturn json.loads(a) + json.loads(b)\n```\n\n" +
		"```async\nPOST /api/analytics {\"event\": \"view\"}\n```\n\n" +
		"```async log_event\nPOST /api/log {\"event\": \"done\"}\n```\n\n" +
		"```escape(input) browser_shot\n@tool browser_screenshot {\"url\": \"https://example.com\"}\n```\n\n" +
		"return build_feed\n"

	events, err := collectEvents(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 7 fences + 1 return = 8 events
	if len(events) != 8 {
		t.Fatalf("expected 8 events, got %d", len(events))
	}

	// Verify return directive is last
	ret := events[7].Return
	if ret == nil {
		t.Fatal("last event should be return directive")
	}
	if ret.StepID != "build_feed" {
		t.Errorf("expected build_feed, got %q", ret.StepID)
	}

	// Verify async auto-id
	asyncStep := events[4].Step
	if asyncStep.ID != "_async_0" {
		t.Errorf("expected _async_0, got %q", asyncStep.ID)
	}

	// Verify pure.python lang
	pyStep := events[3].Step
	if pyStep.Lang != "python" {
		t.Errorf("expected python, got %q", pyStep.Lang)
	}
	if len(pyStep.Deps) != 2 {
		t.Errorf("expected 2 deps, got %v", pyStep.Deps)
	}

	// Verify escape step
	escStep := events[6].Step
	if escStep.Type != Escape {
		t.Errorf("expected Escape type")
	}
	if escStep.ID != "browser_shot" {
		t.Errorf("expected browser_shot, got %q", escStep.ID)
	}
	if len(escStep.Deps) != 1 || escStep.Deps[0] != "input" {
		t.Errorf("expected deps=[input], got %v", escStep.Deps)
	}
}

// Test: Close cancels parsing
func TestClose(t *testing.T) {
	pr, pw := io.Pipe()
	p := NewParser(pr)

	// Close the parser immediately
	p.Close()

	// Write some data (may or may not be processed)
	go func() {
		pw.Write([]byte("```io step\nbody\n```\n"))
		pw.Close()
	}()

	// Events channel should close (either with or without events)
	done := make(chan struct{})
	go func() {
		for range p.Events() {
		}
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(500 * time.Millisecond):
		t.Fatal("parser did not close in time")
	}
}

// TestParser_FuncDef: parses defun header → ID=name, Lang=params, Type=FuncDef
func TestParser_FuncDef(t *testing.T) {
	input := "```defun web_research(query, depth=2)\nbody\n```\n"
	events, err := collectEvents(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	s := events[0].Step
	if s == nil {
		t.Fatal("expected Step event, got nil")
	}
	if s.Type != FuncDef {
		t.Errorf("expected Type=FuncDef, got %v", s.Type)
	}
	if s.ID != "web_research" {
		t.Errorf("expected ID=web_research, got %q", s.ID)
	}
	if s.Lang != "query, depth=2" {
		t.Errorf("expected Lang=%q, got %q", "query, depth=2", s.Lang)
	}
	if s.Body != "body" {
		t.Errorf("expected Body=body, got %q", s.Body)
	}
}

// TestParser_FuncCall: parses call header with deps → Type=FuncCall, ID, Deps
func TestParser_FuncCall(t *testing.T) {
	input := "```call(dep1) my_call\nbody\n```\n"
	events, err := collectEvents(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	s := events[0].Step
	if s == nil {
		t.Fatal("expected Step event, got nil")
	}
	if s.Type != FuncCall {
		t.Errorf("expected Type=FuncCall, got %v", s.Type)
	}
	if s.ID != "my_call" {
		t.Errorf("expected ID=my_call, got %q", s.ID)
	}
	if len(s.Deps) != 1 || s.Deps[0] != "dep1" {
		t.Errorf("expected Deps=[dep1], got %v", s.Deps)
	}
}

// TestParser_Agent: parses agent header with deps → Type=Agent, ID, Deps
func TestParser_Agent(t *testing.T) {
	input := "```agent(step1) reviewer\nbody\n```\n"
	events, err := collectEvents(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	s := events[0].Step
	if s == nil {
		t.Fatal("expected Step event, got nil")
	}
	if s.Type != Agent {
		t.Errorf("expected Type=Agent, got %v", s.Type)
	}
	if s.ID != "reviewer" {
		t.Errorf("expected ID=reviewer, got %q", s.ID)
	}
	if len(s.Deps) != 1 || s.Deps[0] != "step1" {
		t.Errorf("expected Deps=[step1], got %v", s.Deps)
	}
}

// Benchmark: Parse 100-step plan
func BenchmarkParse100Steps(b *testing.B) {
	// Build a 100-step plan
	var sb strings.Builder
	for i := 0; i < 100; i++ {
		sb.WriteString("```io step_" + string(rune('a'+i%26)) + "\n")
		sb.WriteString("GET /api/resource/" + string(rune('a'+i%26)) + "\n")
		sb.WriteString("```\n")
	}
	plan := sb.String()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		events, err := collectEvents(plan)
		if err != nil {
			b.Fatalf("error: %v", err)
		}
		if len(events) != 100 {
			b.Fatalf("expected 100 events, got %d", len(events))
		}
	}
}

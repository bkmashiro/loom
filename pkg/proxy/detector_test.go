package proxy

import (
	"strings"
	"testing"
)

// feedAll feeds all tokens and collects all actions.
func feedAll(d *PlanDetector, tokens []string) []DetectorAction {
	var all []DetectorAction
	for _, tok := range tokens {
		all = append(all, d.Feed(tok)...)
	}
	return all
}

// countByType counts actions of a specific type.
func countByType(actions []DetectorAction, t ActionType) int {
	n := 0
	for _, a := range actions {
		if a.Type == t {
			n++
		}
	}
	return n
}

func hasAction(actions []DetectorAction, t ActionType) bool {
	return countByType(actions, t) > 0
}

// TestDetector_NoFences — plain text, no plan detected.
func TestDetector_NoFences(t *testing.T) {
	d := &PlanDetector{}
	tokens := []string{"Hello, ", "world!\n", "This is plain text.\n"}
	actions := feedAll(d, tokens)

	if d.HasPlan() {
		t.Error("expected HasPlan()=false for plain text")
	}
	if d.State() != StateIdle {
		t.Errorf("expected StateIdle, got %v", d.State())
	}
	for _, a := range actions {
		if a.Type == ActionSuppress {
			t.Errorf("unexpected ActionSuppress for plain text")
		}
	}
}

// TestDetector_NonLoomFence — ```python fence, treated as plain text.
func TestDetector_NonLoomFence(t *testing.T) {
	d := &PlanDetector{}
	input := "```python\nprint('hello')\n```\n"
	actions := feedAll(d, []string{input})

	if d.HasPlan() {
		t.Error("expected HasPlan()=false for non-Loom fence")
	}
	if d.State() != StateIdle {
		t.Errorf("expected StateIdle after non-Loom fence, got %v", d.State())
	}
	for _, a := range actions {
		if a.Type == ActionSuppress {
			t.Errorf("unexpected ActionSuppress for non-Loom fence: %v", a.Content)
		}
	}
}

// TestDetector_SingleStep — one io fence closes → HasPlan()=true, StateHasPlan.
func TestDetector_SingleStep(t *testing.T) {
	d := &PlanDetector{}
	input := "```io fetch\nGET https://api.example.com/users\n```\n"
	actions := feedAll(d, []string{input})

	if d.State() != StateHasPlan {
		t.Errorf("expected StateHasPlan, got %v", d.State())
	}
	if !d.HasPlan() {
		t.Error("expected HasPlan()=true after fence closes")
	}
	if !hasAction(actions, ActionSuppress) {
		t.Error("expected ActionSuppress for fence content")
	}
	if !strings.Contains(d.PlanText(), "GET https://api.example.com/users") {
		t.Errorf("plan text missing fetch body: %q", d.PlanText())
	}
}

// TestDetector_PrePlanText — prose before fence captured in PrePlanText().
func TestDetector_PrePlanText(t *testing.T) {
	d := &PlanDetector{}
	input := "Let me look that up.\n```io fetch\nGET https://example.com\n```\n"
	feedAll(d, []string{input})

	if !d.HasPlan() {
		t.Error("expected HasPlan()=true")
	}
	if d.PrePlanText() != "Let me look that up.\n" {
		t.Errorf("unexpected PrePlanText: %q", d.PrePlanText())
	}
	if strings.Contains(d.PlanText(), "Let me look that up.") {
		t.Errorf("pre-plan prose leaked into PlanText: %q", d.PlanText())
	}
}

// TestDetector_MultiStep — 3 fences all detected, no return needed.
func TestDetector_MultiStep(t *testing.T) {
	d := &PlanDetector{}
	plan := strings.Join([]string{
		"```io fetch_user\nGET https://api.example.com/user/1\n```\n",
		"```io fetch_posts\nGET https://api.example.com/posts?user=1\n```\n",
		"```pure build_feed\nreturn merge(fetch_user, fetch_posts)\n```\n",
	}, "")

	feedAll(d, []string{plan})

	if d.State() != StateHasPlan {
		t.Errorf("expected StateHasPlan, got %v", d.State())
	}
	if !d.HasPlan() {
		t.Error("expected HasPlan()=true")
	}
	if d.StepCount() != 3 {
		t.Errorf("expected 3 completed steps, got %d", d.StepCount())
	}
	planText := d.PlanText()
	if !strings.Contains(planText, "fetch_user") {
		t.Error("plan text missing fetch_user step")
	}
	if !strings.Contains(planText, "fetch_posts") {
		t.Error("plan text missing fetch_posts step")
	}
	if !strings.Contains(planText, "build_feed") {
		t.Error("plan text missing build_feed step")
	}
}

// TestDetector_SplitTokens — same plan fed one char at a time → same result.
func TestDetector_SplitTokens(t *testing.T) {
	d := &PlanDetector{}
	plan := "```io fetch_user\nGET https://api.example.com/user/1\n```\n"

	var tokens []string
	for _, ch := range plan {
		tokens = append(tokens, string(ch))
	}
	feedAll(d, tokens)

	if d.State() != StateHasPlan {
		t.Errorf("expected StateHasPlan with split tokens, got %v", d.State())
	}
	if !d.HasPlan() {
		t.Error("expected HasPlan()=true with split tokens")
	}
}

// TestDetector_ProseAfterFences — text in StateHasPlan is suppressed (part of plan).
func TestDetector_ProseAfterFences(t *testing.T) {
	d := &PlanDetector{}
	plan := strings.Join([]string{
		"```io step1\nGET https://api.example.com/a\n```\n",
		"Now fetching step 2...\n", // prose in StateHasPlan
		"```io step2\nGET https://api.example.com/b\n```\n",
	}, "")

	actions := feedAll(d, []string{plan})

	if d.State() != StateHasPlan {
		t.Errorf("expected StateHasPlan, got %v", d.State())
	}
	if !d.HasPlan() {
		t.Error("expected HasPlan()=true")
	}
	// The inter-fence prose should be suppressed
	for _, a := range actions {
		if a.Type == ActionForward && strings.Contains(a.Content, "Now fetching") {
			t.Error("inter-fence prose should be suppressed, not forwarded")
		}
	}
	if !strings.Contains(d.PlanText(), "Now fetching step 2") {
		t.Errorf("plan text should include inter-fence prose: %q", d.PlanText())
	}
}

// TestDetector_ReturnIgnored — `return` directive is treated as regular plan prose in v2.
func TestDetector_ReturnIgnored(t *testing.T) {
	d := &PlanDetector{}
	plan := "```io fetch_user\nGET https://api.example.com/user/1\n```\nreturn fetch_user\n"
	actions := feedAll(d, []string{plan})

	// In v2, `return` is just prose in StateHasPlan — still HasPlan=true.
	if !d.HasPlan() {
		t.Error("expected HasPlan()=true even with `return` directive")
	}
	if d.State() != StateHasPlan {
		t.Errorf("expected StateHasPlan, got %v", d.State())
	}
	// No ActionForward for the return line (it's in StateHasPlan → suppressed).
	for _, a := range actions {
		if a.Type == ActionForward && strings.Contains(a.Content, "return fetch_user") {
			t.Error("return directive in StateHasPlan should be suppressed, not forwarded")
		}
	}
}

// TestDetector_Reset — reset clears state; detector works again after reset.
func TestDetector_Reset(t *testing.T) {
	d := &PlanDetector{}
	plan := "```io fetch_user\nGET https://api.example.com/user/1\n```\n"
	feedAll(d, []string{plan})

	if !d.HasPlan() {
		t.Fatal("pre-reset: expected HasPlan()=true")
	}

	d.Reset()

	if d.State() != StateIdle {
		t.Errorf("after reset: expected StateIdle, got %v", d.State())
	}
	if d.HasPlan() {
		t.Error("after reset: expected HasPlan()=false")
	}
	if d.PlanText() != "" {
		t.Errorf("after reset: expected empty planText, got %q", d.PlanText())
	}
	if d.PrePlanText() != "" {
		t.Errorf("after reset: expected empty prePlanText, got %q", d.PrePlanText())
	}

	// Should work again after reset.
	feedAll(d, []string{plan})
	if !d.HasPlan() {
		t.Error("after reset+refeed: expected HasPlan()=true")
	}
}

// TestDetector_IncompleteAtEOF — incomplete fence at end of stream: stepCount stays 0
// if no fence completed.
func TestDetector_IncompleteAtEOF(t *testing.T) {
	d := &PlanDetector{}
	// Fence opened but not closed.
	input := "```io fetch\nGET https://example.com\n"
	feedAll(d, []string{input})

	if d.HasPlan() {
		t.Error("expected HasPlan()=false for incomplete fence")
	}
	if d.State() != StateInFence {
		t.Errorf("expected StateInFence, got %v", d.State())
	}
}

// TestDetector_FenceWithPureAndIO — mixed step types all detected.
func TestDetector_FenceWithPureAndIO(t *testing.T) {
	d := &PlanDetector{}
	plan := strings.Join([]string{
		"```io fetch\nGET https://example.com\n```\n",
		"```pure transform\nreturn fetch.data\n```\n",
		"```write save\nPOST https://store.example.com\n${transform}\n```\n",
	}, "")

	feedAll(d, []string{plan})

	if d.StepCount() != 3 {
		t.Errorf("expected 3 steps, got %d", d.StepCount())
	}
}

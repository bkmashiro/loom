package proxy

import "strings"

type DetectorState int

const (
	StateIdle DetectorState = iota
	StateInFence
	StateBetweenSteps
	StatePlanComplete
)

type ActionType int

const (
	ActionForward ActionType = iota
	ActionBuffer
	ActionSuppress
	ActionFlush
	ActionPlanComplete
)

type DetectorAction struct {
	Type    ActionType
	Content string
}

type PlanDetector struct {
	state     DetectorState
	lineBuf   strings.Builder
	planText  strings.Builder
	stepCount int
	returnID  string
}

// loomStepTypes is the set of known Loom step type keywords.
var loomStepTypes = map[string]bool{
	"io": true, "write": true, "pure": true,
	"shell": true, "async": true, "escape": true,
}

// Feed processes a content token. Returns actions for the tee.
// Tokens may not be complete lines — the detector buffers partial lines.
func (d *PlanDetector) Feed(token string) []DetectorAction {
	if d.state == StatePlanComplete {
		// After plan complete, forward everything
		return []DetectorAction{{Type: ActionForward, Content: token}}
	}

	var actions []DetectorAction

	for i := 0; i < len(token); i++ {
		ch := token[i]
		if ch == '\n' {
			line := d.lineBuf.String()
			d.lineBuf.Reset()
			lineActions := d.processLine(line + "\n")
			actions = append(actions, lineActions...)
		} else {
			d.lineBuf.WriteByte(ch)
		}
	}

	// If there's still content in lineBuf (partial line, no newline yet),
	// we buffer it — emit ActionBuffer to signal we're holding.
	if d.lineBuf.Len() > 0 {
		switch d.state {
		case StateIdle:
			// Could be the start of a fence — buffer
			actions = append(actions, DetectorAction{Type: ActionBuffer, Content: d.lineBuf.String()})
		case StateInFence, StateBetweenSteps:
			// Inside plan context — suppress
			actions = append(actions, DetectorAction{Type: ActionSuppress, Content: d.lineBuf.String()})
		}
	}

	return actions
}

// processLine handles a complete line (including the trailing \n).
func (d *PlanDetector) processLine(line string) []DetectorAction {
	// line includes the trailing newline; trim for matching but keep for plan accumulation
	trimmed := strings.TrimRight(line, "\n\r")

	switch d.state {
	case StateIdle:
		if loomFenceType := fenceOpenType(trimmed); loomFenceType != "" {
			// Loom fence opener detected
			d.state = StateInFence
			d.planText.WriteString(line)
			return []DetectorAction{{Type: ActionSuppress, Content: line}}
		}
		// Not a Loom fence — forward as-is
		return []DetectorAction{{Type: ActionForward, Content: line}}

	case StateInFence:
		d.planText.WriteString(line)
		if trimmed == "```" {
			// Fence closer
			d.stepCount++
			d.state = StateBetweenSteps
		}
		return []DetectorAction{{Type: ActionSuppress, Content: line}}

	case StateBetweenSteps:
		if loomFenceType := fenceOpenType(trimmed); loomFenceType != "" {
			// Another step
			d.state = StateInFence
			d.planText.WriteString(line)
			return []DetectorAction{{Type: ActionSuppress, Content: line}}
		}
		if id, ok := returnDirective(trimmed); ok {
			// Return directive found — plan complete
			d.returnID = id
			d.planText.WriteString(line)
			d.state = StatePlanComplete
			return []DetectorAction{{Type: ActionPlanComplete, Content: line}}
		}
		// Prose between steps — stay in StateBetweenSteps, suppress
		d.planText.WriteString(line)
		return []DetectorAction{{Type: ActionSuppress, Content: line}}
	}

	return nil
}

// fenceOpenType checks if a line is a Loom fence opener (```<type>).
// Returns the type keyword if it is, empty string otherwise.
func fenceOpenType(line string) string {
	if !strings.HasPrefix(line, "```") {
		return ""
	}
	rest := line[3:]
	// Split on whitespace to get the type keyword
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return ""
	}
	keyword := fields[0]
	if loomStepTypes[keyword] {
		return keyword
	}
	return ""
}

// returnDirective checks if a line is a "return <id>" directive.
// Returns (id, true) if it is.
func returnDirective(line string) (string, bool) {
	if !strings.HasPrefix(line, "return ") {
		return "", false
	}
	id := strings.TrimSpace(line[len("return "):])
	if id == "" {
		return "", false
	}
	// Validate: must be a valid identifier [a-zA-Z_][a-zA-Z0-9_]*
	for i, ch := range id {
		if i == 0 {
			if !isLetter(ch) && ch != '_' {
				return "", false
			}
		} else {
			if !isLetter(ch) && !isDigit(ch) && ch != '_' {
				return "", false
			}
		}
	}
	return id, true
}

func isLetter(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isDigit(ch rune) bool {
	return ch >= '0' && ch <= '9'
}

// State returns the current detector state.
func (d *PlanDetector) State() DetectorState {
	return d.state
}

// PlanText returns the accumulated plan text (valid after StatePlanComplete).
func (d *PlanDetector) PlanText() string {
	return d.planText.String()
}

// ReturnID returns the step ID from the return directive.
func (d *PlanDetector) ReturnID() string {
	return d.returnID
}

// Reset clears state for reuse.
func (d *PlanDetector) Reset() {
	d.state = StateIdle
	d.lineBuf.Reset()
	d.planText.Reset()
	d.stepCount = 0
	d.returnID = ""
}

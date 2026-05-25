package parser

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
)

// StepType represents the type of execution step.
type StepType int

const (
	IO      StepType = iota // 0 — Idempotent read
	Write                   // 1 — Side-effecting write
	Pure                    // 2 — Deterministic computation
	Shell                   // 3 — Shell command in WASM sandbox
	Async                   // 4 — Fire-and-forget
	Escape                  // 5 — Raw tool call
	FuncDef                 // 6 — function definition (defun)
	FuncCall                // 7 — function invocation (call)
	Agent                   // 8 — sub-agent delegation
)

// Step represents a parsed code fence block.
type Step struct {
	ID   string
	Type StepType
	Deps []string
	Body string
	Lang string // e.g. "python", "js" (from "pure.python")
}

// ReturnDirective represents a return statement outside any fence.
type ReturnDirective struct {
	StepID string
}

// Event is emitted by the parser as fences complete.
type Event struct {
	Step   *Step
	Return *ReturnDirective
}

// Parser reads from an io.Reader and emits Events.
type Parser struct {
	r      io.Reader
	events chan Event
	err    error
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewParser creates a parser that reads from r. Parsing begins in a background goroutine.
func NewParser(r io.Reader) *Parser {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Parser{
		r:      r,
		events: make(chan Event, 64),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go p.run(ctx)
	return p
}

// Events returns channel that emits Events as fences complete. Closed when done.
func (p *Parser) Events() <-chan Event {
	return p.events
}

// Err returns first unrecoverable error, or nil. Only valid after Events() closed.
func (p *Parser) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// Close cancels parsing. Safe to call multiple times.
func (p *Parser) Close() {
	p.cancel()
}

type state int

const (
	stateText state = iota
	stateBody
)

func (p *Parser) setErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err == nil {
		p.err = err
	}
}

func (p *Parser) run(ctx context.Context) {
	defer close(p.events)
	defer close(p.done)

	scanner := bufio.NewScanner(p.r)
	currentState := stateText
	asyncCounter := 0

	var (
		currentStep *Step
		bodyLines   []string
	)

	emit := func(ev Event) bool {
		select {
		case <-ctx.Done():
			return false
		case p.events <- ev:
			return true
		}
	}

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line := scanner.Text()

		switch currentState {
		case stateText:
			// Check for fence opener: line starting with ``` followed by word chars
			if strings.HasPrefix(line, "```") {
				rest := line[3:]
				if rest == "" {
					// bare ``` not at start of fence — ignore
					continue
				}
				// Must start with a word character
				if len(rest) > 0 && isWordChar(rune(rest[0])) {
					step, skip := parseFenceHeader(rest, asyncCounter)
					if skip {
						// malformed / unknown type — skip until closing fence
						currentState = stateBody
						currentStep = nil
						bodyLines = nil
					} else {
						if step.Type == Async && step.ID == fmt.Sprintf("_async_%d", asyncCounter) {
							asyncCounter++
						}
						currentState = stateBody
						currentStep = step
						bodyLines = nil
					}
				}
				// else ignore line
			} else if matched, id := matchReturn(line); matched {
				ev := Event{Return: &ReturnDirective{StepID: id}}
				if !emit(ev) {
					return
				}
			}
			// Other lines in stateText are ignored

		case stateBody:
			if line == "```" {
				// Close fence
				if currentStep != nil {
					currentStep.Body = strings.Join(bodyLines, "\n")
					if !emit(Event{Step: currentStep}) {
						return
					}
				}
				currentStep = nil
				bodyLines = nil
				currentState = stateText
			} else {
				bodyLines = append(bodyLines, line)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		p.setErr(err)
		return
	}

	// EOF reached
	if currentState == stateBody {
		// Unclosed fence at EOF
		p.setErr(fmt.Errorf("parser: unclosed fence at EOF"))
	}
}

func isWordChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_'
}

func matchReturn(line string) (bool, string) {
	s := strings.TrimRight(line, " \t")
	if !strings.HasPrefix(s, "return ") {
		return false, ""
	}
	id := strings.TrimSpace(s[7:])
	if id == "" {
		return false, ""
	}
	// id must be a simple word
	for _, r := range id {
		if !isWordChar(r) {
			return false, ""
		}
	}
	return true, id
}

// stepTypeMap maps keyword strings to StepType values.
var stepTypeMap = map[string]StepType{
	"io":     IO,
	"write":  Write,
	"pure":   Pure,
	"shell":  Shell,
	"async":  Async,
	"escape": Escape,
	"defun":  FuncDef,
	"call":   FuncCall,
	"agent":  Agent,
}

// parseFenceHeader parses the text after the opening ```.
// Returns (step, skip) where skip=true means unknown/malformed type.
func parseFenceHeader(header string, asyncCounter int) (*Step, bool) {
	header = strings.TrimSpace(header)

	// Split type token from the rest
	// Type token is everything up to first space, '(' or end
	typeEnd := strings.IndexAny(header, " (")
	var typeToken, rest string
	if typeEnd == -1 {
		typeToken = header
		rest = ""
	} else {
		typeToken = header[:typeEnd]
		rest = strings.TrimSpace(header[typeEnd:])
	}

	// Parse lang suffix from typeToken (e.g. "pure.python")
	lang := ""
	keyword := typeToken
	if dotIdx := strings.Index(typeToken, "."); dotIdx != -1 {
		keyword = typeToken[:dotIdx]
		lang = typeToken[dotIdx+1:]
	}

	stepType, ok := stepTypeMap[keyword]
	if !ok {
		return nil, true
	}

	// FuncDef (defun) has special syntax: defun name(param1, param2)
	// The function name goes into ID, params string goes into Lang.
	if stepType == FuncDef {
		rest = strings.TrimSpace(rest)
		// rest should be: "name(param1, param2)" or just "name"
		parenIdx := strings.Index(rest, "(")
		var funcName, params string
		if parenIdx == -1 {
			funcName = strings.TrimSpace(rest)
		} else {
			funcName = strings.TrimSpace(rest[:parenIdx])
			closeIdx := strings.Index(rest[parenIdx:], ")")
			if closeIdx == -1 {
				return nil, true
			}
			params = rest[parenIdx+1 : parenIdx+closeIdx]
		}
		return &Step{
			ID:   funcName,
			Type: FuncDef,
			Deps: []string{},
			Lang: params,
		}, false
	}

	// Now parse rest for: optional deps in parens and optional ID
	// Possible formats for rest:
	//   ""                       → no deps, no id
	//   "id"                     → no deps, id = "id"
	//   "(dep1, dep2)"           → deps, no id (async)
	//   "(dep1, dep2) id"        → deps, then id
	//   "id(dep1, dep2)"         — NOT standard but let's handle both orderings

	var deps []string
	var id string

	rest = strings.TrimSpace(rest)

	if strings.HasPrefix(rest, "(") {
		// deps first, then optional id
		closeIdx := strings.Index(rest, ")")
		if closeIdx == -1 {
			// malformed
			return nil, true
		}
		depsStr := rest[1:closeIdx]
		deps = parseDeps(depsStr)
		id = strings.TrimSpace(rest[closeIdx+1:])
	} else {
		// id possibly first, then optional deps
		// Check if rest contains '('
		parenIdx := strings.Index(rest, "(")
		if parenIdx == -1 {
			// just an id or empty
			id = rest
		} else {
			// id before paren
			id = strings.TrimSpace(rest[:parenIdx])
			closeIdx := strings.Index(rest[parenIdx:], ")")
			if closeIdx == -1 {
				return nil, true
			}
			depsStr := rest[parenIdx+1 : parenIdx+closeIdx]
			deps = parseDeps(depsStr)
		}
	}

	if deps == nil {
		deps = []string{}
	}

	// For async, auto-generate ID if not provided
	if id == "" && stepType == Async {
		id = fmt.Sprintf("_async_%d", asyncCounter)
	}

	return &Step{
		ID:   id,
		Type: stepType,
		Deps: deps,
		Lang: lang,
	}, false
}

func parseDeps(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return []string{}
	}
	parts := strings.Split(s, ",")
	deps := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			deps = append(deps, p)
		}
	}
	return deps
}

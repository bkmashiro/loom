package primitives

import (
	"fmt"
	"strings"
)

// FSCommand represents a parsed filesystem command from a step body line.
type FSCommand struct {
	Op      string // "read", "write", "append", "ls"
	Path    string
	Content string // for write/append
}

var validFSOps = map[string]bool{
	"read":   true,
	"write":  true,
	"append": true,
	"ls":     true,
}

// ParseFSCommand parses a step body line into an FSCommand.
// Formats:
//
//	read <path>
//	write <path> <content>
//	append <path> <content>
//	ls <path>
func ParseFSCommand(line string) (FSCommand, error) {
	line = strings.TrimSpace(line)
	tokens := strings.Fields(line)

	if len(tokens) == 0 {
		return FSCommand{}, fmt.Errorf("empty input")
	}

	op := strings.ToLower(tokens[0])
	if !validFSOps[op] {
		return FSCommand{}, fmt.Errorf("unknown FS operation: %q", tokens[0])
	}

	if len(tokens) < 2 {
		return FSCommand{}, fmt.Errorf("missing path for op %q", op)
	}

	path := tokens[1]
	content := strings.Join(tokens[2:], " ")

	return FSCommand{
		Op:      op,
		Path:    path,
		Content: content,
	}, nil
}

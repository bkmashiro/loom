package primitives

import (
	"context"
	"fmt"
	"io/fs"
	"strings"

	"github.com/bkmashiro/loom/pkg/sandbox"
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

// ExecuteFS executes a parsed FSCommand against sb.
// Returns (data any, err error).
// data is:
//   - []byte for ReadFile
//   - []string of entry names for ReadDir (ls)
//   - nil for WriteFile/AppendFile (success)
func ExecuteFS(ctx context.Context, cmd FSCommand, sb *sandbox.Sandbox) (any, error) {
	if sb == nil {
		return nil, sandbox.ErrNoSandbox
	}
	switch cmd.Op {
	case "read", "cat":
		data, err := sb.ReadFile(cmd.Path)
		if err != nil {
			return nil, err
		}
		return data, nil
	case "ls":
		entries, err := sb.ReadDir(cmd.Path)
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		return names, nil
	case "write":
		if err := sb.WriteFile(cmd.Path, []byte(cmd.Content), fs.FileMode(0644)); err != nil {
			return nil, err
		}
		return nil, nil
	case "append":
		if err := sb.AppendFile(cmd.Path, []byte(cmd.Content)); err != nil {
			return nil, err
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("sandbox: unknown FS op %q", cmd.Op)
	}
}

package sandbox

import (
	"errors"
	"path/filepath"
	"strings"
)

var ErrPathEscape = errors.New("sandbox: path escapes chroot boundary")

// Jail resolves untrustedPath relative to root and returns the absolute
// host path. Returns ErrPathEscape if the resolved path would leave root.
//
// Rules:
//   - Absolute guest paths (e.g. "/etc/passwd") are treated as relative
//     to root by stripping the leading slash before joining.
//   - filepath.Clean is applied before the prefix check, neutralising
//     "../" components.
func Jail(root, untrustedPath string) (string, error) {
	// Strip leading slash so absolute guest paths are rebased to root.
	rel := strings.TrimLeft(untrustedPath, "/\\")
	full := filepath.Join(root, rel)
	full = filepath.Clean(full)
	// Ensure result is still under root.
	rootClean := filepath.Clean(root)
	if !strings.HasPrefix(full+string(filepath.Separator), rootClean+string(filepath.Separator)) {
		return "", ErrPathEscape
	}
	return full, nil
}

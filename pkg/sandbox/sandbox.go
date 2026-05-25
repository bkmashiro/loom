package sandbox

import (
	"errors"
	"io/fs"
	"os"
	"sort"
	"strings"
)

var ErrReadOnly = errors.New("sandbox: filesystem is read-only")
var ErrNoSandbox = errors.New("sandbox: no sandbox configured")

// Preset describes the access mode of a mount.
type Preset int

const (
	Ephemeral  Preset = iota // in-memory, discarded after plan
	ReadOnly                 // host directory, read-only
	ReadWrite                // host directory, read-write
	Persistent               // host directory, read-write, created if absent
)

// Mount describes a single guest-path → host-path (or memory) mapping.
type Mount struct {
	Guest string // guest path prefix, e.g. "/work" or "/data"
	Host  string // host directory; empty for Ephemeral
	Mode  Preset // Ephemeral, ReadOnly, ReadWrite, or Persistent
}

// Config describes the full sandbox for a plan execution.
type Config struct {
	Mounts []Mount
}

// Sandbox is the per-plan-execution filesystem jail.
type Sandbox struct {
	mounts []resolvedMount // sorted by guest prefix length desc
}

type resolvedMount struct {
	Mount
	memfs *memFS // non-nil only for Ephemeral
}

// New constructs a Sandbox from cfg. For Persistent mounts, the host
// directory is created if it does not exist.
func New(cfg Config) (*Sandbox, error) {
	s := &Sandbox{}
	for _, m := range cfg.Mounts {
		rm := resolvedMount{Mount: m}
		switch m.Mode {
		case Ephemeral:
			rm.memfs = newMemFS()
		case Persistent:
			if err := os.MkdirAll(m.Host, 0o755); err != nil {
				return nil, err
			}
		case ReadOnly, ReadWrite:
			// nothing extra needed
		}
		s.mounts = append(s.mounts, rm)
	}
	// Sort by descending guest prefix length for longest-prefix-match.
	sort.Slice(s.mounts, func(i, j int) bool {
		return len(s.mounts[i].Guest) > len(s.mounts[j].Guest)
	})
	return s, nil
}

// resolve finds the mount with the longest matching Guest prefix and returns
// the mount and the relative path within that mount.
func (s *Sandbox) resolve(guestPath string) (*resolvedMount, string, error) {
	// Normalise guestPath to have a leading slash for prefix matching.
	if !strings.HasPrefix(guestPath, "/") {
		guestPath = "/" + guestPath
	}

	for i := range s.mounts {
		m := &s.mounts[i]
		guestPrefix := m.Guest
		if !strings.HasPrefix(guestPrefix, "/") {
			guestPrefix = "/" + guestPrefix
		}
		// Ensure prefix ends with "/" or exact match.
		if guestPath == guestPrefix || strings.HasPrefix(guestPath, strings.TrimRight(guestPrefix, "/")+"/") {
			// Compute relative path inside the mount.
			rel := strings.TrimPrefix(guestPath, strings.TrimRight(guestPrefix, "/"))
			rel = strings.TrimLeft(rel, "/")
			return m, rel, nil
		}
	}
	return nil, "", ErrPathEscape
}

// ReadFile reads the contents of guestPath.
func (s *Sandbox) ReadFile(guestPath string) ([]byte, error) {
	m, rel, err := s.resolve(guestPath)
	if err != nil {
		return nil, err
	}
	if m.memfs != nil {
		return m.memfs.ReadFile(rel)
	}
	hostPath, err := Jail(m.Host, rel)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(hostPath)
}

// WriteFile writes data to guestPath.
func (s *Sandbox) WriteFile(guestPath string, data []byte, perm fs.FileMode) error {
	m, rel, err := s.resolve(guestPath)
	if err != nil {
		return err
	}
	if m.Mode == ReadOnly {
		return ErrReadOnly
	}
	if m.memfs != nil {
		m.memfs.WriteFile(rel, data)
		return nil
	}
	hostPath, err := Jail(m.Host, rel)
	if err != nil {
		return err
	}
	return os.WriteFile(hostPath, data, perm)
}

// AppendFile appends data to guestPath.
func (s *Sandbox) AppendFile(guestPath string, data []byte) error {
	m, rel, err := s.resolve(guestPath)
	if err != nil {
		return err
	}
	if m.Mode == ReadOnly {
		return ErrReadOnly
	}
	if m.memfs != nil {
		m.memfs.AppendFile(rel, data)
		return nil
	}
	hostPath, err := Jail(m.Host, rel)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(hostPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(data)
	return err
}

// ReadDir lists directory entries under guestPath.
func (s *Sandbox) ReadDir(guestPath string) ([]fs.DirEntry, error) {
	m, rel, err := s.resolve(guestPath)
	if err != nil {
		return nil, err
	}
	if m.memfs != nil {
		return m.memfs.ReadDir(rel)
	}
	hostPath, err := Jail(m.Host, rel)
	if err != nil {
		return nil, err
	}
	return os.ReadDir(hostPath)
}

// FS returns the fs.FS for guestPath's mount (for wazero integration later).
func (s *Sandbox) FS(guestPath string) (fs.FS, error) {
	m, _, err := s.resolve(guestPath)
	if err != nil {
		return nil, err
	}
	if m.memfs != nil {
		return m.memfs, nil
	}
	return os.DirFS(m.Host), nil
}

// Snapshot captures the current state of all Ephemeral mounts.
func (s *Sandbox) Snapshot() *Snapshot {
	snap := &Snapshot{states: make(map[string]*memFS)}
	for _, m := range s.mounts {
		if m.memfs != nil {
			snap.states[m.Guest] = m.memfs.Snapshot()
		}
	}
	return snap
}

// Restore replaces all Ephemeral mount states with snap's state.
func (s *Sandbox) Restore(snap *Snapshot) {
	for i := range s.mounts {
		m := &s.mounts[i]
		if m.memfs != nil {
			if snapFS, ok := snap.states[m.Guest]; ok {
				m.memfs.Restore(snapFS)
			}
		}
	}
}

// WithTransaction executes fn and rolls back all Ephemeral mounts if fn
// returns a non-nil error.
func (s *Sandbox) WithTransaction(fn func() error) error {
	snap := s.Snapshot()
	if err := fn(); err != nil {
		s.Restore(snap)
		return err
	}
	return nil
}

// Close releases resources. Currently a no-op but reserved for future
// temp-dir cleanup.
func (s *Sandbox) Close() error { return nil }

// Snapshot holds captured state of Ephemeral mounts.
type Snapshot struct {
	states map[string]*memFS // guest mount prefix → memFS snapshot
}

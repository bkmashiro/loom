package sandbox

import (
	"io"
	"io/fs"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// memFS is an in-memory filesystem. Keys are slash-separated paths without
// a leading slash (e.g. "work/output.json"). Directories are implicit.
type memFS struct {
	mu    sync.RWMutex
	files map[string][]byte
}

func newMemFS() *memFS { return &memFS{files: make(map[string][]byte)} }

// Open returns a memFile for reads, fs.ErrNotExist otherwise.
func (m *memFS) Open(name string) (fs.File, error) {
	// Normalise: strip leading slash.
	name = strings.TrimLeft(name, "/")
	if name == "." || name == "" {
		// Return a directory file for root.
		return &memFile{name: ".", isDir: true, mfs: m}, nil
	}
	m.mu.RLock()
	data, ok := m.files[name]
	m.mu.RUnlock()
	if !ok {
		// Check if it could be a directory (i.e. any key has it as prefix).
		m.mu.RLock()
		prefix := name + "/"
		isDir := false
		for k := range m.files {
			if strings.HasPrefix(k, prefix) {
				isDir = true
				break
			}
		}
		m.mu.RUnlock()
		if isDir {
			return &memFile{name: name, isDir: true, mfs: m}, nil
		}
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return &memFile{name: name, contents: slices.Clone(data)}, nil
}

// ReadFile returns the contents of name or fs.ErrNotExist.
func (m *memFS) ReadFile(name string) ([]byte, error) {
	name = strings.TrimLeft(name, "/")
	m.mu.RLock()
	data, ok := m.files[name]
	m.mu.RUnlock()
	if !ok {
		return nil, &fs.PathError{Op: "read", Path: name, Err: fs.ErrNotExist}
	}
	return slices.Clone(data), nil
}

// WriteFile creates or overwrites name with data.
func (m *memFS) WriteFile(name string, data []byte) {
	name = strings.TrimLeft(name, "/")
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[name] = slices.Clone(data)
}

// AppendFile appends data to name.
func (m *memFS) AppendFile(name string, data []byte) {
	name = strings.TrimLeft(name, "/")
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[name] = append(m.files[name], data...)
}

// ReadDir returns DirEntry values for entries whose path has name as parent.
func (m *memFS) ReadDir(name string) ([]fs.DirEntry, error) {
	name = strings.TrimLeft(name, "/")

	m.mu.RLock()
	defer m.mu.RUnlock()

	seen := make(map[string]bool)
	var entries []fs.DirEntry

	var prefix string
	if name == "." || name == "" {
		prefix = ""
	} else {
		prefix = name + "/"
	}

	for k := range m.files {
		var rel string
		if prefix == "" {
			rel = k
		} else {
			if !strings.HasPrefix(k, prefix) {
				continue
			}
			rel = k[len(prefix):]
		}
		// Only immediate children (no slash in remainder).
		slash := strings.Index(rel, "/")
		var entryName string
		var isDir bool
		if slash == -1 {
			entryName = rel
			isDir = false
		} else {
			entryName = rel[:slash]
			isDir = true
		}
		if entryName == "" || seen[entryName] {
			continue
		}
		seen[entryName] = true

		var size int64
		if !isDir {
			fullKey := entryName
			if prefix != "" {
				fullKey = prefix + entryName
			}
			size = int64(len(m.files[fullKey]))
		}
		entries = append(entries, &memDirEntry{
			name:  entryName,
			isDir: isDir,
			size:  size,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	return entries, nil
}

// Snapshot returns a deep copy of the current state.
func (m *memFS) Snapshot() *memFS {
	m.mu.RLock()
	defer m.mu.RUnlock()
	snap := &memFS{files: make(map[string][]byte, len(m.files))}
	for k, v := range m.files {
		snap.files[k] = slices.Clone(v)
	}
	return snap
}

// Restore replaces current state with snap's state.
func (m *memFS) Restore(snap *memFS) {
	snap.mu.RLock()
	defer snap.mu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files = make(map[string][]byte, len(snap.files))
	for k, v := range snap.files {
		m.files[k] = slices.Clone(v)
	}
}

// memFile implements fs.File for in-memory files.
type memFile struct {
	name     string
	contents []byte
	offset   int
	isDir    bool
	mfs      *memFS // set only for directory entries
}

func (f *memFile) Read(b []byte) (int, error) {
	if f.isDir {
		return 0, &fs.PathError{Op: "read", Path: f.name, Err: fs.ErrInvalid}
	}
	if f.offset >= len(f.contents) {
		return 0, io.EOF
	}
	n := copy(b, f.contents[f.offset:])
	f.offset += n
	return n, nil
}

func (f *memFile) Close() error { return nil }

func (f *memFile) Stat() (fs.FileInfo, error) {
	return &memFileInfo{
		name:  f.name,
		size:  int64(len(f.contents)),
		isDir: f.isDir,
	}, nil
}

// memFileInfo implements fs.FileInfo.
type memFileInfo struct {
	name  string
	size  int64
	isDir bool
}

func (fi *memFileInfo) Name() string      { return fi.name }
func (fi *memFileInfo) Size() int64       { return fi.size }
func (fi *memFileInfo) Mode() fs.FileMode {
	if fi.isDir {
		return fs.ModeDir | 0o755
	}
	return 0o644
}
func (fi *memFileInfo) ModTime() time.Time { return time.Time{} }
func (fi *memFileInfo) IsDir() bool        { return fi.isDir }
func (fi *memFileInfo) Sys() any           { return nil }

// memDirEntry implements fs.DirEntry.
type memDirEntry struct {
	name  string
	isDir bool
	size  int64
}

func (e *memDirEntry) Name() string { return e.name }
func (e *memDirEntry) IsDir() bool  { return e.isDir }
func (e *memDirEntry) Type() fs.FileMode {
	if e.isDir {
		return fs.ModeDir
	}
	return 0
}
func (e *memDirEntry) Info() (fs.FileInfo, error) {
	return &memFileInfo{name: e.name, size: e.size, isDir: e.isDir}, nil
}

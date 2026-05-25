package sandbox

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// ── TestJail ────────────────────────────────────────────────────────────────

func TestJail(t *testing.T) {
	root := "/sandbox/root"

	tests := []struct {
		name        string
		path        string
		wantErr     bool
		wantSuffix  string // expected suffix of returned path (when no error)
	}{
		{"traversal attempt", "../../etc/passwd", true, ""},
		{"absolute traversal", "/etc/passwd", false, filepath.Join(root, "etc/passwd")},
		{"normal path", "work/out.txt", false, filepath.Join(root, "work/out.txt")},
		{"empty path", "", false, root},
		{"root path", "/", false, root},
		{"deep traversal", "a/b/../../../etc/passwd", true, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Jail(root, tc.path)
			if tc.wantErr {
				if !errors.Is(err, ErrPathEscape) {
					t.Fatalf("Jail(%q, %q) = %v; want ErrPathEscape", root, tc.path, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Jail(%q, %q) unexpected error: %v", root, tc.path, err)
			}
			if got != tc.wantSuffix {
				t.Fatalf("Jail(%q, %q) = %q; want %q", root, tc.path, got, tc.wantSuffix)
			}
		})
	}
}

// ── TestEphemeral_ReadWrite ──────────────────────────────────────────────────

func TestEphemeral_ReadWrite(t *testing.T) {
	sb, err := New(EphemeralSandbox())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Close()

	// Write then read back.
	if err := sb.WriteFile("/work/hello.txt", []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := sb.ReadFile("/work/hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q; want %q", got, "hello")
	}

	// Overwrite.
	if err := sb.WriteFile("/work/hello.txt", []byte("world"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, _ = sb.ReadFile("/work/hello.txt")
	if string(got) != "world" {
		t.Fatalf("after overwrite got %q; want %q", got, "world")
	}

	// Append.
	if err := sb.AppendFile("/work/hello.txt", []byte("!")); err != nil {
		t.Fatal(err)
	}
	got, _ = sb.ReadFile("/work/hello.txt")
	if string(got) != "world!" {
		t.Fatalf("after append got %q; want %q", got, "world!")
	}
}

// ── TestEphemeral_ReadDir ───────────────────────────────────────────────────

func TestEphemeral_ReadDir(t *testing.T) {
	sb, err := New(EphemeralSandbox())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Close()

	sb.WriteFile("/work/a.txt", []byte("a"), 0o644)
	sb.WriteFile("/work/b.txt", []byte("b"), 0o644)
	sb.WriteFile("/work/sub/c.txt", []byte("c"), 0o644)

	entries, err := sb.ReadDir("/work")
	if err != nil {
		t.Fatal(err)
	}

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}

	// Should have a.txt, b.txt, sub (directory) — not sub/c.txt directly.
	want := map[string]bool{"a.txt": true, "b.txt": true, "sub": true}
	if len(entries) != 3 {
		t.Fatalf("ReadDir returned %v; want 3 entries", names)
	}
	for _, n := range names {
		if !want[n] {
			t.Fatalf("unexpected entry %q", n)
		}
	}
}

// ── TestEphemeral_NotExist ──────────────────────────────────────────────────

func TestEphemeral_NotExist(t *testing.T) {
	sb, _ := New(EphemeralSandbox())
	defer sb.Close()

	_, err := sb.ReadFile("/missing.txt")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected fs.ErrNotExist, got %v", err)
	}
}

// ── TestReadOnly_RejectsWrite ───────────────────────────────────────────────

func TestReadOnly_RejectsWrite(t *testing.T) {
	dir := t.TempDir()
	sb, err := New(ReadOnlySandbox(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Close()

	err = sb.WriteFile("/data.txt", []byte("x"), 0o644)
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("expected ErrReadOnly, got %v", err)
	}
}

// ── TestReadOnly_RejectsTraversal ───────────────────────────────────────────

func TestReadOnly_RejectsTraversal(t *testing.T) {
	dir := t.TempDir()
	sb, err := New(ReadOnlySandbox(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Close()

	// Write a file first via OS so we have something to try to reach.
	_ = os.WriteFile(filepath.Join(dir, "safe.txt"), []byte("safe"), 0o644)

	// Attempt traversal: guest path that would escape root.
	_, err = sb.ReadFile("/../../etc/passwd")
	if !errors.Is(err, ErrPathEscape) {
		t.Fatalf("expected ErrPathEscape, got %v", err)
	}
}

// ── TestReadWrite_HostRoundTrip ─────────────────────────────────────────────

func TestReadWrite_HostRoundTrip(t *testing.T) {
	dir := t.TempDir()
	sb, err := New(ReadWriteSandbox(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Close()

	if err := sb.WriteFile("/output.txt", []byte("roundtrip"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Read directly from host filesystem.
	data, err := os.ReadFile(filepath.Join(dir, "output.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "roundtrip" {
		t.Fatalf("host file = %q; want %q", data, "roundtrip")
	}
}

// ── TestPersistent_CreatesDir ───────────────────────────────────────────────

func TestPersistent_CreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "new", "nested", "dir")
	// dir must not exist yet.
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected dir to not exist before test")
	}

	sb, err := New(PersistentSandbox(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Close()

	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("expected directory to be created: %v", err)
	}
}

// ── TestSandbox_MultiMount ──────────────────────────────────────────────────

func TestSandbox_MultiMount(t *testing.T) {
	dataDir := t.TempDir()
	// Write a file in dataDir on the host.
	if err := os.WriteFile(filepath.Join(dataDir, "readme.txt"), []byte("readonly data"), 0o644); err != nil {
		t.Fatal(err)
	}

	sb, err := New(Config{Mounts: []Mount{
		{Guest: "/data", Host: dataDir, Mode: ReadOnly},
		{Guest: "/work", Mode: Ephemeral},
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Close()

	// Read from /data.
	got, err := sb.ReadFile("/data/readme.txt")
	if err != nil {
		t.Fatalf("ReadFile /data/readme.txt: %v", err)
	}
	if string(got) != "readonly data" {
		t.Fatalf("got %q; want %q", got, "readonly data")
	}

	// Write to /work (Ephemeral) succeeds.
	if err := sb.WriteFile("/work/result.txt", []byte("result"), 0o644); err != nil {
		t.Fatalf("WriteFile /work/result.txt: %v", err)
	}
	got, _ = sb.ReadFile("/work/result.txt")
	if string(got) != "result" {
		t.Fatalf("got %q; want %q", got, "result")
	}

	// Write to /data (ReadOnly) fails.
	err = sb.WriteFile("/data/nope.txt", []byte("x"), 0o644)
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("expected ErrReadOnly for /data write, got %v", err)
	}
}

// ── TestSandbox_Snapshot_Rollback ───────────────────────────────────────────

func TestSandbox_Snapshot_Rollback(t *testing.T) {
	sb, _ := New(EphemeralSandbox())
	defer sb.Close()

	// Write A.
	sb.WriteFile("/a.txt", []byte("A"), 0o644)

	// Snapshot.
	snap := sb.Snapshot()

	// Write B.
	sb.WriteFile("/b.txt", []byte("B"), 0o644)

	// Verify B exists.
	if _, err := sb.ReadFile("/b.txt"); err != nil {
		t.Fatal("b.txt should exist before rollback")
	}

	// Rollback.
	sb.Restore(snap)

	// B should be gone.
	if _, err := sb.ReadFile("/b.txt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("b.txt should not exist after rollback")
	}

	// A should still be present.
	got, err := sb.ReadFile("/a.txt")
	if err != nil {
		t.Fatal("a.txt should still exist after rollback")
	}
	if string(got) != "A" {
		t.Fatalf("a.txt = %q; want %q", got, "A")
	}
}

// ── TestSandbox_WithTransaction_Success ─────────────────────────────────────

func TestSandbox_WithTransaction_Success(t *testing.T) {
	sb, _ := New(EphemeralSandbox())
	defer sb.Close()

	err := sb.WithTransaction(func() error {
		return sb.WriteFile("/tx.txt", []byte("committed"), 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := sb.ReadFile("/tx.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "committed" {
		t.Fatalf("got %q; want %q", got, "committed")
	}
}

// ── TestSandbox_WithTransaction_Failure ─────────────────────────────────────

func TestSandbox_WithTransaction_Failure(t *testing.T) {
	sb, _ := New(EphemeralSandbox())
	defer sb.Close()

	// Pre-existing state.
	sb.WriteFile("/existing.txt", []byte("before"), 0o644)

	txErr := errors.New("transaction failed")
	err := sb.WithTransaction(func() error {
		sb.WriteFile("/new.txt", []byte("new"), 0o644)
		sb.WriteFile("/existing.txt", []byte("modified"), 0o644)
		return txErr
	})
	if !errors.Is(err, txErr) {
		t.Fatalf("expected txErr, got %v", err)
	}

	// new.txt should not exist.
	if _, err := sb.ReadFile("/new.txt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("new.txt should not exist after rollback")
	}

	// existing.txt should be restored.
	got, _ := sb.ReadFile("/existing.txt")
	if string(got) != "before" {
		t.Fatalf("existing.txt = %q; want %q", got, "before")
	}
}

// ── TestSandbox_PathEscape_NoMount ──────────────────────────────────────────

func TestSandbox_PathEscape_NoMount(t *testing.T) {
	// Sandbox with only /work mount.
	sb, _ := New(Config{Mounts: []Mount{
		{Guest: "/work", Mode: Ephemeral},
	}})
	defer sb.Close()

	// /data is not covered by any mount.
	_, err := sb.ReadFile("/data/something.txt")
	if !errors.Is(err, ErrPathEscape) {
		t.Fatalf("expected ErrPathEscape for unmounted path, got %v", err)
	}
}

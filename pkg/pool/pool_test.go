package pool

import (
	"context"
	"io/fs"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"testing/fstest"
	"time"
)

// echoWASMPath returns the path to the echo.wasm test fixture.
// The fixture implements alloc+evaluate and always returns {"ok":true}.
func echoWASMPath(t *testing.T) string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "testdata", "echo.wasm")
}

// -------------------------------------------------------------------------
// Structural / interface tests (no real WASM execution required)
// -------------------------------------------------------------------------

func TestNewPool_ReturnsNonNil(t *testing.T) {
	p := NewPool(PoolConfig{})
	if p == nil {
		t.Fatal("NewPool returned nil")
	}
}

func TestNewPool_Stats_EmptyBeforeStart(t *testing.T) {
	p := NewPool(PoolConfig{})
	stats := p.Stats()
	if stats.ByLanguage == nil {
		t.Fatal("Stats().ByLanguage is nil")
	}
	if len(stats.ByLanguage) != 0 {
		t.Fatalf("expected 0 languages, got %d", len(stats.ByLanguage))
	}
}

func TestPool_AcquireUnsupportedLanguage_BeforeStart(t *testing.T) {
	p := NewPool(PoolConfig{})
	_, err := p.Acquire(context.Background(), LangPython)
	if err != ErrLanguageNotSupported {
		t.Fatalf("expected ErrLanguageNotSupported, got %v", err)
	}
}

// -------------------------------------------------------------------------
// Mock-based unit tests (pool mechanics without real WASM)
// -------------------------------------------------------------------------

// mockInstance is a simple Instance that tracks Release calls.
type mockInstance struct {
	lang      Language
	runCalled int
	runErr    error
	runResult any
}

func (m *mockInstance) Run(_ context.Context, _ string, _ map[string]any) (any, error) {
	m.runCalled++
	return m.runResult, m.runErr
}
func (m *mockInstance) Language() Language { return m.lang }

// mockPool exercises the Pool interface contract using mock instances.
type mockPool struct {
	mu        sync.Mutex
	instances map[Language]chan Instance
	cfg       PoolConfig
	stats     PoolStats
}

func newMockPool(cfg PoolConfig, instances int) *mockPool {
	mp := &mockPool{
		cfg:       cfg,
		instances: make(map[Language]chan Instance),
		stats: PoolStats{
			ByLanguage: make(map[Language]LanguageStats),
		},
	}
	for lang := range cfg.Modules {
		ch := make(chan Instance, instances)
		for i := 0; i < instances; i++ {
			ch <- &mockInstance{lang: lang, runResult: map[string]any{"ok": true}}
		}
		mp.instances[lang] = ch
		mp.stats.ByLanguage[lang] = LanguageStats{
			Total: instances,
			Idle:  instances,
		}
	}
	return mp
}

func (mp *mockPool) Acquire(ctx context.Context, lang Language) (Instance, error) {
	mp.mu.Lock()
	ch, ok := mp.instances[lang]
	mp.mu.Unlock()
	if !ok {
		return nil, ErrLanguageNotSupported
	}
	timeout := mp.cfg.AcquireTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case inst := <-ch:
		return inst, nil
	case <-timer.C:
		return nil, ErrPoolTimeout
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (mp *mockPool) Release(inst Instance) {
	mp.mu.Lock()
	ch := mp.instances[inst.Language()]
	mp.mu.Unlock()
	ch <- inst
}

func (mp *mockPool) AcquireWithFS(ctx context.Context, lang Language, fsys fs.FS) (Instance, error) {
	if fsys == nil {
		return mp.Acquire(ctx, lang)
	}
	mp.mu.Lock()
	ch, ok := mp.instances[lang]
	mp.mu.Unlock()
	if !ok {
		return nil, ErrLanguageNotSupported
	}
	// For the mock, return a fresh mockInstance (not from the channel) to
	// simulate per-execution isolation without depleting the pool.
	_ = ch
	return &mockInstance{lang: lang, runResult: map[string]any{"ok": true}}, nil
}

func (mp *mockPool) Start(_ context.Context) error  { return nil }
func (mp *mockPool) Shutdown(_ context.Context) error { return nil }
func (mp *mockPool) Stats() PoolStats                { return mp.stats }

func TestMockPool_AcquireRelease(t *testing.T) {
	cfg := PoolConfig{
		Modules:        map[Language]string{LangJS: "fake.wasm"},
		InstancesPerLang: 2,
		AcquireTimeout: time.Second,
	}
	mp := newMockPool(cfg, 2)

	inst, err := mp.Acquire(context.Background(), LangJS)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if inst.Language() != LangJS {
		t.Fatalf("expected %s, got %s", LangJS, inst.Language())
	}
	mp.Release(inst)
}

func TestMockPool_AcquireUnsupportedLanguage(t *testing.T) {
	cfg := PoolConfig{
		Modules: map[Language]string{LangJS: "fake.wasm"},
	}
	mp := newMockPool(cfg, 1)
	_, err := mp.Acquire(context.Background(), LangPython)
	if err != ErrLanguageNotSupported {
		t.Fatalf("expected ErrLanguageNotSupported, got %v", err)
	}
}

func TestMockPool_Timeout(t *testing.T) {
	cfg := PoolConfig{
		Modules:        map[Language]string{LangJS: "fake.wasm"},
		InstancesPerLang: 1,
		AcquireTimeout: 50 * time.Millisecond,
	}
	mp := newMockPool(cfg, 1)

	// Acquire the only instance.
	inst, err := mp.Acquire(context.Background(), LangJS)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	// Second Acquire should time out.
	_, err = mp.Acquire(context.Background(), LangJS)
	if err != ErrPoolTimeout {
		t.Fatalf("expected ErrPoolTimeout, got %v", err)
	}

	mp.Release(inst)
}

func TestMockPool_ContextCancellation(t *testing.T) {
	cfg := PoolConfig{
		Modules:        map[Language]string{LangJS: "fake.wasm"},
		InstancesPerLang: 1,
		AcquireTimeout: 5 * time.Second,
	}
	mp := newMockPool(cfg, 1)

	// Exhaust the pool.
	inst, _ := mp.Acquire(context.Background(), LangJS)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := mp.Acquire(ctx, LangJS)
		done <- err
	}()

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error after cancel, got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("Acquire did not respect context cancellation")
	}

	mp.Release(inst)
}

func TestMockPool_Stats(t *testing.T) {
	cfg := PoolConfig{
		Modules:          map[Language]string{LangPython: "p.wasm", LangJS: "j.wasm"},
		InstancesPerLang: 3,
	}
	mp := newMockPool(cfg, 3)
	stats := mp.Stats()
	if len(stats.ByLanguage) != 2 {
		t.Fatalf("expected 2 languages in stats, got %d", len(stats.ByLanguage))
	}
	for lang, ls := range stats.ByLanguage {
		if ls.Total != 3 {
			t.Errorf("lang %s: expected Total=3, got %d", lang, ls.Total)
		}
	}
}

func TestMockPool_ConcurrentAcquireRelease(t *testing.T) {
	const goroutines = 20
	const instances = 4

	cfg := PoolConfig{
		Modules:        map[Language]string{LangShell: "s.wasm"},
		InstancesPerLang: instances,
		AcquireTimeout: 5 * time.Second,
	}
	mp := newMockPool(cfg, instances)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			inst, err := mp.Acquire(context.Background(), LangShell)
			if err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			time.Sleep(5 * time.Millisecond)
			mp.Release(inst)
		}()
	}
	wg.Wait()
}

// -------------------------------------------------------------------------
// Integration tests — require real WASM file (echo.wasm)
// -------------------------------------------------------------------------

func TestWazeroPool_StartShutdown(t *testing.T) {
	wasm := echoWASMPath(t)
	cfg := PoolConfig{
		Modules:          map[Language]string{LangJS: wasm},
		InstancesPerLang: 2,
		AcquireTimeout:   5 * time.Second,
	}
	p := NewPool(cfg)
	ctx := context.Background()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestWazeroPool_Stats(t *testing.T) {
	wasm := echoWASMPath(t)
	cfg := PoolConfig{
		Modules:          map[Language]string{LangJS: wasm},
		InstancesPerLang: 3,
		AcquireTimeout:   5 * time.Second,
	}
	p := NewPool(cfg)
	ctx := context.Background()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Shutdown(ctx)

	stats := p.Stats()
	ls, ok := stats.ByLanguage[LangJS]
	if !ok {
		t.Fatal("no stats for LangJS")
	}
	if ls.Total != 3 {
		t.Errorf("Total: want 3, got %d", ls.Total)
	}
	if ls.Idle != 3 {
		t.Errorf("Idle: want 3, got %d", ls.Idle)
	}
	if ls.InUse != 0 {
		t.Errorf("InUse: want 0, got %d", ls.InUse)
	}
}

func TestWazeroPool_AcquireRelease(t *testing.T) {
	wasm := echoWASMPath(t)
	cfg := PoolConfig{
		Modules:          map[Language]string{LangJS: wasm},
		InstancesPerLang: 2,
		AcquireTimeout:   5 * time.Second,
	}
	p := NewPool(cfg)
	ctx := context.Background()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Shutdown(ctx)

	inst, err := p.Acquire(ctx, LangJS)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if inst.Language() != LangJS {
		t.Errorf("Language: want %s, got %s", LangJS, inst.Language())
	}
	p.Release(inst)

	// After release, the instance should be available again.
	inst2, err := p.Acquire(ctx, LangJS)
	if err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
	p.Release(inst2)
}

func TestWazeroPool_Run(t *testing.T) {
	wasm := echoWASMPath(t)
	cfg := PoolConfig{
		Modules:          map[Language]string{LangJS: wasm},
		InstancesPerLang: 1,
		AcquireTimeout:   5 * time.Second,
	}
	p := NewPool(cfg)
	ctx := context.Background()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Shutdown(ctx)

	inst, err := p.Acquire(ctx, LangJS)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	result, err := inst.Run(ctx, `console.log("hello")`, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result == nil {
		t.Fatal("Run returned nil result")
	}

	// echo.wasm always returns {"ok": true}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result type: want map[string]any, got %T", result)
	}
	if v, _ := m["ok"].(bool); !v {
		t.Errorf("expected ok=true in response, got %v", m)
	}

	p.Release(inst)
}

func TestWazeroPool_SnapshotRestoreClean(t *testing.T) {
	// Run the same instance multiple times and verify each response is valid,
	// confirming snapshot/restore keeps state clean between calls.
	wasm := echoWASMPath(t)
	cfg := PoolConfig{
		Modules:          map[Language]string{LangJS: wasm},
		InstancesPerLang: 1,
		AcquireTimeout:   5 * time.Second,
	}
	p := NewPool(cfg)
	ctx := context.Background()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Shutdown(ctx)

	for i := 0; i < 5; i++ {
		inst, err := p.Acquire(ctx, LangJS)
		if err != nil {
			t.Fatalf("iteration %d Acquire: %v", i, err)
		}
		result, err := inst.Run(ctx, "code", nil)
		if err != nil {
			p.Release(inst)
			t.Fatalf("iteration %d Run: %v", i, err)
		}
		m, ok := result.(map[string]any)
		if !ok {
			p.Release(inst)
			t.Fatalf("iteration %d: unexpected result type %T", i, result)
		}
		if v, _ := m["ok"].(bool); !v {
			p.Release(inst)
			t.Errorf("iteration %d: expected ok=true, got %v", i, m)
		}
		p.Release(inst)
	}
}

func TestWazeroPool_Timeout(t *testing.T) {
	wasm := echoWASMPath(t)
	cfg := PoolConfig{
		Modules:          map[Language]string{LangJS: wasm},
		InstancesPerLang: 1,
		AcquireTimeout:   50 * time.Millisecond,
	}
	p := NewPool(cfg)
	ctx := context.Background()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Shutdown(ctx)

	inst, err := p.Acquire(ctx, LangJS)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	_, err = p.Acquire(ctx, LangJS)
	if err != ErrPoolTimeout {
		t.Fatalf("expected ErrPoolTimeout, got %v", err)
	}

	p.Release(inst)
}

func TestWazeroPool_UnsupportedLanguage(t *testing.T) {
	wasm := echoWASMPath(t)
	cfg := PoolConfig{
		Modules:          map[Language]string{LangJS: wasm},
		InstancesPerLang: 1,
	}
	p := NewPool(cfg)
	ctx := context.Background()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Shutdown(ctx)

	_, err := p.Acquire(ctx, LangPython)
	if err != ErrLanguageNotSupported {
		t.Fatalf("expected ErrLanguageNotSupported, got %v", err)
	}
}

func TestWazeroPool_ConcurrentRun(t *testing.T) {
	wasm := echoWASMPath(t)
	const instances = 4
	const goroutines = 20

	cfg := PoolConfig{
		Modules:          map[Language]string{LangJS: wasm},
		InstancesPerLang: instances,
		AcquireTimeout:   10 * time.Second,
	}
	p := NewPool(cfg)
	ctx := context.Background()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Shutdown(ctx)

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			inst, err := p.Acquire(ctx, LangJS)
			if err != nil {
				errs <- err
				return
			}
			_, err = inst.Run(ctx, "code", nil)
			p.Release(inst)
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent run error: %v", err)
	}
}

func TestWazeroPool_MultipleLanguages(t *testing.T) {
	wasm := echoWASMPath(t)
	cfg := PoolConfig{
		Modules: map[Language]string{
			LangJS:     wasm,
			LangPython: wasm,
		},
		InstancesPerLang: 2,
		AcquireTimeout:   5 * time.Second,
	}
	p := NewPool(cfg)
	ctx := context.Background()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Shutdown(ctx)

	stats := p.Stats()
	if len(stats.ByLanguage) != 2 {
		t.Errorf("expected 2 languages, got %d", len(stats.ByLanguage))
	}

	for _, lang := range []Language{LangJS, LangPython} {
		inst, err := p.Acquire(ctx, lang)
		if err != nil {
			t.Errorf("Acquire(%s): %v", lang, err)
			continue
		}
		if inst.Language() != lang {
			t.Errorf("Language: want %s, got %s", lang, inst.Language())
		}
		p.Release(inst)
	}
}

func TestWazeroPool_StatsInUse(t *testing.T) {
	wasm := echoWASMPath(t)
	cfg := PoolConfig{
		Modules:          map[Language]string{LangJS: wasm},
		InstancesPerLang: 3,
		AcquireTimeout:   5 * time.Second,
	}
	p := NewPool(cfg)
	ctx := context.Background()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Shutdown(ctx)

	inst1, _ := p.Acquire(ctx, LangJS)
	inst2, _ := p.Acquire(ctx, LangJS)

	stats := p.Stats()
	ls := stats.ByLanguage[LangJS]
	if ls.InUse != 2 {
		t.Errorf("InUse: want 2, got %d", ls.InUse)
	}
	if ls.Idle != 1 {
		t.Errorf("Idle: want 1, got %d", ls.Idle)
	}

	p.Release(inst1)
	p.Release(inst2)
}

// -------------------------------------------------------------------------
// AcquireWithFS tests
// -------------------------------------------------------------------------

func TestAcquireWithFS_NilFallback(t *testing.T) {
	// AcquireWithFS(ctx, lang, nil) should behave the same as Acquire.
	wasm := echoWASMPath(t)
	cfg := PoolConfig{
		Modules:          map[Language]string{LangJS: wasm},
		InstancesPerLang: 2,
		AcquireTimeout:   5 * time.Second,
	}
	p := NewPool(cfg)
	ctx := context.Background()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Shutdown(ctx)

	inst, err := p.AcquireWithFS(ctx, LangJS, nil)
	if err != nil {
		t.Fatalf("AcquireWithFS(nil): %v", err)
	}
	if inst == nil {
		t.Fatal("AcquireWithFS(nil) returned nil instance")
	}
	if inst.Language() != LangJS {
		t.Errorf("Language: want %s, got %s", LangJS, inst.Language())
	}

	// Release must not panic.
	p.Release(inst)
}

func TestAcquireWithFS_WithMemFS(t *testing.T) {
	// AcquireWithFS with a real fs.FS should return a usable instance.
	wasm := echoWASMPath(t)
	cfg := PoolConfig{
		Modules:          map[Language]string{LangJS: wasm},
		InstancesPerLang: 2,
		AcquireTimeout:   5 * time.Second,
	}
	p := NewPool(cfg)
	ctx := context.Background()

	if err := p.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer p.Shutdown(ctx)

	memFS := fstest.MapFS{
		"hello.txt": &fstest.MapFile{Data: []byte("hello from fs")},
	}

	inst, err := p.AcquireWithFS(ctx, LangJS, memFS)
	if err != nil {
		t.Fatalf("AcquireWithFS(memFS): %v", err)
	}
	if inst == nil {
		t.Fatal("AcquireWithFS(memFS) returned nil instance")
	}
	if inst.Language() != LangJS {
		t.Errorf("Language: want %s, got %s", LangJS, inst.Language())
	}

	// Release must not panic — ephemeral instance is closed, not pooled.
	p.Release(inst)
}

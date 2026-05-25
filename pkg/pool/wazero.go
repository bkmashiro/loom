package pool

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// wazeroPool implements Pool using wazero as the WASM runtime.
//
// Architecture:
//   - One wazero.Runtime shared across all languages and instances.
//   - One compiled module per language (compile once at Start).
//   - Per-language buffered channel of *wasmInstance (the pool).
//   - Acquire: receive from channel (blocks up to AcquireTimeout).
//   - Release: restore snapshot, send back to channel (or spawn replacement on unhealthy).
type wazeroPool struct {
	cfg PoolConfig

	rt      wazero.Runtime
	mu      sync.RWMutex // protects pools, compiled, started
	started bool

	// compiled holds the compiled module for each language.
	compiled map[Language]wazero.CompiledModule

	// pools holds the channel of idle instances for each language.
	pools map[Language]chan *wasmInstance

	// inUse tracks how many instances are currently held by callers.
	inUse map[Language]*atomic.Int64

	// waitCount tracks cumulative Acquire waits that blocked.
	waitCount map[Language]*atomic.Int64
}

func newWazeroPool(cfg PoolConfig) *wazeroPool {
	if cfg.AcquireTimeout <= 0 {
		cfg.AcquireTimeout = 5 * time.Second
	}
	if cfg.InstancesPerLang <= 0 {
		cfg.InstancesPerLang = runtime.NumCPU()
	}
	return &wazeroPool{
		cfg:       cfg,
		compiled:  make(map[Language]wazero.CompiledModule),
		pools:     make(map[Language]chan *wasmInstance),
		inUse:     make(map[Language]*atomic.Int64),
		waitCount: make(map[Language]*atomic.Int64),
	}
}

// Start compiles WASM modules and warms up the instance pools.
func (p *wazeroPool) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return nil
	}

	rtCfg := wazero.NewRuntimeConfig().WithCloseOnContextDone(true)
	p.rt = wazero.NewRuntimeWithConfig(ctx, rtCfg)

	// Register WASI host functions (needed by TinyGo and many C modules).
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, p.rt); err != nil {
		_ = p.rt.Close(ctx)
		return fmt.Errorf("pool: instantiate wasi: %w", err)
	}

	n := p.cfg.InstancesPerLang

	for lang, modPath := range p.cfg.Modules {
		wasmBytes, err := os.ReadFile(modPath)
		if err != nil {
			_ = p.rt.Close(ctx)
			return fmt.Errorf("pool: read module %q for lang %q: %w", modPath, lang, err)
		}

		compiled, err := p.rt.CompileModule(ctx, wasmBytes)
		if err != nil {
			_ = p.rt.Close(ctx)
			return fmt.Errorf("pool: compile module for lang %q: %w", lang, err)
		}
		p.compiled[lang] = compiled

		modCfg := wazero.NewModuleConfig().
			WithName("").
			WithSysNanosleep().
			WithSysWalltime().
			WithSysNanotime()

		ch := make(chan *wasmInstance, n)
		p.pools[lang] = ch
		p.inUse[lang] = &atomic.Int64{}
		p.waitCount[lang] = &atomic.Int64{}

		for i := 0; i < n; i++ {
			inst, err := p.spawnInstance(ctx, lang, compiled, modCfg)
			if err != nil {
				// Drain what we already put in.
				p.drainLang(ctx, ch)
				_ = p.rt.Close(ctx)
				return fmt.Errorf("pool: start instance %d for lang %q: %w", i, lang, err)
			}
			ch <- inst
		}

		slog.Info("pool: language ready", "lang", lang, "instances", n)
	}

	p.started = true
	return nil
}

// Acquire waits for an idle instance for the given language.
func (p *wazeroPool) Acquire(ctx context.Context, lang Language) (Instance, error) {
	p.mu.RLock()
	ch, ok := p.pools[lang]
	inUseCtr := p.inUse[lang]
	waitCtr := p.waitCount[lang]
	p.mu.RUnlock()

	if !ok {
		return nil, ErrLanguageNotSupported
	}

	timeout := p.cfg.AcquireTimeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	// Try non-blocking first.
	select {
	case inst := <-ch:
		inUseCtr.Add(1)
		return inst, nil
	default:
	}

	// Block with timeout.
	waitCtr.Add(1)
	select {
	case inst := <-ch:
		inUseCtr.Add(1)
		return inst, nil
	case <-timer.C:
		return nil, ErrPoolTimeout
	case <-ctx.Done():
		return nil, fmt.Errorf("pool: acquire: %w", ctx.Err())
	}
}

// Release restores the instance's snapshot and returns it to the pool.
// If the instance is unhealthy, it is discarded and a replacement is spawned.
func (p *wazeroPool) Release(inst Instance) {
	wi, ok := inst.(*wasmInstance)
	if !ok {
		return
	}

	p.mu.RLock()
	ch := p.pools[wi.lang]
	compiled := p.compiled[wi.lang]
	inUseCtr := p.inUse[wi.lang]
	p.mu.RUnlock()

	inUseCtr.Add(-1)

	if wi.IsHealthy() {
		ch <- wi
		return
	}

	// Instance is unhealthy — discard and spawn a replacement asynchronously.
	slog.Warn("pool: unhealthy instance discarded, spawning replacement", "lang", wi.lang)
	go func() {
		_ = wi.shutdown(context.Background())
	}()
	go p.spawnReplacement(wi.lang, compiled, ch)
}

// Stats returns a snapshot of pool utilisation.
func (p *wazeroPool) Stats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := PoolStats{ByLanguage: make(map[Language]LanguageStats)}
	for lang, ch := range p.pools {
		total := p.cfg.InstancesPerLang
		idle := len(ch)
		inUse := int(p.inUse[lang].Load())
		wc := int(p.waitCount[lang].Load())
		stats.ByLanguage[lang] = LanguageStats{
			Total:     total,
			Idle:      idle,
			InUse:     inUse,
			WaitCount: wc,
		}
	}
	return stats
}

// Shutdown drains all instances and closes the wazero runtime.
func (p *wazeroPool) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for lang, ch := range p.pools {
		p.drainLang(ctx, ch)
		slog.Info("pool: language drained", "lang", lang)
	}

	if p.rt != nil {
		if err := p.rt.Close(ctx); err != nil {
			return fmt.Errorf("pool: close runtime: %w", err)
		}
		p.rt = nil
	}

	p.started = false
	return nil
}

// spawnInstance creates and initialises a single wasmInstance.
func (p *wazeroPool) spawnInstance(
	ctx context.Context,
	lang Language,
	compiled wazero.CompiledModule,
	modCfg wazero.ModuleConfig,
) (*wasmInstance, error) {
	instCfg := modCfg.WithStartFunctions("_initialize", "_start")

	mod, err := p.rt.InstantiateModule(ctx, compiled, instCfg)
	if err != nil {
		return nil, fmt.Errorf("instantiate: %w", err)
	}

	inst := &wasmInstance{
		lang:    lang,
		mod:     mod,
		allocFn: mod.ExportedFunction("alloc"),
		evalFn:  mod.ExportedFunction("evaluate"),
		healthy: true,
		pool:    p,
	}

	if err := inst.takeSnapshot(); err != nil {
		_ = mod.Close(ctx)
		return nil, fmt.Errorf("snapshot: %w", err)
	}

	return inst, nil
}

// spawnReplacement creates a fresh instance and puts it back in ch.
// Called in a goroutine when an unhealthy instance is discarded.
func (p *wazeroPool) spawnReplacement(lang Language, compiled wazero.CompiledModule, ch chan *wasmInstance) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	modCfg := wazero.NewModuleConfig().
		WithName("").
		WithSysNanosleep().
		WithSysWalltime().
		WithSysNanotime()

	inst, err := p.spawnInstance(ctx, lang, compiled, modCfg)
	if err != nil {
		slog.Error("pool: replacement instance failed", "lang", lang, "err", err)
		return
	}
	ch <- inst
	slog.Info("pool: replacement instance ready", "lang", lang)
}

// drainLang closes all instances currently in ch and empties the channel.
// Must be called with p.mu held (write lock).
func (p *wazeroPool) drainLang(ctx context.Context, ch chan *wasmInstance) {
	for {
		select {
		case inst := <-ch:
			_ = inst.shutdown(ctx)
		default:
			return
		}
	}
}

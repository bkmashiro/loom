package primitives

import "sync"

// KVStore is a simple key-value store interface.
type KVStore interface {
	Get(key string) (any, bool)
	Set(key string, value any)
	Del(key string)
}

type memoryKV struct {
	data map[string]any
	mu   sync.RWMutex
}

// NewMemoryKV creates a new in-memory KV store.
func NewMemoryKV() KVStore {
	return &memoryKV{
		data: make(map[string]any),
	}
}

func (m *memoryKV) Get(key string) (any, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[key]
	return v, ok
}

func (m *memoryKV) Set(key string, value any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

func (m *memoryKV) Del(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

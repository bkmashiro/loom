package exec

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

// IOCache is a thread-safe LRU cache with TTL for IO step results.
type IOCache struct {
	mu    sync.Mutex
	cap   int
	ttl   time.Duration
	items map[string]*list.Element
	lru   *list.List
}

type cacheEntry struct {
	key       string
	value     any
	expiresAt time.Time
}

// NewIOCache creates an IOCache with the given capacity and TTL.
func NewIOCache(capacity int, ttl time.Duration) *IOCache {
	return &IOCache{
		cap:   capacity,
		ttl:   ttl,
		items: make(map[string]*list.Element),
		lru:   list.New(),
	}
}

// Get returns (value, true) if key exists and hasn't expired.
func (c *IOCache) Get(key string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		return nil, false
	}

	entry := el.Value.(*cacheEntry)
	if time.Now().After(entry.expiresAt) {
		// Expired — remove and return miss.
		c.lru.Remove(el)
		delete(c.items, key)
		return nil, false
	}

	// Move to front (most recently used).
	c.lru.MoveToFront(el)
	return entry.value, true
}

// Set stores value under key, evicting the LRU item if at capacity.
func (c *IOCache) Set(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If key already exists, update and move to front.
	if el, ok := c.items[key]; ok {
		entry := el.Value.(*cacheEntry)
		entry.value = value
		entry.expiresAt = time.Now().Add(c.ttl)
		c.lru.MoveToFront(el)
		return
	}

	// Evict LRU if at capacity.
	if c.lru.Len() >= c.cap {
		oldest := c.lru.Back()
		if oldest != nil {
			entry := oldest.Value.(*cacheEntry)
			delete(c.items, entry.key)
			c.lru.Remove(oldest)
		}
	}

	entry := &cacheEntry{
		key:       key,
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}
	el := c.lru.PushFront(entry)
	c.items[key] = el
}

// Len returns the number of items currently in the cache.
func (c *IOCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}

// cacheKey returns a deterministic string key for the given method, URL, and body.
func cacheKey(method, url, body string) string {
	return fmt.Sprintf("%s %s\n%s", method, url, body)
}

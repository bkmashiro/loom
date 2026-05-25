package exec

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestIOCache_BasicGetSet verifies store and retrieve.
func TestIOCache_BasicGetSet(t *testing.T) {
	c := NewIOCache(10, time.Minute)

	c.Set("key1", "value1")
	c.Set("key2", 42)

	v, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected key1 to be present")
	}
	if v != "value1" {
		t.Errorf("expected 'value1', got %v", v)
	}

	v, ok = c.Get("key2")
	if !ok {
		t.Fatal("expected key2 to be present")
	}
	if v != 42 {
		t.Errorf("expected 42, got %v", v)
	}

	_, ok = c.Get("missing")
	if ok {
		t.Error("expected missing key to return false")
	}
}

// TestIOCache_TTLExpiry verifies expired entries are not returned.
func TestIOCache_TTLExpiry(t *testing.T) {
	c := NewIOCache(10, 50*time.Millisecond)
	c.Set("key", "value")

	// Should be present immediately.
	if _, ok := c.Get("key"); !ok {
		t.Fatal("expected key to be present before expiry")
	}

	// Wait for expiry.
	time.Sleep(100 * time.Millisecond)

	_, ok := c.Get("key")
	if ok {
		t.Error("expected expired key to return false")
	}
	if c.Len() != 0 {
		t.Errorf("expected 0 items after expiry, got %d", c.Len())
	}
}

// TestIOCache_LRUEviction verifies the LRU item is evicted when at capacity.
func TestIOCache_LRUEviction(t *testing.T) {
	c := NewIOCache(3, time.Minute)

	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)

	// Access "a" to make it recently used; "b" becomes LRU.
	c.Get("a")

	// Insert "d" — should evict "b" (LRU).
	c.Set("d", 4)

	if c.Len() != 3 {
		t.Errorf("expected 3 items, got %d", c.Len())
	}

	_, ok := c.Get("b")
	if ok {
		t.Error("expected 'b' to be evicted")
	}

	for _, key := range []string{"a", "c", "d"} {
		if _, ok := c.Get(key); !ok {
			t.Errorf("expected %q to still be present", key)
		}
	}
}

// TestIOCache_UpdateExisting verifies updating a key moves it to the front.
func TestIOCache_UpdateExisting(t *testing.T) {
	c := NewIOCache(3, time.Minute)

	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)

	// Update "a" — moves it to front, making "b" LRU.
	c.Set("a", 100)

	// Check value updated.
	v, ok := c.Get("a")
	if !ok {
		t.Fatal("expected 'a' to be present after update")
	}
	if v != 100 {
		t.Errorf("expected 100, got %v", v)
	}

	// Insert "d" — should evict "b" (LRU after "a" was updated).
	c.Set("d", 4)

	_, ok = c.Get("b")
	if ok {
		t.Error("expected 'b' to be evicted (LRU)")
	}

	// "a", "c", and "d" should remain.
	for _, key := range []string{"a", "c", "d"} {
		if _, ok := c.Get(key); !ok {
			t.Errorf("expected %q to still be present", key)
		}
	}
}

// TestIOCache_ConcurrentAccess verifies no data races under parallel Get/Set.
func TestIOCache_ConcurrentAccess(t *testing.T) {
	c := NewIOCache(50, time.Minute)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", n%20)
			c.Set(key, n)
			c.Get(key)
			c.Len()
		}(i)
	}
	wg.Wait()
}

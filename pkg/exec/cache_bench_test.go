package exec

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkIOCache_Hit measures the hot-path: repeated gets for an already-cached key.
func BenchmarkIOCache_Hit(b *testing.B) {
	c := NewIOCache(256, time.Minute)
	c.Set("GET https://api.example.com/users\n", "result")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, ok := c.Get("GET https://api.example.com/users\n")
		if !ok || v == nil {
			b.Fatal("expected cache hit")
		}
	}
}

// BenchmarkIOCache_Miss measures the cold-path: every key is unique (always a miss).
func BenchmarkIOCache_Miss(b *testing.B) {
	c := NewIOCache(256, time.Minute)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(fmt.Sprintf("GET https://api.example.com/item/%d\n", i))
	}
}

// BenchmarkIOCache_Set measures write throughput with LRU eviction.
func BenchmarkIOCache_Set(b *testing.B) {
	c := NewIOCache(128, time.Minute) // smaller cap to trigger evictions

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(fmt.Sprintf("key%d", i), "value")
	}
}

// BenchmarkIOCache_Mixed measures a realistic read-heavy workload (80% hits, 20% new keys)
// against a warm cache.
func BenchmarkIOCache_Mixed(b *testing.B) {
	c := NewIOCache(256, time.Minute)
	// Pre-warm with 64 entries.
	for i := 0; i < 64; i++ {
		c.Set(fmt.Sprintf("key%d", i), "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%5 == 0 {
			// 20%: new key (miss → set)
			c.Set(fmt.Sprintf("new%d", i), "value")
		} else {
			// 80%: hot key (hit)
			c.Get(fmt.Sprintf("key%d", i%64))
		}
	}
}

// BenchmarkIOCache_Parallel measures contention under concurrent access (GOMAXPROCS goroutines).
func BenchmarkIOCache_Parallel(b *testing.B) {
	c := NewIOCache(256, time.Minute)
	for i := 0; i < 64; i++ {
		c.Set(fmt.Sprintf("key%d", i), "value")
	}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%4 == 0 {
				c.Set(fmt.Sprintf("new%d", i), "value")
			} else {
				c.Get(fmt.Sprintf("key%d", i%64))
			}
			i++
		}
	})
}

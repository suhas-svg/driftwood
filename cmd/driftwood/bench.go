package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"
)

// bench runs closed-loop clients against a live cluster: each client sends
// one request, waits for the answer, and sends the next. Every operation,
// reads included, goes through the Raft log, so every result is linearizable.
func bench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	servers := fs.String("servers", defaultServers, "comma-separated server addresses")
	clients := fs.Int("clients", 8, "concurrent clients")
	duration := fs.Duration("duration", 10*time.Second, "how long to run")
	keys := fs.Int("keys", 100, "number of distinct keys")
	valueSize := fs.Int("value", 16, "value size in bytes")
	reads := fs.Float64("reads", 0.5, "fraction of operations that are Gets")
	fs.Parse(args)

	value := strings.Repeat("x", *valueSize)
	deadline := time.Now().Add(*duration)
	latencies := make([][]time.Duration, *clients)

	// Warm up: find the leader before the clock starts.
	newClerk(*servers).Get("bench-warmup")

	var wg sync.WaitGroup
	for c := range *clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ck := newClerk(*servers)
			for time.Now().Before(deadline) {
				key := fmt.Sprintf("bench-%d", rand.IntN(*keys))
				start := time.Now()
				if rand.Float64() < *reads {
					ck.Get(key)
				} else {
					ck.Put(key, value)
				}
				latencies[c] = append(latencies[c], time.Since(start))
			}
		}()
	}
	wg.Wait()

	all := slices.Concat(latencies...)
	if len(all) == 0 {
		return fmt.Errorf("no operations completed")
	}
	slices.Sort(all)
	pct := func(p float64) time.Duration { return all[int(p*float64(len(all)-1))].Round(10 * time.Microsecond) }
	fmt.Printf("%d clients, %v, %d keys, %d-byte values, %.0f%% reads\n",
		*clients, *duration, *keys, *valueSize, *reads*100)
	fmt.Printf("  %d ops, %.0f ops/s\n", len(all), float64(len(all))/duration.Seconds())
	fmt.Printf("  latency p50 %v  p90 %v  p99 %v  max %v\n", pct(0.5), pct(0.9), pct(0.99), all[len(all)-1].Round(10*time.Microsecond))
	return nil
}

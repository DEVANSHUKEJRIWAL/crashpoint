// Command bench is the open-loop load generator for the proxy baseline (P1/P2).
//
// Open-loop means records are scheduled at a fixed rate and each one's latency
// is measured from its SCHEDULED time, not its actual send time. That is what
// avoids coordinated omission: if the system stalls, the backlog shows up as
// latency instead of the load generator quietly slowing down (BENCHMARKS.md §2).
//
// Point -brokers at the broker directly, or at the proxy, to compare. Raw
// per-record latencies (microseconds) are written to -out for committing under
// bench/baseline/.
package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated bootstrap (broker or proxy)")
	topic := flag.String("topic", "bench", "topic to produce to")
	rate := flag.Int("rate", 50000, "records/sec; 0 = saturate (max throughput, P1)")
	size := flag.Int("size", 1024, "record value size in bytes")
	warmup := flag.Duration("warmup", 60*time.Second, "warmup excluded from stats")
	dur := flag.Duration("duration", 3*time.Minute, "measured duration after warmup")
	outPath := flag.String("out", "", "write raw latencies (µs/line) here")
	flag.Parse()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(splitComma(*brokers)...),
		kgo.ClientID("crashpoint-bench"), // harness prefix: never faulted
		kgo.DefaultProduceTopic(*topic),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		os.Exit(1)
	}
	defer cl.Close()

	payload := bytes.Repeat([]byte("x"), *size)
	ctx := context.Background()

	var mu sync.Mutex
	lats := make([]time.Duration, 0, 1<<20) // ponytail: raw slice; swap for an HDR histogram if memory bites
	var errs int

	start := time.Now()
	warmEnd := start.Add(*warmup)
	end := warmEnd.Add(*dur)

	record := func(sched time.Time, r *kgo.Record, err error) {
		now := time.Now()
		if now.Before(warmEnd) {
			return // warmup: not measured
		}
		mu.Lock()
		if err != nil {
			errs++
		} else {
			lats = append(lats, now.Sub(sched))
		}
		mu.Unlock()
	}

	var interval time.Duration
	if *rate > 0 {
		interval = time.Second / time.Duration(*rate)
	}
	for i := 0; time.Now().Before(end); i++ {
		sched := time.Now()
		if *rate > 0 {
			// Scheduled target; measure latency from here regardless of when we
			// actually get to send — the coordinated-omission-safe clock.
			sched = start.Add(time.Duration(i) * interval)
			if d := time.Until(sched); d > 0 {
				time.Sleep(d)
			}
		}
		s := sched
		cl.Produce(ctx, &kgo.Record{Value: payload}, func(r *kgo.Record, err error) { record(s, r, err) })
	}
	cl.Flush(ctx)

	mu.Lock()
	defer mu.Unlock()
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	n := len(lats)
	secs := dur.Seconds()
	recsPerSec := float64(n) / secs
	fmt.Printf("records=%d errors=%d duration=%s\n", n, errs, dur)
	fmt.Printf("throughput: %.0f rec/s  %.1f MB/s\n", recsPerSec, recsPerSec*float64(*size)/1e6)
	fmt.Printf("latency p50=%s p99=%s p99.9=%s max=%s\n",
		percentile(lats, 50), percentile(lats, 99), percentile(lats, 99.9), percentile(lats, 100))

	if *outPath != "" {
		writeRaw(*outPath, lats)
	}
}

// percentile returns the nearest-rank p-th percentile of a sorted slice.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(ceil(p / 100 * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// ceil without importing math for one call.
func ceil(f float64) float64 {
	i := float64(int64(f))
	if f > i {
		return i + 1
	}
	return i
}

func splitComma(s string) []string {
	var out []string
	for _, p := range bytes.Split([]byte(s), []byte(",")) {
		if len(p) > 0 {
			out = append(out, string(p))
		}
	}
	return out
}

func writeRaw(path string, lats []time.Duration) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "out:", err)
		return
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	for _, l := range lats {
		w.WriteString(strconv.FormatInt(l.Microseconds(), 10))
		w.WriteByte('\n')
	}
}

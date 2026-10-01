// Command workload produces the input stream for a trial and writes one JSONL
// record per input (event_id, key, seq, amount, partition, offset, status) to
// stdout or -out. A timeout is recorded as indeterminate, never failed: the
// record may have landed (DESIGN.md §4).
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/DEVANSHUKEJRIWAL/crashpoint/internal/workload"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func isTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kerr.RequestTimedOut)
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka bootstrap")
	topic := flag.String("topic", "payments.requested", "input topic")
	count := flag.Int("count", 1000, "number of events to produce")
	rate := flag.Int("rate", 100, "events per second (0 = unbounded)")
	keys := flag.Int("keys", 50, "number of distinct accounts (partition keys)")
	seed := flag.Int64("seed", time.Now().UnixNano(), "generator seed (same seed = same events)")
	outPath := flag.String("out", "", "write input records as JSONL here (default stdout)")
	timeout := flag.Duration("timeout", 5*time.Second, "per-produce timeout; a timeout is indeterminate")
	flag.Parse()

	out := os.Stdout
	if *outPath != "" {
		f, err := os.Create(*outPath)
		if err != nil {
			log.Error("open out", "err", err)
			os.Exit(1)
		}
		defer f.Close()
		out = f
	}
	w := bufio.NewWriter(out)
	defer w.Flush()
	rec := json.NewEncoder(w)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		// Harness prefix: the proxy classifies this connection as harness and
		// must never fault it, or it would corrupt the ground truth.
		kgo.ClientID("crashpoint-workload"),
	)
	if err != nil {
		log.Error("create kafka client", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	gen := workload.NewGenerator(*seed, *keys)
	var enc workload.PayloadEncoder = workload.JSONEncoder{}

	var tick *time.Ticker
	if *rate > 0 {
		tick = time.NewTicker(time.Second / time.Duration(*rate))
		defer tick.Stop()
	}

	var ack, fail, indet int
	for n := 0; n < *count && ctx.Err() == nil; n++ {
		if tick != nil {
			select {
			case <-tick.C:
			case <-ctx.Done():
			}
		}
		in := gen.Next(n)
		val, err := enc.Encode(in)
		if err != nil {
			log.Error("encode", "event_id", in.EventID, "err", err)
			in.Status = workload.Failed
			fail++
			rec.Encode(in)
			continue
		}

		pctx, cancel := context.WithTimeout(ctx, *timeout)
		// Key sets the partition (same account → same partition), which I3 relies on.
		res := cl.ProduceSync(pctx, &kgo.Record{Topic: *topic, Key: []byte(in.Key), Value: val})
		cancel()

		r, perr := res.First()
		switch {
		case perr == nil:
			in.Status = workload.Acknowledged
			in.Partition, in.Offset = r.Partition, r.Offset
			ack++
		case isTimeout(perr):
			in.Status = workload.Indeterminate // may or may not have landed
			indet++
		default:
			in.Status = workload.Failed
			fail++
		}
		if err := rec.Encode(in); err != nil {
			log.Error("write record", "err", err)
		}
	}

	log.Info("workload done",
		"acknowledged", ack, "failed", fail, "indeterminate", indet, "seed", *seed)
}

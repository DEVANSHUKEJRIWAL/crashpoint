// Reference payments consumer (issues #2, #3).
//
// -mode selects the control consumer or one of three seeded bugs. Control is
// the correct idempotent at-least-once consumer the checker must always pass;
// the bug modes are invisible without faults and only break inside the precise
// protocol windows DESIGN.md §9 enumerates. Only bootstrap.servers changes to
// run through the proxy, so KAFKA_BROKERS is the sole knob the harness rewrites.
package main

import (
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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// writeFn is the per-record effect write. control/bug1/bug2 use the idempotent
// processControl; bug3 uses processNoDedup.
type writeFn func(context.Context, *pgxpool.Pool, string, Payment) (bool, error)

func writerFor(mode string) (writeFn, bool) {
	switch mode {
	case "control", "bug1", "bug2":
		return processControl, true
	case "bug3":
		return processNoDedup, true
	default:
		return nil, false
	}
}

// decode parses and validates one record. ok == false means the record isn't a
// valid input and should be skipped (the workload only emits valid JSON).
func decode(log *slog.Logger, r *kgo.Record) (Payment, bool) {
	var p Payment
	if err := json.Unmarshal(r.Value, &p); err != nil {
		log.Warn("skip unparseable record", "partition", r.Partition, "offset", r.Offset, "err", err)
		return p, false
	}
	if err := p.validate(); err != nil {
		log.Warn("skip invalid payment", "event_id", p.EventID, "partition", r.Partition, "offset", r.Offset, "err", err)
		return p, false
	}
	return p, true
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	mode := flag.String("mode", getenv("MODE", "control"), "control|bug1|bug2|bug3")
	flag.Parse()
	write, ok := writerFor(*mode)
	if !ok {
		log.Error("unknown mode", "mode", *mode)
		os.Exit(2)
	}

	instanceID := getenv("INSTANCE_ID", "")
	if instanceID == "" {
		instanceID, _ = os.Hostname() // unique per container → distinct writer_id (enables T3)
	}
	log = log.With("instance_id", instanceID, "mode", *mode)

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := getenv("KAFKA_TOPIC", "payments.requested")
	group := getenv("KAFKA_GROUP", "payments")
	dsn := getenv("POSTGRES_DSN", "postgres://crashpoint:crashpoint@localhost:55432/crashpoint")

	// Cancelled on SIGINT/SIGTERM. SIGTERM models the `stop` fault (rolling
	// deploy): the in-flight batch's tx fails cleanly and idempotent redelivery
	// makes the restart safe. SIGKILL (the `kill` fault) has no graceful path —
	// that is the whole point of it.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ClientID("payments-consumer/" + instanceID),
	}
	if *mode == "bug1" {
		// bug1: auto-commit stays ON (franz-go default) and handlers run
		// asynchronously, so the offset advances on a timer whether or not the
		// handler has written the effect. Kill after a commit → lost effect.
		opts = append(opts, kgo.AutoCommitInterval(time.Second))
	} else {
		// control/bug2/bug3: we own offset commits. BlockRebalanceOnPoll keeps a
		// rebalance from interleaving mid-batch.
		opts = append(opts, kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Error("create kafka client", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	log.Info("payments consumer started", "brokers", brokers, "topic", topic, "group", group)

	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		fetches.EachError(func(t string, part int32, err error) {
			log.Error("fetch error", "topic", t, "partition", part, "err", err)
		})

		switch *mode {
		case "bug1":
			// Fire-and-forget; auto-commit advances offsets independently.
			// context.Background so a later poll can't cancel an in-flight write.
			fetches.EachRecord(func(r *kgo.Record) {
				p, ok := decode(log, r)
				if !ok {
					return
				}
				go func() {
					if _, err := write(context.Background(), pool, instanceID, p); err != nil {
						log.Error("async process", "event_id", p.EventID, "err", err)
					}
				}()
			})

		case "bug2":
			// Commit the offset BEFORE the DB write. A crash in the gap commits
			// an offset whose effect never became durable → permanent loss.
			fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
				for _, r := range ftp.Records {
					p, ok := decode(log, r)
					if !ok {
						continue
					}
					if err := cl.CommitRecords(ctx, r); err != nil {
						log.Error("commit offset", "err", err)
						break
					}
					if _, err := write(ctx, pool, instanceID, p); err != nil {
						log.Error("process record", "event_id", p.EventID, "err", err)
					}
				}
			})
			cl.AllowRebalance()

		default: // control, bug3: write, then commit the contiguous prefix.
			// Stopping at the first failure (not committing the highest success)
			// is what keeps a transient error from skipping a record — committing
			// the highest completed offset instead is corpus bug 5.
			var toCommit []*kgo.Record
			fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
				for _, r := range ftp.Records {
					p, ok := decode(log, r)
					if !ok {
						toCommit = append(toCommit, r) // advance past a poison record
						continue
					}
					if _, err := write(ctx, pool, instanceID, p); err != nil {
						log.Error("process record", "event_id", p.EventID, "partition", r.Partition, "offset", r.Offset, "err", err)
						break
					}
					toCommit = append(toCommit, r)
				}
			})
			if len(toCommit) > 0 {
				if err := cl.CommitRecords(ctx, toCommit...); err != nil && !errors.Is(err, context.Canceled) {
					log.Error("commit offsets", "err", err)
				}
			}
			cl.AllowRebalance()
		}
	}

	log.Info("shutting down")
}

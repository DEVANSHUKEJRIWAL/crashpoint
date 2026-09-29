// Reference payments consumer, control mode (issue #2).
//
// The correct, idempotent at-least-once consumer that Crashpoint's checker must
// always pass. Only bootstrap.servers changes to run it through the proxy, so
// KAFKA_BROKERS is the sole knob the harness rewrites.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	instanceID := getenv("INSTANCE_ID", "")
	if instanceID == "" {
		instanceID, _ = os.Hostname() // unique per container → distinct writer_id (enables T3)
	}
	log = log.With("instance_id", instanceID)

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := getenv("KAFKA_TOPIC", "payments.requested")
	group := getenv("KAFKA_GROUP", "payments")
	dsn := getenv("POSTGRES_DSN", "postgres://crashpoint:crashpoint@localhost:55432/crashpoint")

	// Cancelled on SIGINT/SIGTERM. SIGTERM models the `stop` fault (rolling
	// deploy): the in-flight batch's tx fails cleanly, its offsets stay
	// uncommitted, and idempotent redelivery makes the restart safe. SIGKILL
	// (the `kill` fault) has no graceful path — that is the whole point of it.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ClientID("payments-consumer/"+instanceID),
		kgo.DisableAutoCommit(),    // we commit offsets ourselves, only after the DB tx
		kgo.BlockRebalanceOnPoll(), // no rebalance mid-batch; we commit before releasing it
	)
	if err != nil {
		log.Error("create kafka client", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	log.Info("payments consumer started (control mode)",
		"brokers", brokers, "topic", topic, "group", group)

	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		fetches.EachError(func(t string, part int32, err error) {
			log.Error("fetch error", "topic", t, "partition", part, "err", err)
		})

		// Commit only the contiguous prefix per partition: stop at the first
		// failure instead of committing the highest success. Committing past a
		// failed offset would lose that record — that is exactly corpus bug 5.
		var toCommit []*kgo.Record
		fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
			for _, r := range ftp.Records {
				var p Payment
				if err := json.Unmarshal(r.Value, &p); err != nil {
					// Not a valid input; wedging the partition on it forever is
					// worse than skipping. The workload only emits valid JSON.
					log.Warn("skip unparseable record", "partition", r.Partition, "offset", r.Offset, "err", err)
					toCommit = append(toCommit, r)
					continue
				}
				if err := p.validate(); err != nil {
					log.Warn("skip invalid payment", "event_id", p.EventID, "partition", r.Partition, "offset", r.Offset, "err", err)
					toCommit = append(toCommit, r)
					continue
				}
				applied, err := processControl(ctx, pool, instanceID, p)
				if err != nil {
					// Leave this record and the rest of the partition uncommitted;
					// they redeliver. Idempotency makes the replay safe.
					log.Error("process record", "event_id", p.EventID, "partition", r.Partition, "offset", r.Offset, "err", err)
					break
				}
				log.Debug("processed", "event_id", p.EventID, "applied", applied, "partition", r.Partition, "offset", r.Offset)
				toCommit = append(toCommit, r)
			}
		})

		if len(toCommit) > 0 {
			if err := cl.CommitRecords(ctx, toCommit...); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("commit offsets", "err", err)
			}
		}
		cl.AllowRebalance() // required with BlockRebalanceOnPoll
	}

	log.Info("shutting down")
}

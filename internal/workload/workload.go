// Package workload generates the input events Crashpoint drives through the
// proxy and lets the caller classify each one's delivery status. The checker
// consumes the resulting Inputs and never needs to know Crashpoint produced
// them (DESIGN.md §4).
package workload

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
)

// Status is the tri-state delivery outcome of a produce attempt. The split
// matters to the checker: a timeout is Indeterminate (the record may or may not
// exist), so it is excluded from loss checks but still counts for duplicates.
type Status string

const (
	Acknowledged  Status = "acknowledged"
	Failed        Status = "failed"
	Indeterminate Status = "indeterminate"
)

// Input is one generated event plus where it landed. Partition, Offset and
// Status are filled in by the caller after the produce attempt.
//
// ponytail: a struct, not an interface. Observed mode (v2) is the second
// producer that would justify an Input interface; one type is enough until then.
type Input struct {
	EventID   string `json:"event_id"`
	Key       string `json:"key"`    // account id as string; also the Kafka partition key
	Seq       int64  `json:"seq"`    // per-key sequence number
	Amount    string `json:"amount"` // exact decimal
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Status    Status `json:"status"`
}

// PayloadEncoder turns an Input into record-value bytes. JSON ships in v1;
// Avro, Protobuf or Schema-Registry framing plug in here (or the SUT uses
// observed mode).
type PayloadEncoder interface {
	Encode(Input) ([]byte, error)
}

// JSONEncoder emits the payments.requested contract the reference consumer
// reads. Amount is a json.Number so it serializes as a bare numeric token
// (10.00, not "10.00"); the consumer's json.Number field rejects a quoted
// string. This is a money path — no float round-trip.
type JSONEncoder struct{}

func (JSONEncoder) Encode(in Input) ([]byte, error) {
	accID, err := strconv.ParseInt(in.Key, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("key %q is not an account id: %w", in.Key, err)
	}
	return json.Marshal(struct {
		EventID   string      `json:"event_id"`
		AccountID int64       `json:"account_id"`
		Seq       int64       `json:"seq"`
		Amount    json.Number `json:"amount"`
	}{in.EventID, accID, in.Seq, json.Number(in.Amount)})
}

// Generator produces a deterministic stream of inputs from a seed: the same
// seed yields the same events, so a failing trial replays (DESIGN.md values
// reproducibility over claimed determinism).
type Generator struct {
	seed int64
	keys int
	rng  *rand.Rand
	seq  map[string]int64
}

func NewGenerator(seed int64, keys int) *Generator {
	if keys < 1 {
		keys = 1
	}
	return &Generator{seed: seed, keys: keys, rng: rand.New(rand.NewSource(seed)), seq: map[string]int64{}}
}

// Next returns the n-th input (0-based). Keys are assigned round-robin so every
// account gets a contiguous per-key seq, which is what I3 (per-key order)
// checks. Status is left zero; the caller sets it from the produce result.
func (g *Generator) Next(n int) Input {
	key := strconv.Itoa(n%g.keys + 1) // account ids are 1-based (account_id > 0)
	g.seq[key]++
	return Input{
		EventID: fmt.Sprintf("%d-%d", g.seed, n),
		Key:     key,
		Seq:     g.seq[key],
		Amount:  fmt.Sprintf("%d.%02d", g.rng.Intn(100)+1, g.rng.Intn(100)),
	}
}

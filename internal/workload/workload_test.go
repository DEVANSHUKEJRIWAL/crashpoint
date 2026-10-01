package workload

import (
	"encoding/json"
	"testing"
)

func TestGeneratorPerKeySeqAndUniqueIDs(t *testing.T) {
	g := NewGenerator(42, 3) // keys "1","2","3"
	seen := map[string]bool{}
	want := map[string]int64{}
	for n := 0; n < 9; n++ {
		in := g.Next(n)
		if seen[in.EventID] {
			t.Fatalf("duplicate event_id %s", in.EventID)
		}
		seen[in.EventID] = true
		want[in.Key]++
		if in.Seq != want[in.Key] {
			t.Fatalf("key %s: want seq %d, got %d", in.Key, want[in.Key], in.Seq)
		}
	}
	for _, k := range []string{"1", "2", "3"} {
		if want[k] != 3 {
			t.Fatalf("key %s: expected 3 events, got %d", k, want[k])
		}
	}
}

// The amount must reach the wire as a JSON number token; the consumer decodes
// it into json.Number, which rejects a quoted string. This guards the money
// contract between the two modules.
func TestJSONEncoderEmitsNumericAmount(t *testing.T) {
	b, err := JSONEncoder{}.Encode(Input{EventID: "e1", Key: "7", Seq: 2, Amount: "10.00"})
	if err != nil {
		t.Fatal(err)
	}
	var probe struct {
		EventID   string      `json:"event_id"`
		AccountID int64       `json:"account_id"`
		Seq       int64       `json:"seq"`
		Amount    json.Number `json:"amount"` // decode fails here if amount is a string
	}
	if err := json.Unmarshal(b, &probe); err != nil {
		t.Fatalf("consumer-side decode failed: %v (payload: %s)", err, b)
	}
	if probe.AccountID != 7 || probe.Seq != 2 || probe.EventID != "e1" {
		t.Fatalf("round-trip mismatch: %+v", probe)
	}
	if probe.Amount.String() != "10.00" {
		t.Fatalf("amount: want 10.00, got %s", probe.Amount)
	}
}

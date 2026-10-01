package proxy

import (
	"encoding/json"
	"io"
	"sync"
)

// Record is one frame crossing the proxy. In Week 1 these are written as JSON
// Lines so a trial is greppable with jq; a binary format replaces it in Week 2
// (ARCHITECTURE §6).
type Record struct {
	TimeUnixNano int64  `json:"t"`
	Conn         string `json:"conn"`
	Listener     string `json:"listener"`
	Dir          string `json:"dir"` // "req" or "resp"
	API          int16  `json:"api"`
	APIName      string `json:"api_name"`
	Version      int16  `json:"version"`
	Correlation  int32  `json:"corr"`
	Size         int    `json:"size"`
	// LatencyMicros is set on responses: time since the matching request was
	// forwarded. Omitted for requests (and responses with no matched request).
	LatencyMicros int64 `json:"latency_us,omitempty"`
}

// Recorder serializes frame records to a writer. A nil *Recorder is the
// switched-off state: Emit is a no-op, so benchmark baselines pay nothing
// (ARCHITECTURE §2.4). One mutex-guarded writer, so concurrent connections
// don't interleave partial lines.
type Recorder struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func NewRecorder(w io.Writer) *Recorder {
	return &Recorder{enc: json.NewEncoder(w)}
}

func (r *Recorder) Emit(rec Record) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.enc.Encode(rec) // JSON of fixed-type fields cannot fail; ignore err
	r.mu.Unlock()
}

package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestRecorderCapturesFramesAndLatency(t *testing.T) {
	clientEnd, proxyClient := net.Pipe()
	proxyBroker, brokerEnd := net.Pipe()

	var buf bytes.Buffer
	c := NewConn(proxyClient, proxyBroker, 0, WithRecorder(NewRecorder(&buf), "c1", "bootstrap"))

	done := make(chan error, 1)
	go func() { done <- c.Run(context.Background()) }()

	deadline := time.Now().Add(5 * time.Second)
	clientEnd.SetDeadline(deadline)
	brokerEnd.SetDeadline(deadline)

	// Request flows client → broker and is recorded.
	go clientEnd.Write(reqWire(kmsg.NewPtrMetadataRequest(), 9, 4242))
	if _, err := ReadFrame(brokerEnd, DefaultMaxFrameBytes); err != nil {
		t.Fatalf("broker read: %v", err)
	}
	// Delay so the response shows a measurable latency, then send it back.
	time.Sleep(3 * time.Millisecond)
	go brokerEnd.Write(respWire(4242))
	if _, err := ReadFrame(clientEnd, DefaultMaxFrameBytes); err != nil {
		t.Fatalf("client read: %v", err)
	}

	clientEnd.Close()
	brokerEnd.Close()
	<-done // no more Emits after Run returns; safe to read the buffer

	var recs []Record
	sc := bufio.NewScanner(&buf)
	for sc.Scan() {
		var r Record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("bad JSONL line %q: %v", sc.Text(), err)
		}
		recs = append(recs, r)
	}
	if len(recs) != 2 {
		t.Fatalf("want 2 records (req, resp), got %d: %+v", len(recs), recs)
	}

	req := recs[0]
	if req.Dir != "req" || req.API != 3 || req.APIName != "Metadata" || req.Version != 9 || req.Correlation != 4242 {
		t.Fatalf("request record wrong: %+v", req)
	}
	if req.Conn != "c1" || req.Listener != "bootstrap" || req.Size <= 0 || req.TimeUnixNano == 0 {
		t.Fatalf("request record metadata wrong: %+v", req)
	}

	resp := recs[1]
	if resp.Dir != "resp" || resp.Correlation != 4242 {
		t.Fatalf("response record wrong: %+v", resp)
	}
	// api key/version are carried from the matched request, proving the join.
	if resp.API != 3 || resp.Version != 9 {
		t.Fatalf("response not matched to its request: %+v", resp)
	}
	if resp.LatencyMicros <= 0 {
		t.Fatalf("response latency should be positive, got %d", resp.LatencyMicros)
	}
}

func TestNilRecorderIsNoop(t *testing.T) {
	var r *Recorder
	r.Emit(Record{API: 1}) // must not panic
}

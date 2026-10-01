package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// End-to-end: a real franz-go client bootstraps through the proxy to an
// in-process kfake broker, produces and consumes records, and never talks to
// the broker directly. This closes the integration gap for #6 (address
// rewriting / no bypass / works with franz-go) and #7 (a real client is
// classified sut) without needing Docker.
func TestEndToEndThroughProxy(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, "e2e"))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()
	seed := cluster.ListenAddrs()[0]

	// Recording classifier so we can assert the client is tagged sut.
	var clsMu sync.Mutex
	seenClass := map[string]Class{}
	classifier := func(id string) Class {
		c := PrefixClassifier(DefaultHarnessPrefix)(id)
		clsMu.Lock()
		seenClass[id] = c
		clsMu.Unlock()
		return c
	}

	var recBuf bytes.Buffer
	srv := &Server{AdvHost: "127.0.0.1", Recorder: NewRecorder(&recBuf), Classifier: classifier}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxyAddr := ln.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx, ln, seed) }()

	const n = 20
	produce(t, proxyAddr, "e2e", n)
	got := consume(t, proxyAddr, "e2e", n)

	cancel()
	<-served // all connections drained; safe to read the recorder buffer

	if got != n {
		t.Fatalf("round-trip through proxy: produced %d, consumed %d", n, got)
	}

	// No bypass: Produce (api 0) and Fetch (api 1) must have reached node
	// listeners, which only happens if the client used rewritten addresses.
	var sawProduceOnNode, sawFetchOnNode bool
	sc := bufio.NewScanner(&recBuf)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		var r Record
		if json.Unmarshal(sc.Bytes(), &r) != nil {
			continue
		}
		if r.Dir == "req" && strings.HasPrefix(r.Listener, "node-") {
			switch r.API {
			case 0:
				sawProduceOnNode = true
			case 1:
				sawFetchOnNode = true
			}
		}
	}
	if !sawProduceOnNode {
		t.Error("no Produce frame reached a node listener — client may have bypassed the proxy")
	}
	if !sawFetchOnNode {
		t.Error("no Fetch frame reached a node listener — client may have bypassed the proxy")
	}
	if len(srv.nodes) == 0 {
		t.Error("proxy created no node listeners; Metadata was not rewritten")
	}

	// #7: the real client connections are classified sut, none harness.
	clsMu.Lock()
	defer clsMu.Unlock()
	if len(seenClass) == 0 {
		t.Fatal("classifier never ran")
	}
	for id, c := range seenClass {
		if c != ClassSUT {
			t.Errorf("client %q classified %s, want sut", id, c)
		}
	}
}

func produce(t *testing.T, addr, topic string, n int) {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ClientID("e2e-producer"),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		if res := cl.ProduceSync(ctx, &kgo.Record{Value: []byte("msg")}); res.FirstErr() != nil {
			t.Fatalf("produce %d: %v", i, res.FirstErr())
		}
	}
}

func consume(t *testing.T, addr, topic string, want int) int {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.ClientID("e2e-consumer"),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	got := 0
	for got < want && ctx.Err() == nil {
		fs := cl.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 && got == 0 && ctx.Err() != nil {
			t.Fatalf("fetch: %v", errs)
		}
		fs.EachRecord(func(*kgo.Record) { got++ })
	}
	return got
}

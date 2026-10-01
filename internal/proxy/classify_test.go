package proxy

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestPrefixClassifier(t *testing.T) {
	cl := PrefixClassifier(DefaultHarnessPrefix)
	cases := map[string]Class{
		"crashpoint-workload":  ClassHarness,
		"crashpoint-anything":  ClassHarness,
		"payments-consumer/42": ClassSUT,
		"franz-go":             ClassSUT,
		"":                     ClassSUT, // empty client id is still the SUT, not harness
	}
	for id, want := range cases {
		if got := cl(id); got != want {
			t.Fatalf("classify %q: got %s, want %s", id, got, want)
		}
	}
}

// connWithClientID runs a Conn over pipes, sends one request carrying clientID
// so the connection classifies itself, and returns the Conn. The caller closes
// the returned ends.
func connWithClientID(t *testing.T, clientID string) (*Conn, func()) {
	t.Helper()
	clientEnd, proxyClient := net.Pipe()
	proxyBroker, brokerEnd := net.Pipe()

	c := NewConn(proxyClient, proxyBroker, 0, WithClassifier(PrefixClassifier(DefaultHarnessPrefix)))
	go c.Run(context.Background())
	brokerEnd.SetDeadline(time.Now().Add(5 * time.Second))

	req := reqWire2(kmsg.NewPtrMetadataRequest(), 9, 1, clientID)
	go clientEnd.Write(req)
	if _, err := ReadFrame(brokerEnd, DefaultMaxFrameBytes); err != nil {
		t.Fatalf("broker read: %v", err)
	}
	return c, func() { clientEnd.Close(); brokerEnd.Close() }
}

// reqWire2 is reqWire with an explicit client id.
func reqWire2(req kmsg.Request, version int16, corr int32, clientID string) []byte {
	req.SetVersion(version)
	return kmsg.NewRequestFormatter(kmsg.FormatterClientID(clientID)).AppendRequest(nil, req, corr)
}

func TestConnClassifiesFromFirstRequest(t *testing.T) {
	sut, done1 := connWithClientID(t, "payments-consumer/1")
	defer done1()
	harness, done2 := connWithClientID(t, "crashpoint-workload")
	defer done2()

	if sut.Class() != ClassSUT || !sut.Faultable() {
		t.Fatalf("sut conn: class=%s faultable=%v", sut.Class(), sut.Faultable())
	}
	if harness.Class() != ClassHarness || harness.Faultable() {
		t.Fatalf("harness conn: class=%s faultable=%v", harness.Class(), harness.Faultable())
	}
}

// The core safety invariant (DESIGN.md §7.2): a fault that targets every
// connection must never select a harness one.
func TestFaultTargetingEverythingSkipsHarness(t *testing.T) {
	sut1, d1 := connWithClientID(t, "payments-consumer/1")
	defer d1()
	sut2, d2 := connWithClientID(t, "franz-go")
	defer d2()
	harness, d3 := connWithClientID(t, "crashpoint-workload")
	defer d3()

	all := []*Conn{sut1, sut2, harness}
	var targeted []*Conn
	for _, c := range all { // "fault everything"
		if c.Faultable() {
			targeted = append(targeted, c)
		}
	}
	if len(targeted) != 2 {
		t.Fatalf("want 2 faultable (sut) conns, got %d", len(targeted))
	}
	for _, c := range targeted {
		if c.Class() == ClassHarness {
			t.Fatal("a harness connection was selected for faulting")
		}
	}
}

// An unclassified connection (no request seen) is never faultable.
func TestUnknownConnNotFaultable(t *testing.T) {
	c := NewConn(nil, nil, 0)
	if c.Class() != ClassUnknown || c.Faultable() {
		t.Fatalf("fresh conn: class=%s faultable=%v", c.Class(), c.Faultable())
	}
}

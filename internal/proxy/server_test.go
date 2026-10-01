package proxy

import (
	"net"
	"testing"
)

// ensureNode must create exactly one listener per node id, return a stable proxy
// port on repeat calls, and give different nodes different ports.
func TestEnsureNodeOneListenerPerNode(t *testing.T) {
	s := &Server{AdvHost: "127.0.0.1"}
	defer s.closeNodes()

	host1, port1 := s.ensureNode(1, "broker-1", 9092)
	host1b, port1b := s.ensureNode(1, "broker-1", 9092) // same node again
	host2, port2 := s.ensureNode(2, "broker-2", 9092)

	if host1 != "127.0.0.1" || host2 != "127.0.0.1" {
		t.Fatalf("proxy host should be AdvHost, got %s / %s", host1, host2)
	}
	if port1 != port1b || host1 != host1b {
		t.Fatalf("node 1 address not stable: %s:%d vs %s:%d", host1, port1, host1b, port1b)
	}
	if port1 == port2 {
		t.Fatalf("distinct nodes must get distinct ports, both got %d", port1)
	}
	if len(s.nodes) != 2 {
		t.Fatalf("want 2 node listeners, got %d", len(s.nodes))
	}
	// The returned port must actually be listening.
	c, err := net.Dial("tcp", net.JoinHostPort(host1, itoa(port1)))
	if err != nil {
		t.Fatalf("node listener not accepting: %v", err)
	}
	c.Close()
}

func itoa(p int32) string {
	// tiny local helper so the test needn't import strconv just for this
	if p == 0 {
		return "0"
	}
	var b [6]byte
	i := len(b)
	for p > 0 {
		i--
		b[i] = byte('0' + p%10)
		p /= 10
	}
	return string(b[i:])
}

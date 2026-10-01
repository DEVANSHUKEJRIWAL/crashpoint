package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// reqWire encodes a full length-prefixed request, ready to write to the client.
func reqWire(req kmsg.Request, version int16, corr int32) []byte {
	req.SetVersion(version)
	return kmsg.NewRequestFormatter(kmsg.FormatterClientID("test")).AppendRequest(nil, req, corr)
}

// respWire builds a minimal length-prefixed response: correlation id + dummy body.
func respWire(corr int32) []byte {
	body := make([]byte, 6)
	binary.BigEndian.PutUint32(body, uint32(corr))
	out := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(out, uint32(len(body)))
	copy(out[4:], body)
	return out
}

func TestConnForwardsAndTracksCorrelation(t *testing.T) {
	clientEnd, proxyClient := net.Pipe()
	proxyBroker, brokerEnd := net.Pipe()
	defer clientEnd.Close()
	defer brokerEnd.Close()

	c := NewConn(proxyClient, proxyBroker, 0, nil)
	go c.Run(context.Background())

	deadline := time.Now().Add(5 * time.Second)
	for _, e := range []net.Conn{clientEnd, brokerEnd} {
		e.SetDeadline(deadline)
	}

	// A Metadata request (expects a response) should be forwarded byte-for-byte
	// and registered before it reaches the broker.
	req := reqWire(kmsg.NewPtrMetadataRequest(), 9, 7001)
	go clientEnd.Write(req)
	got, err := ReadFrame(brokerEnd, DefaultMaxFrameBytes)
	if err != nil {
		t.Fatalf("broker read: %v", err)
	}
	if !bytes.Equal(got, req[4:]) {
		t.Fatal("forwarded request differs from the original")
	}
	if n := c.corr.len(); n != 1 {
		t.Fatalf("want 1 registered correlation, got %d", n)
	}

	// Its response flows back and clears the correlation.
	go brokerEnd.Write(respWire(7001))
	if _, err := ReadFrame(clientEnd, DefaultMaxFrameBytes); err != nil {
		t.Fatalf("client read: %v", err)
	}
	// take() happens before the response is written to the client, so by the
	// time the client read returns the map is clear.
	if n := c.corr.len(); n != 0 {
		t.Fatalf("want 0 correlations after response, got %d", n)
	}
}

func TestConnSkipsAcks0Produce(t *testing.T) {
	clientEnd, proxyClient := net.Pipe()
	proxyBroker, brokerEnd := net.Pipe()
	defer clientEnd.Close()
	defer brokerEnd.Close()

	c := NewConn(proxyClient, proxyBroker, 0, nil)
	go c.Run(context.Background())
	brokerEnd.SetDeadline(time.Now().Add(5 * time.Second))

	p := kmsg.NewPtrProduceRequest()
	p.Acks = 0
	go clientEnd.Write(reqWire(p, 9, 7002))
	if _, err := ReadFrame(brokerEnd, DefaultMaxFrameBytes); err != nil {
		t.Fatalf("broker read: %v", err)
	}
	// acks=0 gets no response, so it must not be registered.
	if n := c.corr.len(); n != 0 {
		t.Fatalf("acks=0 Produce must not be registered, got %d", n)
	}
}

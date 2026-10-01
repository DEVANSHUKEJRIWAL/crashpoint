package proxy

import (
	"context"
	"net"
	"sync"

	"github.com/DEVANSHUKEJRIWAL/crashpoint/internal/protocol"
)

// apiVersion is what a response needs but its header lacks: the api key and
// version of the request it answers.
type apiVersion struct {
	Key     int16
	Version int16
}

// correlations maps a connection's in-flight correlation ids to the request
// they belong to. A request registers before it is forwarded; the matching
// response takes it. One map per connection (ARCHITECTURE §5).
type correlations struct {
	mu sync.Mutex
	m  map[int32]apiVersion
}

func newCorrelations() *correlations { return &correlations{m: map[int32]apiVersion{}} }

func (c *correlations) register(corr int32, av apiVersion) {
	c.mu.Lock()
	c.m[corr] = av
	c.mu.Unlock()
}

func (c *correlations) take(corr int32) (apiVersion, bool) {
	c.mu.Lock()
	av, ok := c.m[corr]
	delete(c.m, corr)
	c.mu.Unlock()
	return av, ok
}

func (c *correlations) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m)
}

// Conn proxies one client connection to one broker connection. Two goroutines
// pump the two directions (Kafka connections are long-lived and ordered); the
// request pump records each correlation id so the response pump can pair the
// headerless response back to its api key and version.
type Conn struct {
	client, broker net.Conn
	max            int
	corr           *correlations
}

func NewConn(client, broker net.Conn, maxFrame int) *Conn {
	if maxFrame <= 0 {
		maxFrame = DefaultMaxFrameBytes
	}
	return &Conn{client: client, broker: broker, max: maxFrame, corr: newCorrelations()}
}

// Run pumps both directions until either side closes or errors, then closes
// both connections so the other goroutine unblocks. It returns the first error
// (io.EOF on a clean close).
func (c *Conn) Run(ctx context.Context) error {
	errc := make(chan error, 2)
	go func() { errc <- c.pumpRequests() }()
	go func() { errc <- c.pumpResponses() }()

	var err error
	select {
	case err = <-errc:
	case <-ctx.Done():
		err = ctx.Err()
	}
	c.client.Close()
	c.broker.Close()
	<-errc // drain the second goroutine
	return err
}

// pumpRequests forwards client → broker, registering the correlation id of each
// request that will get a response. A Produce with acks=0 gets none, so it is
// not registered; a header we cannot parse is still forwarded (transparency)
// but not registered.
func (c *Conn) pumpRequests() error {
	for {
		frame, err := ReadFrame(c.client, c.max)
		if err != nil {
			return err
		}
		if h, perr := protocol.ParseRequestHeader(frame); perr == nil && expectsResponse(frame, h) {
			c.corr.register(h.CorrelationID, apiVersion{h.APIKey, h.APIVersion})
		}
		if err := WriteFrame(c.broker, frame); err != nil {
			return err
		}
	}
}

// pumpResponses forwards broker → client, taking the correlation id so the map
// stays bounded and (in later issues) the api key/version is available to
// decode the body.
func (c *Conn) pumpResponses() error {
	for {
		frame, err := ReadFrame(c.broker, c.max)
		if err != nil {
			return err
		}
		if corr, ok := protocol.ResponseCorrelationID(frame); ok {
			c.corr.take(corr)
		}
		if err := WriteFrame(c.client, frame); err != nil {
			return err
		}
	}
}

// expectsResponse reports whether the broker will reply to this request. The
// only request that gets no reply is a Produce (api key 0) with acks=0.
func expectsResponse(frame []byte, h protocol.RequestHeader) bool {
	if h.APIKey != 0 {
		return true
	}
	acks, ok := protocol.ProduceAcks(frame[h.Size:], h.APIVersion)
	return !ok || acks != 0
}

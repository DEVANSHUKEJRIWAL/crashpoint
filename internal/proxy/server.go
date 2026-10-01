package proxy

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"sync"
)

// Server is the transparent proxy. A client connects to the bootstrap listener;
// the proxy rewrites broker addresses in Metadata/FindCoordinator/DescribeCluster
// responses to point at per-node listeners it creates lazily, so every later
// Produce/Fetch connection also lands on the proxy instead of a real broker.
//
// One listener per broker node (not one shared port) is what makes multi-broker
// clusters route correctly — the flaw ARCHITECTURE §2.1 calls out in Kafka's own
// fault proxy.
type Server struct {
	// AdvHost is the host clients dial to reach the proxy. Per-node listeners
	// bind here on ephemeral ports. Defaults to the bootstrap listen host.
	AdvHost  string
	MaxFrame int
	Log      *slog.Logger
	// Classifier tags each accepted connection sut/harness. Defaults to
	// PrefixClassifier(DefaultHarnessPrefix).
	Classifier Classifier

	mu    sync.Mutex
	nodes map[int32]*nodeListener
	wg    sync.WaitGroup
}

type nodeListener struct {
	realAddr string
	ln       net.Listener
	port     int32
}

func (s *Server) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

// ensureNode is the production AddrRewriter: it guarantees a listener exists for
// nodeID (forwarding to the broker's real host:port) and returns the proxy
// address clients should use. Idempotent per node.
func (s *Server) ensureNode(nodeID int32, host string, port int32) (string, int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n, ok := s.nodes[nodeID]; ok {
		return s.AdvHost, n.port
	}
	real := net.JoinHostPort(host, strconv.Itoa(int(port)))
	ln, err := net.Listen("tcp", net.JoinHostPort(s.AdvHost, "0"))
	if err != nil {
		// ponytail: ephemeral binds rarely fail; if one does we can't proxy this
		// node, so hand back the real address and log it rather than wedging.
		// The no-bypass integration test is what would catch the leak.
		s.log().Error("listen for node failed; client will bypass proxy", "node", nodeID, "err", err)
		return host, port
	}
	n := &nodeListener{realAddr: real, ln: ln, port: int32(ln.Addr().(*net.TCPAddr).Port)}
	if s.nodes == nil {
		s.nodes = map[int32]*nodeListener{}
	}
	s.nodes[nodeID] = n
	s.wg.Add(1)
	go s.accept(n.ln, n.realAddr)
	s.log().Info("node listener up", "node", nodeID, "proxy_port", n.port, "broker", real)
	return s.AdvHost, n.port
}

// ListenAndServe serves the bootstrap listener until ctx is cancelled or the
// listener errors, then tears down every node listener. seed is the real broker
// address the bootstrap connection dials.
func (s *Server) ListenAndServe(ctx context.Context, bootstrapAddr, seed string) error {
	ln, err := net.Listen("tcp", bootstrapAddr)
	if err != nil {
		return err
	}
	if s.AdvHost == "" {
		if host, _, e := net.SplitHostPort(bootstrapAddr); e == nil && host != "" {
			s.AdvHost = host
		} else {
			s.AdvHost = "localhost"
		}
	}
	context.AfterFunc(ctx, func() { ln.Close() })

	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.serve(ln, seed) }()

	<-ctx.Done()
	s.closeNodes()
	s.wg.Wait()
	return ctx.Err()
}

// accept runs one listener's accept loop, proxying each client to dialAddr.
func (s *Server) accept(ln net.Listener, dialAddr string) {
	defer s.wg.Done()
	s.serve(ln, dialAddr)
}

func (s *Server) serve(ln net.Listener, dialAddr string) {
	for {
		client, err := ln.Accept()
		if err != nil {
			return // listener closed
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(client, dialAddr)
		}()
	}
}

func (s *Server) handle(client net.Conn, dialAddr string) {
	broker, err := net.Dial("tcp", dialAddr)
	if err != nil {
		s.log().Error("dial broker", "addr", dialAddr, "err", err)
		client.Close()
		return
	}
	classify := s.Classifier
	if classify == nil {
		classify = PrefixClassifier(DefaultHarnessPrefix)
	}
	// Each Conn rewrites the address-bearing responses it carries, so a Metadata
	// request answered by any node still hands the client proxy addresses, and
	// tags itself sut/harness so faults never touch harness traffic.
	NewConn(client, broker, s.MaxFrame,
		WithRewriter(s.ensureNode),
		WithClassifier(classify),
	).Run(context.Background())
}

func (s *Server) closeNodes() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.nodes {
		n.ln.Close()
	}
}

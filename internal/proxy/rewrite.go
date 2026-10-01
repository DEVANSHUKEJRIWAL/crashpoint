package proxy

import (
	"github.com/DEVANSHUKEJRIWAL/crashpoint/internal/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// API keys whose responses advertise broker addresses.
const (
	keyMetadata        = 3
	keyFindCoordinator = 10
	keyDescribeCluster = 60
)

// AddrRewriter maps a real broker (node id + host:port) to the proxy address a
// client should use instead. In the real server it also ensures a proxy
// listener for that node exists (server.go); in tests it is a stub.
type AddrRewriter func(nodeID int32, host string, port int32) (string, int32)

// RewriteResponse rewrites every broker address in a Metadata, FindCoordinator
// or DescribeCluster response so clients only ever learn proxy addresses; any
// other response is returned unchanged. Without this, a client connects
// straight to the real brokers after bootstrapping and bypasses the proxy
// (ARCHITECTURE §2.1). The full bodies are decoded with kmsg — re-deriving
// Kafka's schema across every version by hand would be a bug farm, and the
// novel work here is the rewrite, not the codec.
func RewriteResponse(frame []byte, apiKey, apiVersion int16, rw AddrRewriter) ([]byte, error) {
	switch apiKey {
	case keyMetadata, keyFindCoordinator, keyDescribeCluster:
	default:
		return frame, nil
	}

	header, body, err := protocol.SplitResponse(frame, apiKey, apiVersion)
	if err != nil {
		return nil, err
	}
	resp := kmsg.ResponseForKey(apiKey)
	resp.SetVersion(apiVersion)
	if err := resp.ReadFrom(body); err != nil {
		return nil, err
	}

	switch r := resp.(type) {
	case *kmsg.MetadataResponse:
		for i := range r.Brokers {
			b := &r.Brokers[i]
			b.Host, b.Port = rw(b.NodeID, b.Host, b.Port)
		}
	case *kmsg.FindCoordinatorResponse:
		if apiVersion >= 4 { // KIP-699 batched form
			for i := range r.Coordinators {
				co := &r.Coordinators[i]
				if co.ErrorCode != 0 {
					continue // an errored entry carries no usable address
				}
				co.Host, co.Port = rw(co.NodeID, co.Host, co.Port)
			}
		} else if r.ErrorCode == 0 {
			r.Host, r.Port = rw(r.NodeID, r.Host, r.Port)
		}
	case *kmsg.DescribeClusterResponse:
		for i := range r.Brokers {
			b := &r.Brokers[i]
			b.Host, b.Port = rw(b.NodeID, b.Host, b.Port)
		}
	}

	out := append([]byte(nil), header...)
	return resp.AppendTo(out), nil
}

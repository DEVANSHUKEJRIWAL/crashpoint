package proxy

import (
	"encoding/binary"
	"testing"

	"github.com/DEVANSHUKEJRIWAL/crashpoint/internal/protocol"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// respFrameFromKmsg encodes a response body with kmsg and prepends a response
// header (correlation id + an empty tagged-field buffer when the response uses a
// flexible header), giving exactly what RewriteResponse receives (no length
// prefix). The flexible threshold is per-API, so it comes from the same helper
// production uses.
func respFrameFromKmsg(t *testing.T, resp kmsg.Response, apiKey, version int16, corr int32) []byte {
	t.Helper()
	resp.SetVersion(version)
	body := resp.AppendTo(nil)
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(corr))
	if protocol.ResponseHeaderVersion(apiKey, version) >= 1 {
		hdr = append(hdr, 0) // empty tag buffer
	}
	return append(hdr, body...)
}

func respHeaderLen(apiKey, version int16) int {
	if protocol.ResponseHeaderVersion(apiKey, version) >= 1 {
		return 5
	}
	return 4
}

// toProxy is a stub AddrRewriter that records calls and maps everything to one
// fixed proxy address.
func toProxy(seen *[]int32) AddrRewriter {
	return func(nodeID int32, _ string, _ int32) (string, int32) {
		*seen = append(*seen, nodeID)
		return "proxy.local", 19000 + nodeID
	}
}

func TestRewriteMetadata(t *testing.T) {
	for _, version := range []int16{8, 12} { // non-flexible and flexible
		resp := kmsg.NewPtrMetadataResponse()
		b1 := kmsg.NewMetadataResponseBroker()
		b1.NodeID, b1.Host, b1.Port = 1, "broker-1.internal", 9092
		b2 := kmsg.NewMetadataResponseBroker()
		b2.NodeID, b2.Host, b2.Port = 2, "broker-2.internal", 9092
		resp.Brokers = append(resp.Brokers, b1, b2)

		frame := respFrameFromKmsg(t, resp, keyMetadata, version, 1)
		var seen []int32
		out, err := RewriteResponse(frame, keyMetadata, version, toProxy(&seen))
		if err != nil {
			t.Fatalf("v%d: %v", version, err)
		}
		got := decodeMetadata(t, out, version)
		for i, b := range got.Brokers {
			if b.Host != "proxy.local" || b.Port != 19000+b.NodeID {
				t.Fatalf("v%d broker %d not rewritten: %s:%d", version, i, b.Host, b.Port)
			}
		}
		if len(seen) != 2 {
			t.Fatalf("v%d: expected 2 brokers rewritten, got %d", version, len(seen))
		}
	}
}

func decodeMetadata(t *testing.T, frame []byte, version int16) *kmsg.MetadataResponse {
	t.Helper()
	r := kmsg.NewPtrMetadataResponse()
	r.SetVersion(version)
	if err := r.ReadFrom(frame[respHeaderLen(keyMetadata, version):]); err != nil {
		t.Fatalf("decode rewritten metadata: %v", err)
	}
	return r
}

func TestRewriteFindCoordinatorTopLevel(t *testing.T) {
	resp := kmsg.NewPtrFindCoordinatorResponse() // v3: top-level coordinator
	resp.NodeID, resp.Host, resp.Port = 5, "coord.internal", 9092
	frame := respFrameFromKmsg(t, resp, keyFindCoordinator, 3, 1)

	var seen []int32
	out, err := RewriteResponse(frame, keyFindCoordinator, 3, toProxy(&seen))
	if err != nil {
		t.Fatal(err)
	}
	r := kmsg.NewPtrFindCoordinatorResponse()
	r.SetVersion(3)
	if err := r.ReadFrom(out[5:]); err != nil { // v3 is flexible → 1 tag byte
		t.Fatal(err)
	}
	if r.Host != "proxy.local" || r.Port != 19005 {
		t.Fatalf("coordinator not rewritten: %s:%d", r.Host, r.Port)
	}
}

func TestRewriteFindCoordinatorBatchedSkipsErrored(t *testing.T) {
	resp := kmsg.NewPtrFindCoordinatorResponse()
	good := kmsg.NewFindCoordinatorResponseCoordinator()
	good.NodeID, good.Host, good.Port, good.ErrorCode = 7, "coord-ok.internal", 9092, 0
	bad := kmsg.NewFindCoordinatorResponseCoordinator()
	bad.NodeID, bad.Host, bad.Port, bad.ErrorCode = 9, "", 0, 15 // COORDINATOR_NOT_AVAILABLE
	resp.Coordinators = append(resp.Coordinators, good, bad)

	frame := respFrameFromKmsg(t, resp, keyFindCoordinator, 4, 1)
	var seen []int32
	out, err := RewriteResponse(frame, keyFindCoordinator, 4, toProxy(&seen))
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0] != 7 {
		t.Fatalf("only the healthy coordinator should be rewritten, got %v", seen)
	}
	r := kmsg.NewPtrFindCoordinatorResponse()
	r.SetVersion(4)
	if err := r.ReadFrom(out[5:]); err != nil {
		t.Fatal(err)
	}
	if r.Coordinators[0].Host != "proxy.local" {
		t.Fatalf("healthy coordinator not rewritten: %s", r.Coordinators[0].Host)
	}
	if r.Coordinators[1].Host != "" {
		t.Fatalf("errored coordinator must be left untouched, got %s", r.Coordinators[1].Host)
	}
}

func TestRewriteIgnoresOtherAPIs(t *testing.T) {
	frame := []byte{0, 0, 0, 1, 9, 9, 9} // arbitrary non-address response
	out, err := RewriteResponse(frame, 0 /* Produce */, 9, func(int32, string, int32) (string, int32) {
		t.Fatal("rewriter must not be called for a non-address API")
		return "", 0
	})
	if err != nil || string(out) != string(frame) {
		t.Fatalf("non-address response should pass through unchanged: %v", err)
	}
}

package protocol

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// frameFor encodes a real request with kmsg and strips the 4-byte length prefix,
// giving exactly what the proxy hands ParseRequestHeader.
func frameFor(req kmsg.Request, version int16, corr int32, clientID string) []byte {
	req.SetVersion(version)
	f := kmsg.NewRequestFormatter(kmsg.FormatterClientID(clientID))
	wire := f.AppendRequest(nil, req, corr)
	return wire[4:] // drop length prefix
}

func TestParseRequestHeader(t *testing.T) {
	cases := []struct {
		name       string
		req        kmsg.Request
		version    int16
		clientID   string
		wantFlex   int16 // expected header version
	}{
		{"metadata non-flexible v8", kmsg.NewPtrMetadataRequest(), 8, "franz", 1},
		{"metadata flexible v9", kmsg.NewPtrMetadataRequest(), 9, "franz", 2},
		{"fetch non-flexible v11", kmsg.NewPtrFetchRequest(), 11, "consumer-1", 1},
		{"fetch flexible v12", kmsg.NewPtrFetchRequest(), 12, "consumer-1", 2},
		{"apiversions flexible v3", kmsg.NewPtrApiVersionsRequest(), 3, "c", 2},
		{"empty client id", kmsg.NewPtrMetadataRequest(), 9, "", 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			frame := frameFor(c.req, c.version, 4242, c.clientID)
			h, err := ParseRequestHeader(frame)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if h.APIKey != c.req.Key() || h.APIVersion != c.version {
				t.Fatalf("key/version: got (%d,%d), want (%d,%d)", h.APIKey, h.APIVersion, c.req.Key(), c.version)
			}
			if h.CorrelationID != 4242 {
				t.Fatalf("correlation: got %d, want 4242", h.CorrelationID)
			}
			if h.ClientID != c.clientID {
				t.Fatalf("client id: got %q, want %q", h.ClientID, c.clientID)
			}
			if h.HeaderVersion != c.wantFlex {
				t.Fatalf("header version: got %d, want %d", h.HeaderVersion, c.wantFlex)
			}
			// Size must land exactly where the body begins: the whole frame is
			// header + empty-ish body, so Size <= len and parsing is consistent.
			if h.Size <= 0 || h.Size > len(frame) {
				t.Fatalf("header size %d out of range (frame %d)", h.Size, len(frame))
			}
		})
	}
}

func TestParseRequestHeaderShort(t *testing.T) {
	if _, err := ParseRequestHeader([]byte{0, 0, 0}); err == nil {
		t.Fatal("want error on a truncated header, got nil")
	}
}

func TestResponseHeaderVersionApiVersionsQuirk(t *testing.T) {
	// ApiVersions response is header v0 even though v3 is flexible.
	if v := ResponseHeaderVersion(apiVersionsKey, 3); v != 0 {
		t.Fatalf("ApiVersions response header: got v%d, want v0", v)
	}
	// Metadata v9 is flexible → response header v1.
	if v := ResponseHeaderVersion(3, 9); v != 1 {
		t.Fatalf("Metadata v9 response header: got v%d, want v1", v)
	}
	// Metadata v8 is not flexible → response header v0.
	if v := ResponseHeaderVersion(3, 8); v != 0 {
		t.Fatalf("Metadata v8 response header: got v%d, want v0", v)
	}
}

func TestProduceAcks(t *testing.T) {
	for _, version := range []int16{7, 9} { // non-flexible and flexible
		for _, acks := range []int16{0, 1, -1} {
			req := kmsg.NewPtrProduceRequest()
			req.Acks = acks
			frame := frameFor(req, version, 1, "p")
			h, err := ParseRequestHeader(frame)
			if err != nil {
				t.Fatalf("v%d: header: %v", version, err)
			}
			got, ok := ProduceAcks(frame[h.Size:], version)
			if !ok || got != acks {
				t.Fatalf("v%d acks: got (%d,%v), want %d", version, got, ok, acks)
			}
		}
	}
}

package protocol

import (
	"encoding/binary"

	"github.com/twmb/franz-go/pkg/kmsg"
)

const apiVersionsKey = 18

// RequestHeader is everything the proxy reads from a request before forwarding
// it: the api key/version it must remember for the (headerless) response, the
// correlation id that pairs the two, and the client id used to classify the
// connection (issue #7).
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string // "" if null
	HeaderVersion int16
	Size          int // bytes the header occupied in the frame
}

// ParseRequestHeader decodes a request header from a frame whose length prefix
// has already been stripped. It reads the client id only for header v1+ and
// skips tagged fields only for v2, matching how the broker will read it.
func ParseRequestHeader(frame []byte) (RequestHeader, error) {
	r := &reader{b: frame}
	h := RequestHeader{
		APIKey:        r.int16(),
		APIVersion:    r.int16(),
		CorrelationID: r.int32(),
	}
	if r.err != nil {
		return h, r.err
	}
	h.HeaderVersion = requestHeaderVersion(h.APIKey, h.APIVersion)
	if h.HeaderVersion >= 1 {
		h.ClientID = r.nullableString()
	}
	if h.HeaderVersion >= 2 {
		r.skipTags()
	}
	if r.err != nil {
		return h, r.err
	}
	h.Size = r.i
	return h, nil
}

// requestHeaderVersion: flexible requests use header v2, everything else v1.
// (Header v0, which omits the client id, is only used by a couple of
// inter-broker requests the proxy never sees, so we never produce it.)
func requestHeaderVersion(apiKey, apiVersion int16) int16 {
	req := kmsg.RequestForKey(apiKey)
	if req == nil {
		return 1 // unknown api: forwarded untouched, assume a normal v1 header
	}
	req.SetVersion(apiVersion)
	if req.IsFlexible() {
		return 2
	}
	return 1
}

// ResponseHeaderVersion returns the header version a response uses, for when
// later issues decode response bodies. ApiVersions is the exception: its
// response always uses header v0 (no tagged fields), because a client issues
// ApiVersions before it knows the broker's version and an old broker could not
// read a flexible header. Otherwise flexible responses use header v1, others v0.
func ResponseHeaderVersion(apiKey, apiVersion int16) int16 {
	if apiKey == apiVersionsKey {
		return 0
	}
	resp := kmsg.ResponseForKey(apiKey)
	if resp == nil {
		return 0
	}
	resp.SetVersion(apiVersion)
	if resp.IsFlexible() {
		return 1
	}
	return 0
}

// ResponseCorrelationID reads the correlation id that begins every response
// header (present in both v0 and v1). A Kafka response carries no api key, so
// this id is the only link back to the request the proxy recorded.
func ResponseCorrelationID(frame []byte) (int32, bool) {
	if len(frame) < 4 {
		return 0, false
	}
	return int32(binary.BigEndian.Uint32(frame)), true
}

// ProduceAcks reads the acks field from a Produce request body (the bytes after
// the request header). acks == 0 means the broker sends no response, so the
// proxy must not register a correlation id for that request.
func ProduceAcks(body []byte, apiVersion int16) (int16, bool) {
	r := &reader{b: body}
	if apiVersion >= 3 { // v3+ prefixes a transactional id before acks
		if apiVersion >= 9 { // ponytail: Produce is flexible at v9+; inline, not a kmsg call per frame
			r.skipCompactNullableString()
		} else {
			r.skipNullableString()
		}
	}
	acks := r.int16()
	if r.err != nil {
		return 0, false
	}
	return acks, true
}

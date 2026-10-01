// Package proxy is Crashpoint's transparent Kafka wire-protocol proxy: it reads
// length-prefixed frames, tracks the correlation ids responses need, and (in
// later issues) rewrites addresses and injects faults.
package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
)

// DefaultMaxFrameBytes caps a single frame so a bad length prefix can't make the
// proxy allocate unbounded memory. Kafka's own socket.request.max.bytes default
// is 100 MiB; we match it.
const DefaultMaxFrameBytes = 100 << 20

// ReadFrame reads one length-prefixed Kafka frame (INT32 length, then that many
// bytes) and returns the payload without the prefix. A length above max is an
// error, not an allocation.
func ReadFrame(r io.Reader, max int) ([]byte, error) {
	var lenbuf [4]byte
	if _, err := io.ReadFull(r, lenbuf[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(lenbuf[:]))
	if n < 0 || n > max {
		return nil, fmt.Errorf("proxy: frame length %d exceeds max %d", n, max)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// WriteFrame writes a payload with its INT32 length prefix.
func WriteFrame(w io.Writer, payload []byte) error {
	var lenbuf [4]byte
	binary.BigEndian.PutUint32(lenbuf[:], uint32(len(payload)))
	if _, err := w.Write(lenbuf[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

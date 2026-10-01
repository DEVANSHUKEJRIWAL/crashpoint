package proxy

import (
	"bytes"
	"testing"
)

// ReadFrame must never panic or hand back a frame larger than its guard,
// whatever the length prefix claims.
func FuzzReadFrame(f *testing.F) {
	f.Add([]byte{0, 0, 0, 3, 1, 2, 3}) // well-formed 3-byte frame
	f.Add([]byte{0, 0x10, 0, 0})       // huge length, no body
	f.Add([]byte{0, 0, 0})             // truncated prefix
	f.Add([]byte{})

	const max = 1 << 20
	f.Fuzz(func(t *testing.T, data []byte) {
		frame, err := ReadFrame(bytes.NewReader(data), max)
		if err == nil && len(frame) > max {
			t.Fatalf("frame length %d exceeds max %d", len(frame), max)
		}
	})
}

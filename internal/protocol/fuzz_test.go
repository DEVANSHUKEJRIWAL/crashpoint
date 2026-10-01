package protocol

import "testing"

// A protocol parser that can panic or read out of bounds on hostile input is a
// liability in a proxy that sees every client's bytes. These assert neither
// happens on arbitrary input.

func FuzzParseRequestHeader(f *testing.F) {
	// Metadata v9 (flexible) header: key=3, ver=9, corr=1, client "hi", 0 tags.
	f.Add([]byte{0, 3, 0, 9, 0, 0, 0, 1, 0, 2, 'h', 'i', 0})
	// Fetch v11 (non-flexible) header: key=1, ver=11, corr=7, null client id.
	f.Add([]byte{0, 1, 0, 11, 0, 0, 0, 7, 0xff, 0xff})
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		h, err := ParseRequestHeader(data)
		if err == nil && (h.Size < 0 || h.Size > len(data)) {
			t.Fatalf("header Size %d out of range (len %d)", h.Size, len(data))
		}
	})
}

func FuzzSkipTags(f *testing.F) {
	f.Add([]byte{0})             // zero tags
	f.Add([]byte{1, 5, 2, 9, 9}) // one tag: id=5, size=2, 2 bytes
	f.Add([]byte{0xff, 0xff})    // garbage varint
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		r := &reader{b: data}
		r.skipTags()
		if r.i < 0 || r.i > len(data) {
			t.Fatalf("cursor %d out of range (len %d)", r.i, len(data))
		}
	})
}

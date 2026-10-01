// Package protocol parses the slices of the Kafka wire protocol that Crashpoint
// needs to track and, later, rewrite. It decodes request headers and the few
// fields the proxy acts on; it never decodes whole request/response bodies.
package protocol

import (
	"encoding/binary"
	"errors"
)

var errShort = errors.New("protocol: buffer underflow")

// reader is a bounds-checked big-endian cursor over a byte slice. On any
// underflow it latches err and returns zero values, so callers check err once
// at the end instead of after every field.
type reader struct {
	b   []byte
	i   int
	err error
}

func (r *reader) fail() {
	if r.err == nil {
		r.err = errShort
	}
}

func (r *reader) int16() int16 {
	if r.err != nil || r.i+2 > len(r.b) {
		r.fail()
		return 0
	}
	v := int16(binary.BigEndian.Uint16(r.b[r.i:]))
	r.i += 2
	return v
}

func (r *reader) int32() int32 {
	if r.err != nil || r.i+4 > len(r.b) {
		r.fail()
		return 0
	}
	v := int32(binary.BigEndian.Uint32(r.b[r.i:]))
	r.i += 4
	return v
}

// uvarint reads an unsigned LEB128 varint (the KIP-482 "compact" length prefix).
func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b[r.i:])
	if n <= 0 {
		r.fail()
		return 0
	}
	r.i += n
	return v
}

// nullableString reads an INT16-length string; length -1 is null, returned as "".
// Client ids use this encoding even inside a flexible (v2) header.
func (r *reader) nullableString() string {
	n := int(r.int16())
	if r.err != nil || n < 0 {
		return ""
	}
	if r.i+n > len(r.b) {
		r.fail()
		return ""
	}
	s := string(r.b[r.i : r.i+n])
	r.i += n
	return s
}

func (r *reader) skip(n int) {
	// Compare against remaining bytes rather than r.i+n: a hostile compact
	// length (e.g. a tag size near maxint) would overflow r.i+n and slip past
	// the guard. len(r.b)-r.i is always a small non-negative int.
	if r.err != nil || n < 0 || n > len(r.b)-r.i {
		r.fail()
		return
	}
	r.i += n
}

// skipNullableString skips an INT16-length string (-1 = null).
func (r *reader) skipNullableString() {
	n := int(r.int16())
	if r.err != nil || n < 0 {
		return
	}
	r.skip(n)
}

// skipCompactNullableString skips a COMPACT_NULLABLE_STRING: uvarint of len+1,
// where 0 means null.
func (r *reader) skipCompactNullableString() {
	n := r.uvarint()
	if r.err != nil || n == 0 {
		return
	}
	r.skip(int(n - 1))
}

// skipTags skips a flexible TAG_BUFFER: a uvarint count, then for each tag a
// uvarint id, a uvarint size, and that many bytes.
func (r *reader) skipTags() {
	n := r.uvarint()
	for k := uint64(0); k < n && r.err == nil; k++ {
		r.uvarint()         // tag id
		size := r.uvarint() // tag size
		r.skip(int(size))
	}
}

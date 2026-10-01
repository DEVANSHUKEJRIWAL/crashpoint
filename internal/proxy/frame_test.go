package proxy

import (
	"bytes"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	payload := []byte("the quick brown fox")
	var buf bytes.Buffer
	if err := WriteFrame(&buf, payload); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFrame(&buf, DefaultMaxFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round trip: got %q, want %q", got, payload)
	}
}

func TestReadFrameRejectsOversize(t *testing.T) {
	// Length prefix claims 1 MiB; max is 8. Must error without allocating it.
	frame := []byte{0, 0x10, 0, 0}
	if _, err := ReadFrame(bytes.NewReader(frame), 8); err == nil {
		t.Fatal("want error for oversize frame, got nil")
	}
}

func TestReadFrameShort(t *testing.T) {
	// Prefix says 10 bytes, only 3 follow.
	frame := []byte{0, 0, 0, 10, 1, 2, 3}
	if _, err := ReadFrame(bytes.NewReader(frame), DefaultMaxFrameBytes); err == nil {
		t.Fatal("want error for truncated frame, got nil")
	}
}

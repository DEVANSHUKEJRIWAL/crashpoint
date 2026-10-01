package main

import (
	"testing"
	"time"
)

func TestPercentileNearestRank(t *testing.T) {
	// sorted 1..100 ms
	var s []time.Duration
	for i := 1; i <= 100; i++ {
		s = append(s, time.Duration(i)*time.Millisecond)
	}
	cases := map[float64]time.Duration{
		50:   50 * time.Millisecond,  // ceil(0.50*100)=50 -> s[49]
		99:   99 * time.Millisecond,  // ceil(0.99*100)=99 -> s[98]
		99.9: 100 * time.Millisecond, // ceil(0.999*100)=100 -> s[99]
		100:  100 * time.Millisecond,
	}
	for p, want := range cases {
		if got := percentile(s, p); got != want {
			t.Fatalf("p%.1f: got %s, want %s", p, got, want)
		}
	}
}

func TestPercentileEmpty(t *testing.T) {
	if got := percentile(nil, 99); got != 0 {
		t.Fatalf("empty: want 0, got %s", got)
	}
}

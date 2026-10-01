// Crashpoint tool module. The reference SUT lives in sut/ as its own module so
// its drivers don't leak in here (ARCHITECTURE.md §8). Run `go mod tidy`.
module github.com/DEVANSHUKEJRIWAL/crashpoint

go 1.26.0

require (
	github.com/twmb/franz-go v1.22.1
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20260927204940-b5a45ccfdf7e
	github.com/twmb/franz-go/pkg/kmsg v1.14.0
)

require (
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.30 // indirect
)

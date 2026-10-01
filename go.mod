// Crashpoint tool module. The reference SUT lives in sut/ as its own module so
// its drivers don't leak in here (ARCHITECTURE.md §8). Run `go mod tidy`.
module github.com/DEVANSHUKEJRIWAL/crashpoint

go 1.23

require (
	github.com/twmb/franz-go v1.18.0
	github.com/twmb/franz-go/pkg/kmsg v1.9.0
)

require (
	github.com/klauspost/compress v1.17.8 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
)

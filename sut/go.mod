// Reference SUT — a separate Go module so its Kafka/Postgres drivers never leak
// into the crashpoint tool (ARCHITECTURE.md §8). Run `go mod tidy` in this dir
// to populate indirect requires and go.sum before building.
module github.com/DEVANSHUKEJRIWAL/crashpoint/sut

go 1.23

require (
	github.com/jackc/pgx/v5 v5.7.2
	github.com/twmb/franz-go v1.18.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.17.8 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.9.0 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/sync v0.10.0 // indirect
	golang.org/x/text v0.21.0 // indirect
)

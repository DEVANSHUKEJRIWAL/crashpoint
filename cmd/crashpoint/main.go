// Command crashpoint is the tool CLI. Week 1 ships the `proxy` subcommand; run,
// explore, shrink and the rest arrive with their issues (ARCHITECTURE §7).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/DEVANSHUKEJRIWAL/crashpoint/internal/proxy"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "proxy":
		runProxy(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: crashpoint proxy [flags]")
	os.Exit(2)
}

func runProxy(args []string) {
	fs := flag.NewFlagSet("proxy", flag.ExitOnError)
	listen := fs.String("listen", ":19092", "bootstrap listen address")
	advHost := fs.String("adv-host", "localhost", "host clients dial to reach the proxy")
	seed := fs.String("seed", "localhost:9092", "real broker to dial from the bootstrap connection")
	record := fs.String("record", "", "write frame records (JSONL) here; empty = recording off")
	fs.Parse(args)

	srv := &proxy.Server{AdvHost: *advHost}
	if *record != "" {
		f, err := os.Create(*record)
		if err != nil {
			fmt.Fprintln(os.Stderr, "record file:", err)
			os.Exit(1)
		}
		defer f.Close()
		srv.Recorder = proxy.NewRecorder(f)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(os.Stderr, "proxy listening on %s, seed %s, recording=%v\n", *listen, *seed, *record != "")
	if err := srv.ListenAndServe(ctx, *listen, *seed); err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, "proxy:", err)
		os.Exit(1)
	}
}

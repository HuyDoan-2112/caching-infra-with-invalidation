// Command loadgen sends open-loop load to the reader and writer: requests go
// out on a fixed timetable whether or not earlier ones were answered, and each
// one is timed from when it was due. The library is internal/loadgen.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/loadgen"
)

// seedParallel is how many creates run at once while seeding: the writer's
// MySQL pool size, so seeding never queues for a connection.
const seedParallel = 16

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run() error {
	rate := flag.Int("rate", 1000, "requests per second")
	duration := flag.Duration("duration", 30*time.Second, "how long to measure, after the warmup")
	warmup := flag.Duration("warmup", 10*time.Second, "how long to run before measuring; reported on its own")
	mixFlag := flag.String("mix", "80/10/10", "read/update/create percentages")
	dist := flag.String("dist", "uniform", "which items reads and updates pick: uniform or zipf")
	items := flag.Int("items", 10_000, "items to create before the run")
	maxInFlight := flag.Int("max-inflight", 10_000, "requests waiting at once; above it new ones are dropped")
	idleConns := flag.Int("idle-conns", 10_000, "idle connections kept per host (Go's default is 2)")
	timeout := flag.Duration("timeout", 5*time.Second, "per-request timeout; a request past it counts as an error")
	readURL := flag.String("read-url", "http://localhost:8081", "reader base URL")
	writeURL := flag.String("write-url", "http://localhost:8080", "writer base URL")
	flag.Parse()

	// Check every flag before seeding: seeding a million items takes minutes.
	mix, err := parseMix(*mixFlag)
	if err != nil {
		return err
	}
	if *dist != "uniform" && *dist != "zipf" {
		return fmt.Errorf("-dist=%q: want uniform or zipf", *dist)
	}
	if *rate < 1 || *items < 1 || *maxInFlight < 1 {
		return fmt.Errorf("-rate, -items and -max-inflight must be at least 1")
	}

	// Ctrl+C stops seeding and stops the schedule. Requests already sent are
	// not cancelled (they use context.Background below): they finish or time
	// out on their own, so a Ctrl+C doesn't turn them into errors.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Clone Go's default transport for its dial and TLS settings, then lift
	// its idle limits: 100 idle connections in total and 2 per host.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 0 // no limit in total
	transport.MaxIdleConnsPerHost = *idleConns
	client := &http.Client{Transport: transport, Timeout: *timeout}

	fmt.Printf("seeding %d items...\n", *items)
	seedStart := time.Now()
	ids, err := loadgen.Seed(ctx, client, *writeURL, *items, seedParallel)
	if err != nil {
		return fmt.Errorf("seed: %w", err)
	}
	fmt.Printf("seeded in %v\n", time.Since(seedStart).Round(time.Millisecond))

	pick := loadgen.Uniform(ids)
	if *dist == "zipf" {
		pick = loadgen.Zipf(ids, 1.1)
	}

	warmRec, rec := loadgen.NewRecorder(), loadgen.NewRecorder()
	inFlight := context.Background()
	warmupN := int(*warmup * time.Duration(*rate) / time.Second) // multiply first
	send := loadgen.SplitWarmup(warmupN,
		loadgen.NewSend(inFlight, client, *readURL, *writeURL, pick, mix, warmRec),
		loadgen.NewSend(inFlight, client, *readURL, *writeURL, pick, mix, rec))

	fmt.Printf("running %d req/s: %v warmup + %v measured, mix %s, dist %s\n",
		*rate, *warmup, *duration, *mixFlag, *dist)
	sent, dropped := loadgen.Schedule(ctx, *rate, *warmup+*duration, *maxInFlight, send)
	fmt.Printf("\nwhole run: %d scheduled, %d sent, %d dropped\n\n", sent+dropped, sent, dropped)

	if err := warmRec.Report(os.Stdout, fmt.Sprintf("warmup (%v)", *warmup)); err != nil {
		return err
	}
	fmt.Println()
	return rec.Report(os.Stdout, fmt.Sprintf("measured (%v)", *duration))
}

// parseMix reads "80/10/10" as read/update/create percentages that add up to
// 100.
func parseMix(s string) (loadgen.Mix, error) {
	var read, update, create int
	if _, err := fmt.Sscanf(s, "%d/%d/%d", &read, &update, &create); err != nil {
		return loadgen.Mix{}, fmt.Errorf("-mix=%q: want read/update/create, like 80/10/10", s)
	}
	if read < 0 || update < 0 || create < 0 || read+update+create != 100 {
		return loadgen.Mix{}, fmt.Errorf("-mix=%q: percentages must be 0 or more and add up to 100", s)
	}
	return loadgen.Mix{Read: read, Update: update}, nil
}

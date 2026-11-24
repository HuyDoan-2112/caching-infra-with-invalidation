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
	window := flag.Duration("window", 0, "if set, also print the p99 of every window this long, their average, and the p99 of all requests")
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
	perWindow := int(*window * time.Duration(*rate) / time.Second) // multiply first
	if *window < 0 || (*window > 0 && perWindow < 1) {
		return fmt.Errorf("-window=%v: must hold at least one request at -rate=%d", *window, *rate)
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
	measured := loadgen.NewSend(inFlight, client, *readURL, *writeURL, pick, mix, rec)

	// With -window, every window records into its own Recorder, and rec gets
	// them all merged after the run.
	var windows []*loadgen.Recorder
	if *window > 0 {
		n := int((*duration + *window - 1) / *window) // round up: a shorter last window still counts
		sends := make([]func(int, time.Time), n)
		windows = make([]*loadgen.Recorder, n)
		for w := range n {
			windows[w] = loadgen.NewRecorder()
			sends[w] = loadgen.NewSend(inFlight, client, *readURL, *writeURL, pick, mix, windows[w])
		}
		measured = loadgen.ByWindow(warmupN, perWindow, sends)
	}
	send := loadgen.SplitWarmup(warmupN,
		loadgen.NewSend(inFlight, client, *readURL, *writeURL, pick, mix, warmRec), measured)

	fmt.Printf("running %d req/s: %v warmup + %v measured, mix %s, dist %s\n",
		*rate, *warmup, *duration, *mixFlag, *dist)
	sent, dropped := loadgen.Schedule(ctx, *rate, *warmup+*duration, *maxInFlight, send)
	for _, w := range windows {
		rec.Merge(w)
	}

	fmt.Printf("\nwhole run: %d scheduled, %d sent, %d dropped\n\n", sent+dropped, sent, dropped)

	if err := warmRec.Report(os.Stdout, fmt.Sprintf("warmup (%v)", *warmup)); err != nil {
		return err
	}
	fmt.Println()

	if err := rec.Report(os.Stdout, fmt.Sprintf("measured (%v)", *duration)); err != nil {
		return err
	}
	if len(windows) > 0 {
		printWindows(*window, *duration, windows, rec)
	}
	return nil
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

// printWindows prints each window's p99 per kind, the plain average of those
// p99s, and the p99 of all the requests together (merged): the average is not
// a percentile of anything, the merged one is.
func printWindows(size, duration time.Duration, windows []*loadgen.Recorder, merged *loadgen.Recorder) {
	kinds := []loadgen.Kind{loadgen.ReadHit, loadgen.ReadMiss, loadgen.Update, loadgen.Create}
	row := func(label string, cells []string) {
		fmt.Printf("%-22s", label)
		for _, c := range cells {
			fmt.Printf(" %11s", c)
		}
		fmt.Println()
	}

	fmt.Printf("\np99 per %v window\n", size)
	row("window", []string{"read HIT", "read MISS", "update", "create"})
	sums := make([]time.Duration, len(kinds))
	counts := make([]int, len(kinds))
	for i, rec := range windows {
		cells := make([]string, len(kinds))
		for j, k := range kinds {
			if rec.Count(k) == 0 {
				cells[j] = "-"
				continue
			}
			p := rec.Percentile(k, 99)
			sums[j] += p
			counts[j]++
			cells[j] = p.Round(time.Microsecond).String()
		}
		from, to := time.Duration(i)*size, min(time.Duration(i+1)*size, duration)
		row(fmt.Sprintf("%v-%v", from, to), cells)
	}

	avg := make([]string, len(kinds))
	all := make([]string, len(kinds))
	for j, k := range kinds {
		avg[j], all[j] = "-", "-"
		if counts[j] > 0 {
			avg[j] = (sums[j] / time.Duration(counts[j])).Round(time.Microsecond).String()
			all[j] = merged.Percentile(k, 99).Round(time.Microsecond).String()
		}
	}
	row("average of the p99s", avg)
	row("p99 of all (merged)", all)
}

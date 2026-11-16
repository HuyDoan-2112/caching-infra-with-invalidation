package invalidator_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/invalidator"
)

// fakeDeleter fails its first `failures` calls, then succeeds. It records every id.
type fakeDeleter struct {
	failures int
	calls    []int64
}

func (f *fakeDeleter) Delete(_ context.Context, id int64) error {
	f.calls = append(f.calls, id)
	if len(f.calls) <= f.failures {
		return errors.New("redis down")
	}
	return nil
}

// newHandler uses 1ms-2ms backoff so retries take milliseconds, not seconds,
// and registers its metrics in reg.
func newHandler(d invalidator.Deleter, log *slog.Logger, reg prometheus.Registerer) *invalidator.Handler {
	return invalidator.NewHandler(d, log, time.Millisecond, 2*time.Millisecond, invalidator.NewMetrics(reg))
}

// TestHandle covers every case where Handle must return nil (commit the offset).
func TestHandle(t *testing.T) {
	update := readTestdata(t, "update.json")
	tests := []struct {
		name        string
		value       []byte  // nil means a tombstone
		failures    int     // DELs that fail before one succeeds
		wantCalls   []int64 // ids passed to Delete, in order
		wantSkipLog bool    // logged as unparseable
		wantDeletes string  // op counted in invalidator_deletes_total, "" for none
		wantSkipped string  // reason counted in invalidator_skipped_total, "" for none
		wantLag     uint64  // samples in invalidator_lag_seconds
	}{
		{name: "update", value: update, wantCalls: []int64{220}, wantDeletes: "u", wantLag: 1},
		{name: "snapshot row has no lag", value: readTestdata(t, "snapshot.json"), wantCalls: []int64{211}, wantDeletes: "r"},
		{name: "tombstone", value: nil, wantCalls: nil, wantSkipped: "tombstone"},
		{name: "poison", value: []byte(`{nope`), wantCalls: nil, wantSkipLog: true, wantSkipped: "unparseable"},
		{name: "redis recovers", value: update, failures: 2, wantCalls: []int64{220, 220, 220}, wantDeletes: "u", wantLag: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			reg := prometheus.NewRegistry()
			fake := &fakeDeleter{failures: tt.failures}
			err := newHandler(fake, slog.New(slog.NewTextHandler(&logs, nil)), reg).Handle(t.Context(), &kgo.Record{Value: tt.value})
			if err != nil {
				t.Fatalf("Handle() error: %v, want nil", err)
			}
			if !slices.Equal(fake.calls, tt.wantCalls) {
				t.Errorf("Delete calls = %v, want %v", fake.calls, tt.wantCalls)
			}
			if got := strings.Contains(logs.String(), "unparseable"); got != tt.wantSkipLog {
				t.Errorf("logged unparseable = %v, want %v; logs:\n%s", got, tt.wantSkipLog, logs.String())
			}
			for _, op := range []string{"c", "u", "d", "r"} {
				if got, want := sample(t, reg, "invalidator_deletes_total", op), boolCount(op == tt.wantDeletes); got != want {
					t.Errorf("invalidator_deletes_total{op=%q} = %v, want %v", op, got, want)
				}
			}
			for _, reason := range []string{"tombstone", "unparseable"} {
				if got, want := sample(t, reg, "invalidator_skipped_total", reason), boolCount(reason == tt.wantSkipped); got != want {
					t.Errorf("invalidator_skipped_total{reason=%q} = %v, want %v", reason, got, want)
				}
			}
			if got := sample(t, reg, "invalidator_delete_failures_total", ""); got != float64(tt.failures) {
				t.Errorf("invalidator_delete_failures_total = %v, want %d", got, tt.failures)
			}
			if got := sample(t, reg, "invalidator_lag_seconds", ""); got != float64(tt.wantLag) {
				t.Errorf("invalidator_lag_seconds samples = %v, want %d", got, tt.wantLag)
			}
		})
	}
}

// TestHandleStopsWhenContextEnds: if the service shuts down while Redis is still
// failing, Handle must return an error so the consumer does not commit.
func TestHandleStopsWhenContextEnds(t *testing.T) {
	var logs bytes.Buffer
	fake := &fakeDeleter{failures: 1 << 30}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	err := newHandler(fake, slog.New(slog.NewTextHandler(&logs, nil)), prometheus.NewRegistry()).Handle(ctx, &kgo.Record{Value: readTestdata(t, "update.json")})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Handle() error = %v, want context.DeadlineExceeded", err)
	}
	if len(fake.calls) == 0 {
		t.Error("Handle() made no Delete calls, want at least one")
	}
}

// readTestdata loads a real Debezium message captured by the cdc package.
func readTestdata(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "cdc", "testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// sample returns a metric's value from reg: a counter's value, or a histogram's
// sample count. label is the series' label value, or "" for a metric without
// labels. It fails the test if the series does not exist, so a typo in a name
// cannot pass as 0.
func sample(t *testing.T, reg prometheus.Gatherer, name, label string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if label != "" && (len(m.GetLabel()) != 1 || m.GetLabel()[0].GetValue() != label) {
				continue
			}
			if h := m.GetHistogram(); h != nil {
				return float64(h.GetSampleCount())
			}
			return m.GetCounter().GetValue()
		}
	}
	t.Fatalf("no series %s{%q}", name, label)
	return 0
}

// boolCount is 1 for true and 0 for false.
func boolCount(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

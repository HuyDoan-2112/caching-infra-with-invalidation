package item_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/item"
)

// discardLog is a logger for tests: the code under test logs, nobody reads it.
// It lives in this untagged file so the integration tests can use it too.
func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// counter returns the value of the counter series name{label=value} in reg.
// It fails the test if the series does not exist, so a typo in a metric or
// label name cannot pass as "0".
func counter(t *testing.T, reg prometheus.Gatherer, name, value string) float64 {
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
			for _, l := range m.GetLabel() {
				if l.GetValue() == value {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	t.Fatalf("no series %s{...=%q}", name, value)
	return 0
}

// wantCounters requires every listed series of name to have the given value.
func wantCounters(t *testing.T, reg prometheus.Gatherer, name string, want map[string]float64) {
	t.Helper()
	for value, n := range want {
		if got := counter(t, reg, name, value); got != n {
			t.Errorf("%s{%s} = %v, want %v", name, value, got, n)
		}
	}
}

const (
	requestsMetric    = "reader_cache_requests_total"
	redisErrorsMetric = "reader_cache_redis_errors_total"
)

func TestNewCacheMetricsStartsEverySeriesAtZero(t *testing.T) {
	reg := prometheus.NewRegistry()
	item.NewCacheMetrics(reg)

	wantCounters(t, reg, requestsMetric, map[string]float64{"hit": 0, "miss": 0, "not_found": 0, "error": 0})
	wantCounters(t, reg, redisErrorsMetric, map[string]float64{"get": 0, "set": 0, "decode": 0})
}

// fakeStore is the MySQL side of the cache: it returns err if set, otherwise
// an item with the requested id. calls counts how often the cache asked.
type fakeStore struct {
	err   error
	calls int
}

func (f *fakeStore) Get(_ context.Context, id int64) (*item.Item, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &item.Item{ID: id, Name: "from mysql"}, nil
}

// deadRedis returns a client for an address nothing listens on, with the
// same fail-fast settings as redisdb.Open, so every command fails quickly.
func deadRedis(t *testing.T) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{
		Addr:          "localhost:1",
		DialTimeout:   250 * time.Millisecond,
		MaxRetries:    -1,
		DialerRetries: 1,
	})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestCacheWithRedisDown covers every outcome that does not need a working
// Redis: the cache must fall back to the store, pass its errors through
// unchanged, and count what happened.
func TestCacheWithRedisDown(t *testing.T) {
	ctx := context.Background()
	errMySQL := errors.New("mysql is down")

	tests := []struct {
		name      string
		storeErr  error
		wantErr   error // nil: want the item back
		wantCount map[string]float64
	}{
		{
			name:      "found in MySQL is a miss",
			wantCount: map[string]float64{"hit": 0, "miss": 1, "not_found": 0, "error": 0},
		},
		{
			name:      "not found passes ErrNotFound through",
			storeErr:  item.ErrNotFound,
			wantErr:   item.ErrNotFound,
			wantCount: map[string]float64{"hit": 0, "miss": 0, "not_found": 1, "error": 0},
		},
		{
			name:      "MySQL failure passes the error through",
			storeErr:  errMySQL,
			wantErr:   errMySQL,
			wantCount: map[string]float64{"hit": 0, "miss": 0, "not_found": 0, "error": 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			store := &fakeStore{err: tt.storeErr}
			cache := item.NewCache(store, deadRedis(t), discardLog(), time.Minute, item.NewCacheMetrics(reg))

			it, hit, err := cache.Get(ctx, 7)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if hit {
				t.Errorf("hit = true, want false: Redis is down")
			}
			if tt.wantErr == nil && (it == nil || it.ID != 7 || it.Name != "from mysql") {
				t.Errorf("item = %+v, want id 7 from the store", it)
			}
			if store.calls != 1 {
				t.Errorf("store calls = %d, want 1", store.calls)
			}
			wantCounters(t, reg, requestsMetric, tt.wantCount)
			// The GET failed; the SET is skipped because Redis is known to be down.
			wantCounters(t, reg, redisErrorsMetric, map[string]float64{"get": 1, "set": 0, "decode": 0})
		})
	}
}

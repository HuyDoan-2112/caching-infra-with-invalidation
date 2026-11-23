package redisdb_test

import (
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/redisdb"
)

func TestPoolCollectors(t *testing.T) {
	// Nothing listens here: PoolStats works without a connection, and a fresh
	// pool reports all zeros.
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = rdb.Close() })

	reg := prometheus.NewRegistry()
	reg.MustRegister(redisdb.PoolCollectors(rdb, "reader")...) // panics on a bad or duplicate name

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	var got []string
	for _, f := range families {
		got = append(got, f.GetName())
		if n := len(f.GetMetric()); n != 1 {
			t.Errorf("%s: %d series, want 1", f.GetName(), n)
		}
	}
	want := []string{
		"reader_redis_pool_idle_connections",
		"reader_redis_pool_open_connections",
		"reader_redis_pool_pending_requests",
		"reader_redis_pool_timeouts_total",
		"reader_redis_pool_wait_count_total",
		"reader_redis_pool_wait_duration_seconds_total",
	}
	if !slices.Equal(got, want) { // Gather sorts by name
		t.Errorf("metrics = %q, want %q", got, want)
	}
}

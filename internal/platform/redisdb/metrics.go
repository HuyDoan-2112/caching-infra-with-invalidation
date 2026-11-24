package redisdb

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// PoolCollectors exposes rdb's connection-pool stats to Prometheus, named
// <service>_redis_pool_*. Each function reads a fresh rdb.PoolStats() when
// /metrics is scraped, so nothing runs on the request path. The wait and
// timeout counters are the Redis side of go_sql_wait_*: they show requests
// queueing for a connection, which the cache then turns into MySQL reads.
//
// The pool's own "hits" and "misses" (a free connection was or was not ready)
// are left out on purpose: they are easy to confuse with cache hits.
func PoolCollectors(rdb *redis.Client, service string) []prometheus.Collector {
	name := func(n string) string { return service + "_redis_pool_" + n }

	return []prometheus.Collector{
		// Counters: running totals since start, read them as a difference over time.
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: name("wait_count_total"),
			Help: "Times a Redis command had to wait for a free connection because all were busy.",
		}, func() float64 { return float64(rdb.PoolStats().WaitCount) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: name("wait_duration_seconds_total"),
			Help: "Total time Redis commands spent waiting for a free connection.",
		}, func() float64 { return float64(rdb.PoolStats().WaitDurationNs) / 1e9 }), // ns -> s
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: name("timeouts_total"),
			Help: "Times a Redis command gave up waiting for a free connection (PoolTimeout).",
		}, func() float64 { return float64(rdb.PoolStats().Timeouts) }),

		// Gauges: the state right now.
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: name("open_connections"),
			Help: "Connections in the pool right now, busy or idle.",
		}, func() float64 { return float64(rdb.PoolStats().TotalConns) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: name("idle_connections"),
			Help: "Connections open and unused right now.",
		}, func() float64 { return float64(rdb.PoolStats().IdleConns) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: name("pending_requests"),
			Help: "Redis commands waiting for a free connection right now.",
		}, func() float64 { return float64(rdb.PoolStats().PendingRequests) }),
	}
}

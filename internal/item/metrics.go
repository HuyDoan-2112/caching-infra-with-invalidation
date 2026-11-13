package item

import (
	"github.com/prometheus/client_golang/prometheus"
)

// CacheMetrics counts what the reader cache does, for Prometheus.
type CacheMetrics struct {
	requests    *prometheus.CounterVec // label: outcome
	redisErrors *prometheus.CounterVec // label: op
}

// NewCacheMetrics creates the counters and registers them with reg.
func NewCacheMetrics(reg prometheus.Registerer) *CacheMetrics {
	m := &CacheMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reader_cache_requests_total",
			Help: "Item reads by cache outcome: hit, miss, not_found, error.",
		}, []string{"outcome"}),
		redisErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reader_cache_redis_errors_total",
			Help: "Redis failures the cache absorbed by falling back to MySQL, by op: get, set, decode.",
		}, []string{"op"}),
	}
	reg.MustRegister(m.requests, m.redisErrors)

	// Create every series at 0 so all of them show up in /metrics from the start.
	for _, o := range []string{"hit", "miss", "not_found", "error"} {
		m.requests.WithLabelValues(o)
	}

	for _, op := range []string{"get", "set", "decode"} {
		m.redisErrors.WithLabelValues(op)
	}
	return m
}

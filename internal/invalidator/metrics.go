package invalidator

import (
	"time"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/cdc"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics counts what the invalidator does, for Prometheus.
type Metrics struct {
	deletes  *prometheus.CounterVec // label: op
	skipped  *prometheus.CounterVec // label: reason
	failures prometheus.Counter
	lag      prometheus.Histogram
}

// NewMetrics creates the metrics and registers them with reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		deletes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "invalidator_deletes_total",
			Help: "Cache keys deleted, by the change event's op: c, u, d, r.",
		}, []string{"op"}),
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "invalidator_skipped_total",
			Help: "Events committed without a DEL, by reason: tombstone, unparseable.",
		}, []string{"reason"}),
		failures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "invalidator_delete_failures_total",
			Help: "Failed DEL attempts. Each one is retried with backoff.",
		}),
		lag: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "invalidator_lag_seconds",
			Help:    "Time from the MySQL commit (source.ts_ms) to the DEL, for c, u and d events.",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		}),
	}
	reg.MustRegister(m.deletes, m.skipped, m.failures, m.lag)

	// Create every series at 0 so all of them show up in /metrics from the start.
	for _, op := range []string{"c", "u", "d", "r"} {
		m.deletes.WithLabelValues(op)
	}
	for _, r := range []string{"tombstone", "unparseable"} {
		m.skipped.WithLabelValues(r)
	}
	return m
}

// deleted records a successful DEL for ev, done at now.
func (m *Metrics) deleted(ev cdc.Event, now time.Time) {
	m.deletes.WithLabelValues(ev.Op).Inc()
	if ev.Op == "r" {
		return // a snapshot row's ts_ms is when the snapshot ran, not a commit
	}
	// max: the MySQL and invalidator clocks can disagree by a little.
	m.lag.Observe(max(0, now.Sub(ev.CommitAt).Seconds()))
}

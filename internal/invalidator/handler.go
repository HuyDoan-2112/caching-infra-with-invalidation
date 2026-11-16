// Package invalidator deletes cached items when their rows change.
package invalidator

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/cdc"
)

// Deleter drops the cached copy of one item. The real one (4d) is a Redis DEL.
type Deleter interface {
	Delete(ctx context.Context, id int64) error
}

// Handler turns one change event into a cache DEL, retrying until Redis accepts it.
type Handler struct {
	cache   Deleter
	log     *slog.Logger
	minWait time.Duration // first retry delay
	maxWait time.Duration // backoff cap
	metrics *Metrics
}

// NewHandler returns a Handler that backs off from minWait, doubling up to maxWait.
func NewHandler(cache Deleter, log *slog.Logger, minWait, maxWait time.Duration, metrics *Metrics) *Handler {
	return &Handler{cache: cache, log: log, minWait: minWait, maxWait: maxWait, metrics: metrics}
}

// Handle returns nil when the record is done or skipped, or ctx.Err() if ctx
// ends while the DEL is still failing (the record must then not be committed).
func (h *Handler) Handle(ctx context.Context, rec *kgo.Record) error {
	if rec.Value == nil {
		h.metrics.skipped.WithLabelValues("tombstone").Inc()
		return nil
	}

	ev, err := cdc.Parse(rec.Value)
	if err != nil {
		h.metrics.skipped.WithLabelValues("unparseable").Inc()
		h.log.Warn("skipping unparseable event",
			"partition", rec.Partition, "offset", rec.Offset, "err", err)
		return nil
	}

	wait := h.minWait
	for {
		err := h.cache.Delete(ctx, ev.ID)
		if err == nil {
			h.metrics.deleted(ev, time.Now())
			return nil
		}
		h.metrics.failures.Inc()
		h.log.Warn("cache delete failed, retrying",
			"id", ev.ID, "op", ev.Op, "retry_in", wait, "err", err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		wait = min(wait*2, h.maxWait)
	}
}

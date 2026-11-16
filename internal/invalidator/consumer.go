package invalidator

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// commitTimeout bounds the final commit, which runs after ctx is cancelled.
const commitTimeout = 5 * time.Second

// Run polls records and passes each to h. A batch's offsets are committed only
// after its records were handled, so a crash before the commit means
// redelivery, never a lost DEL. Run returns nil when ctx ends.
//
// The client must be built with kgo.DisableAutoCommit and kgo.BlockRebalanceOnPoll.
func Run(ctx context.Context, cl *kgo.Client, h *Handler, log *slog.Logger) error {
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			cl.AllowRebalance() // or Close blocks: leaving the group is a rebalance
			return nil
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			log.Warn("fetch failed", "topic", topic, "partition", partition, "err", err)
		})

		var done []*kgo.Record
		stopped := false
		for iter := fetches.RecordIter(); !iter.Done(); {
			rec := iter.Next()
			if err := h.Handle(ctx, rec); err != nil {
				stopped = true // ctx ended mid-retry: rec is not done
				break
			}
			done = append(done, rec)
		}
		commit(ctx, cl, done, log)
		cl.AllowRebalance()
		if stopped {
			return nil
		}
	}
}

// commit saves the offsets of handled records. It outlives ctx so the work
// finished before a shutdown is not redone after it.
func commit(ctx context.Context, cl *kgo.Client, done []*kgo.Record, log *slog.Logger) {
	if len(done) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
	defer cancel()
	if err := cl.CommitRecords(ctx, done...); err != nil {
		log.Warn("offset commit failed, records will be redelivered", "records", len(done), "err", err)
	}
}

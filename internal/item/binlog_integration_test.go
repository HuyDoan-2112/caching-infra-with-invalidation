//go:build integration

package item_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/item"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/kafka"
)

// Every write now reaches the cache asynchronously: MySQL binlog -> Debezium ->
// Kafka -> invalidator -> DEL. A test that reads the cache after a write must
// first wait for that DEL, or it races the invalidator. Sleeping would be slow
// or flaky; these helpers wait for the exact event instead.

// kafkaCfg says where the invalidator reads change events. Set in TestMain.
var kafkaCfg kafka.Config

// invalidatorTimeout bounds each wait. The pipeline normally takes well under
// a second; hitting this means a part of it is not running.
const invalidatorTimeout = 20 * time.Second

// markChanges returns where the change topic ends now. Events for writes made
// after the call are at or past these offsets.
func markChanges(t *testing.T) kadm.ListedOffsets {
	t.Helper()
	ends, err := newAdmin(t).ListEndOffsets(t.Context(), kafkaCfg.Topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		t.Fatalf("list end offsets of %s: %v", kafkaCfg.Topic, err)
	}
	return ends
}

// waitInvalidated waits until the invalidator has handled the first change
// event for item id after mark. The invalidator commits an offset only after
// its DEL succeeded, so "committed past the event" means "the DEL is done".
func waitInvalidated(t *testing.T, id int64, mark kadm.ListedOffsets) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), invalidatorTimeout)
	defer cancel()

	partition, offset := findChange(ctx, t, id, mark)

	adm := newAdmin(t)
	for {
		committed, err := adm.FetchOffsets(ctx, kafkaCfg.Group)
		if err == nil {
			if o, ok := committed.Lookup(kafkaCfg.Topic, partition); ok && o.At > offset {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("group %s did not commit past item %d's event (partition %d, offset %d) within %v: "+
				"is the invalidator running? (`make up`)", kafkaCfg.Group, id, partition, offset, invalidatorTimeout)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// findChange reads the change topic from mark and returns the position of the
// first event for item id. Events for one id share a partition, in commit order.
func findChange(ctx context.Context, t *testing.T, id int64, mark kadm.ListedOffsets) (partition int32, offset int64) {
	t.Helper()
	start := map[string]map[int32]kgo.Offset{kafkaCfg.Topic: {}}
	mark.Each(func(o kadm.ListedOffset) {
		start[o.Topic][o.Partition] = kgo.NewOffset().At(o.Offset)
	})
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaCfg.Brokers...), kgo.ConsumePartitions(start))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("no change event for item %d within %v: is the Debezium connector running? (`make up`)",
				id, invalidatorTimeout)
		}
		var found *kgo.Record
		fetches.EachRecord(func(r *kgo.Record) {
			if found == nil && keyID(r.Key) == id {
				found = r
			}
		})
		if found != nil {
			return found.Partition, found.Offset
		}
	}
}

// keyID reads the row id from a Debezium message key, {"id":42}.
func keyID(key []byte) int64 {
	var k struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(key, &k); err != nil {
		return 0
	}
	return k.ID
}

func newAdmin(t *testing.T) *kadm.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaCfg.Brokers...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return kadm.NewClient(cl)
}

// createSettled is create, plus a wait for the invalidator's DEL of the
// INSERT's event. Use it before reading through the cache: otherwise that late
// DEL can remove a copy the test just cached.
func createSettled(t *testing.T, h http.Handler, body string) item.Item {
	t.Helper()
	mark := markChanges(t)
	it := create(t, h, body)
	waitInvalidated(t, it.ID, mark)
	return it
}

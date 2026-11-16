//go:build integration

package invalidator_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/invalidator"
)

// chanDeleter reports every Delete on a channel. With fail set, every Delete
// fails, like a Redis that is down.
type chanDeleter struct {
	ids  chan int64
	fail bool
}

func (d *chanDeleter) Delete(_ context.Context, id int64) error {
	d.ids <- id
	if d.fail {
		return errors.New("redis down")
	}
	return nil
}

func brokers() string {
	if b := os.Getenv("KAFKA_BOOTSTRAP_SERVERS"); b != "" {
		return b
	}
	return "localhost:9092"
}

// newTopic returns a fresh topic name, so each test starts with an empty log
// and a group with no committed offsets.
func newTopic(t *testing.T) string {
	t.Helper()
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return "test-invalidator-" + hex.EncodeToString(b)
}

// produce writes values to topic, in order, and waits until Kafka has them.
func produce(t *testing.T, topic string, values ...[]byte) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers()), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for _, v := range values {
		if err := cl.ProduceSync(t.Context(), &kgo.Record{Topic: topic, Value: v}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

// consume runs the invalidator on topic (group = topic) until stop is called.
func consume(t *testing.T, topic string, d invalidator.Deleter) (stop func()) {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers()),
		kgo.ConsumerGroup(topic),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	h := invalidator.NewHandler(d, slog.New(slog.DiscardHandler), time.Millisecond, 10*time.Millisecond,
		invalidator.NewMetrics(prometheus.NewRegistry()))
	done := make(chan error, 1)
	go func() { done <- invalidator.Run(ctx, cl, h, slog.New(slog.DiscardHandler)) }()
	return func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run() = %v, want nil", err)
		}
		cl.Close()
	}
}

// next waits for the next Delete call.
func next(t *testing.T, ids <-chan int64) int64 {
	t.Helper()
	select {
	case id := <-ids:
		return id
	case <-time.After(30 * time.Second):
		t.Fatal("no Delete within 30s")
		return 0
	}
}

// A handled record's offset is committed: a restarted consumer in the same
// group starts after it.
func TestRunCommitsAfterDelete(t *testing.T) {
	topic := newTopic(t)
	produce(t, topic, readTestdata(t, "update.json")) // id 220

	first := &chanDeleter{ids: make(chan int64, 100)}
	stop := consume(t, topic, first)
	if id := next(t, first.ids); id != 220 {
		t.Fatalf("first Delete = %d, want 220", id)
	}
	stop()

	produce(t, topic, readTestdata(t, "snapshot.json")) // id 211, a marker
	second := &chanDeleter{ids: make(chan int64, 100)}
	stop = consume(t, topic, second)
	defer stop()
	if id := next(t, second.ids); id != 211 {
		t.Fatalf("after restart, first Delete = %d, want 211 (220 was committed)", id)
	}
}

// A record whose DEL never succeeded is not committed: after a restart it is
// delivered again (at-least-once).
func TestRunRedeliversWhenDeleteNeverSucceeded(t *testing.T) {
	topic := newTopic(t)
	produce(t, topic, readTestdata(t, "update.json")) // id 220

	down := &chanDeleter{ids: make(chan int64, 100), fail: true}
	stop := consume(t, topic, down)
	next(t, down.ids) // it tried, and failed
	stop()            // shutdown while Redis is still down

	up := &chanDeleter{ids: make(chan int64, 100)}
	stop = consume(t, topic, up)
	defer stop()
	if id := next(t, up.ids); id != 220 {
		t.Fatalf("after restart, first Delete = %d, want 220 redelivered", id)
	}
}

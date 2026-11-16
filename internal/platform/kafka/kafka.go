// Package kafka opens  Kafka consumer clients.
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Config says where to consume change events from.
type Config struct {
	Brokers []string // KAFKA_BOOTSTRAP_SERVERS, comma-separated
	Topic   string
	Group   string
}

// Open returns a consumer group client for at-least-once processing, and
// pings a broker once so a bad address fails at startup.
//
// The client never commits on its own (the caller commits after handling a
// batch), and holds rebalances between a poll and AllowRebalance, so the
// caller only commits partitions it still owns.
func Open(ctx context.Context, cfg Config) (*kgo.Client, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // a new group starts from the oldest event
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := cl.Ping(pingCtx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka ping %v: %w", cfg.Brokers, err)
	}
	return cl, nil
}

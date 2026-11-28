// Package kafka opens  Kafka consumer clients.
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// The balancers KAFKA_BALANCER can name.
const (
	// BalancerCooperativeSticky moves only the partitions that change owner;
	// the others keep being consumed during a rebalance (KIP-429).
	BalancerCooperativeSticky = "cooperative-sticky"
	// BalancerRange is eager: on every rebalance, every member gives up all its
	// partitions until the new assignment arrives. It is what the reference's
	// Java consumers end up using by default.
	BalancerRange = "range"
)

// sessionTimeout is how long the group waits for a member that stopped sending
// heartbeats (a crashed pod) before giving its partitions to the others. 30s like
// the reference; franz-go's default is 45s.
const sessionTimeout = 30 * time.Second

// Config says where to consume change events from, and how many group members
// one process runs.
type Config struct {
	Brokers []string // KAFKA_BOOTSTRAP_SERVERS, comma-separated
	Topic   string
	Group   string
	// Concurrency is the number of clients, each its own group member, that one
	// process runs (KAFKA_CONCURRENCY). Like Spring Kafka's setConcurrency.
	Concurrency int
	// Balancer is BalancerCooperativeSticky or BalancerRange (KAFKA_BALANCER).
	// The broker refuses a member whose balancer the group doesn't use
	// (INCONSISTENT_GROUP_PROTOCOL), so all members should name the same one.
	Balancer string
}

// Open returns a consumer group client for at-least-once processing, and
// pings a broker once so a bad address fails at startup. clientID names the
// member in the broker's group listing (kafka-consumer-groups.sh CLIENT-ID).
//
// The client never commits on its own (the caller commits after handling a
// batch), and holds rebalances between a poll and AllowRebalance, so the
// caller only commits partitions it still owns.
func Open(ctx context.Context, cfg Config, clientID string) (*kgo.Client, error) {
	balancer, err := groupBalancer(cfg.Balancer)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(clientID),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.Balancers(balancer),
		kgo.SessionTimeout(sessionTimeout),
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

func groupBalancer(name string) (kgo.GroupBalancer, error) {
	switch name {
	case BalancerCooperativeSticky:
		return kgo.CooperativeStickyBalancer(), nil
	case BalancerRange:
		return kgo.RangeBalancer(), nil
	}
	return nil, fmt.Errorf("kafka balancer %q: want %q or %q", name, BalancerCooperativeSticky, BalancerRange)
}

// Command invalidator reads row changes from Kafka (published by Debezium from
// the MySQL binlog) and deletes the matching item keys from the Redis cache.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/invalidator"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/config"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/health"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/httpserver"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/kafka"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/redisdb"
)

// Backoff for a failing DEL. The cap is small so that once Redis is back, the
// invalidator notices within about 2s and stale keys restored from AOF are
// cleared quickly
const (
	firstRetryWait = 100 * time.Millisecond
	maxRetryWait   = 2 * time.Second
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("invalidator stopped", "err", err)
		os.Exit(1)
	}
}

// run holds the real work, so its defers run before main calls os.Exit.
func run(log *slog.Logger) error {
	// ctx is cancelled on Ctrl+C (SIGINT) or `docker stop` (SIGTERM).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load("INVALIDATOR_PORT", "8082")
	if err != nil {
		return err
	}

	rdb, err := redisdb.Open(ctx, cfg.Redis)
	if err != nil {
		return err
	}
	defer func() {
		if err := rdb.Close(); err != nil {
			log.Warn("closing redis pool", "err", err)
		}
	}()
	log.Info("redis connected", "addr", net.JoinHostPort(cfg.Redis.Host, cfg.Redis.Port))

	// One client per group member: each owns its partitions alone,
	// so Run and its commit logic don't change. Client IDs
	// consumer-<group>-<n>.
	clients := make([]*kgo.Client, 0, cfg.Kafka.Concurrency)
	started := false
	defer func() { // once the members start, each one closes its own client
		if !started {
			for _, cl := range clients {
				cl.Close()
			}
		}
	}()
	for i := range cfg.Kafka.Concurrency {
		cl, err := kafka.Open(ctx, cfg.Kafka, fmt.Sprintf("consumer-%s-%d", cfg.Kafka.Group, i+1))
		if err != nil {
			return err
		}
		clients = append(clients, cl)
	}
	log.Info("kafka connected", "brokers", cfg.Kafka.Brokers, "topic", cfg.Kafka.Topic, "group", cfg.Kafka.Group,
		"members", cfg.Kafka.Concurrency, "balancer", cfg.Kafka.Balancer)

	ln, err := net.Listen("tcp", ":"+cfg.HTTPPort)
	if err != nil {
		return err
	}
	// Our own registry, as in the reader: /metrics shows only what is registered here.
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	metrics := invalidator.NewMetrics(reg)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	// No readiness checks, like the reference (Spring Boot's readiness group only
	// says the app has started). Startup already failed if Redis or Kafka was
	// unreachable, no Service sends the invalidator traffic, and a Redis outage
	// is handled by retrying each DEL, not by taking pods out.
	health.Register(mux, log)
	// The members and the HTTP server stop together: when the server returns, or
	// the last member does, cancel ends the rest.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	httpErr := make(chan error, 1)
	go func() { httpErr <- httpserver.Run(ctx, ln, mux, log) }()

	// One Handler for all members: it holds no state of its own, and the Redis
	// pool and the metrics are safe to share between goroutines.
	h := invalidator.NewHandler(invalidator.NewRedisDeleter(rdb), log, firstRetryWait, maxRetryWait, metrics)
	runErrs := make([]error, len(clients))
	var wg sync.WaitGroup
	started = true
	for i, cl := range clients {
		wg.Go(func() {
			mlog := log.With("member", i+1)
			runErrs[i] = invalidator.Run(ctx, cl, h, mlog)
			cl.Close() // leaves the group, so its partitions move at once
			// Like Spring Kafka's containers, a member that stops on its own
			// doesn't stop the others.
			if ctx.Err() == nil {
				mlog.Warn("member stopped, the others keep running", "err", runErrs[i])
			}
		})
	}
	wg.Wait()
	cancel()
	return errors.Join(errors.Join(runErrs...), <-httpErr)
}

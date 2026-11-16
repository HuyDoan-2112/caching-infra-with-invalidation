// Command invalidator reads row changes from Kafka (published by Debezium from
// the MySQL binlog) and deletes the matching item keys from the Redis cache.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

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

	cl, err := kafka.Open(ctx, cfg.Kafka)
	if err != nil {
		return err
	}
	defer cl.Close() // after Run returns: leaves the group so its partitions move at once
	log.Info("kafka connected", "brokers", cfg.Kafka.Brokers, "topic", cfg.Kafka.Topic, "group", cfg.Kafka.Group)

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
	health.Register(mux, log,
		health.Check{Name: "redis", Ping: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
		health.Check{Name: "kafka", Ping: cl.Ping},
	)
	// The consumer and the HTTP server stop together: when either returns,
	// cancel ends the other.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	httpErr := make(chan error, 1)
	go func() { httpErr <- httpserver.Run(ctx, ln, mux, log) }()

	h := invalidator.NewHandler(invalidator.NewRedisDeleter(rdb), log, firstRetryWait, maxRetryWait, metrics)
	runErr := invalidator.Run(ctx, cl, h, log)
	cancel()
	return errors.Join(runErr, <-httpErr)
}

// Command reader serves item reads: GET /api/v1/items/{id}, from MySQL with a
// Redis cache in front (cache-aside).
package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/item"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/config"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/health"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/httpserver"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/mysqldb"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/redisdb"
)

// cacheTTL bounds how long a cached item can be served stale if an
// invalidation is missed.
const cacheTTL = 10 * time.Minute

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("reader stopped", "err", err)
		os.Exit(1)
	}
}

// run holds the real work, so its defers run before main calls os.Exit.
func run(log *slog.Logger) error {
	// ctx is cancelled on Ctrl+C (SIGINT) or `docker stop` (SIGTERM).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load("READER_PORT", "8081")
	if err != nil {
		return err
	}

	db, err := mysqldb.Open(ctx, cfg.MySQL)
	if err != nil {
		return err
	}

	defer func() {
		if err := db.Close(); err != nil {
			log.Warn("closing mysql pool", "err", err)
		}
	}() // runs after Run returns: requests are drained before the pool closes

	log.Info("mysql connected",
		"addr", net.JoinHostPort(cfg.MySQL.Host, cfg.MySQL.Port),
		"max_open_conns", cfg.MySQL.MaxOpenConns)

	// Redis must be reachable at startup, so a wrong address fails loudly here.
	// Once running, the cache falls back to MySQL when Redis is down.
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

	ln, err := net.Listen("tcp", ":"+cfg.HTTPPort)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	health.Register(mux, log, health.Check{Name: "mysql", Ping: db.PingContext})
	cache := item.NewCache(item.NewStore(db), rdb, log, cacheTTL)
	item.NewReadHandler(cache, log).Register(mux)

	return httpserver.Run(ctx, ln, mux, log)
}

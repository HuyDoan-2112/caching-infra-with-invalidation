// Command reader serves item reads: GET /api/v1/items/{id}, straight from MySQL for now.
package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/item"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/config"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/health"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/httpserver"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/mysqldb"
)

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

	ln, err := net.Listen("tcp", ":"+cfg.HTTPPort)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	health.Register(mux, log, health.Check{Name: "mysql", Ping: db.PingContext})
	item.NewReadHandler(item.NewStore(db), log).Register(mux)

	return httpserver.Run(ctx, ln, mux, log)
}

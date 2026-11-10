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

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/config"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/httpserver"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/mysqldb"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("writer stopped", "err", err)
		os.Exit(1)
	}
}

// run holds the real work, so its defers run before main calls os.Exit.
func run(log *slog.Logger) error {
	// ctx is cancelled on Ctrl+C (SIGINT) or `docker stop` (SIGTERM).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load("WRITER_PORT", "8080")
	if err != nil {
		return err
	}

	db, err := mysqldb.Open(ctx, cfg.MySQL)
	if err != nil {
		return err
	}
	defer db.Close() // runs after Run returns: requests are drained before the pool closes
	log.Info("mysql connected",
		"addr", net.JoinHostPort(cfg.MySQL.Host, cfg.MySQL.Port),
		"max_open_conns", cfg.MySQL.MaxOpenConns)

	ln, err := net.Listen("tcp", ":"+cfg.HTTPPort)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	// healthz: the process is alive. No dependencies, so it never fails while we run
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	// readyz: we can serve traffic right now, which needs MySQL.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		pingCtx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := db.PingContext(pingCtx); err != nil {
			log.Warn("not ready", "err", err)
			http.Error(w, "mysql unreachable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready"))
	})

	return httpserver.Run(ctx, ln, mux, log)
}

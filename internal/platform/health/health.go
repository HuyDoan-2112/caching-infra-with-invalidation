// Package health serves the liveness (/healthz) and readiness (/readyz) endpoints.
package health

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// pingTimeout bounds each dependency check, so a hung dependency fails readiness
// quickly instead of hanging the orchestrator's probe.
const pingTimeout = time.Second

// Check is one dependency the service needs to serve traffic.
type Check struct {
	Name string                      // used in the 503 body and the log, e.g. "mysql"
	Ping func(context.Context) error // nil error means the dependency is usable
}

// Register adds GET /healthz and GET /readyz to mux.
//
// /healthz answers "ok" while the process runs; it checks nothing, so a slow
// dependency never gets a healthy process restarted.
// /readyz runs every check in order and answers 503 at the first failure.
func Register(mux *http.ServeMux, log *slog.Logger, checks ...Check) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		for _, c := range checks {
			if err := ping(r.Context(), c); err != nil {
				log.Warn("not ready", "check", c.Name, "err", err)
				http.Error(w, c.Name+" unreachable", http.StatusServiceUnavailable)
				return
			}
		}
		_, _ = w.Write([]byte("ready"))
	})
}

func ping(ctx context.Context, c Check) error {
	ctx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	return c.Ping(ctx)
}

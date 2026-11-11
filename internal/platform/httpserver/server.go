// Package httpserver runs an http.Server until its context is cancelled,
// then lets in-flight requests finish before returning.
package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
)

const shutdownTimeout = 10 * time.Second

// Run serves h on ln until ctx is cancelled, then shuts down gracefully.
// Run takes ownership of ln and closes it on every path.
// It returns nil on a clean shutdown, or the first error from serving or shutdown.
func Run(ctx context.Context, ln net.Listener, h http.Handler, log *slog.Logger) error {
	if h == nil {
		_ = ln.Close()
		return errors.New("httpserver: nil handler")
	}

	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serveErr := make(chan error, 1)

	go func() { serveErr <- srv.Serve(ln) }()
	log.Info("http server listening", "addr", ln.Addr().String())

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	log.Info("http server shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}

	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	log.Info("http server stopped")
	return nil
}

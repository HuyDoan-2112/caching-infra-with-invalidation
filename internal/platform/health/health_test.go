package health

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func serve(t *testing.T, path string, checks ...Check) (int, string) {
	t.Helper()
	mux := http.NewServeMux()
	Register(mux, slog.New(slog.NewTextHandler(io.Discard, nil)), checks...)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, strings.TrimSpace(rec.Body.String())
}

func ok(context.Context) error { return nil }

func TestHealthzIgnoresChecks(t *testing.T) {
	failing := Check{Name: "mysql", Ping: func(context.Context) error { return errors.New("down") }}
	code, body := serve(t, "/healthz", failing)
	if code != http.StatusOK || body != "ok" {
		t.Fatalf("got %d %q, want 200 \"ok\"", code, body)
	}
}

func TestReadyz(t *testing.T) {
	down := func(context.Context) error { return errors.New("connection refused") }
	tests := []struct {
		name     string
		checks   []Check
		wantCode int
		wantBody string
	}{
		{"no checks", nil, http.StatusOK, "ready"},
		{"all pass", []Check{{"mysql", ok}, {"redis", ok}}, http.StatusOK, "ready"},
		{"first failing check is named", []Check{{"mysql", ok}, {"redis", down}, {"kafka", down}},
			http.StatusServiceUnavailable, "redis unreachable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, body := serve(t, "/readyz", tt.checks...)
			if code != tt.wantCode || body != tt.wantBody {
				t.Fatalf("got %d %q, want %d %q", code, body, tt.wantCode, tt.wantBody)
			}
		})
	}
}

func TestReadyzBoundsSlowCheck(t *testing.T) {
	hung := Check{Name: "mysql", Ping: func(ctx context.Context) error {
		<-ctx.Done() // a well-behaved client returns when its context ends
		return ctx.Err()
	}}
	start := time.Now()
	code, _ := serve(t, "/readyz", hung)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", code)
	}
	if elapsed := time.Since(start); elapsed > 3*pingTimeout {
		t.Fatalf("readyz took %v, want about %v", elapsed, pingTimeout)
	}
}

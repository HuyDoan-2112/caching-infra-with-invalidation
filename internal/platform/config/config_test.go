package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/config"
)

func TestLoadCacheTTL(t *testing.T) {
	tests := []struct {
		name    string
		env     string // "" means unset: getenv treats empty as unset
		want    time.Duration
		wantErr bool
	}{
		{name: "unset uses the 10m default", env: "", want: 10 * time.Minute},
		{name: "seconds", env: "30s", want: 30 * time.Second},
		{name: "compound duration", env: "1h30m", want: 90 * time.Minute},
		{name: "not a duration", env: "abc", wantErr: true},
		{name: "bare number has no unit", env: "600", wantErr: true},
		{name: "zero would mean no expiry in Redis", env: "0s", wantErr: true},
		{name: "negative", env: "-1m", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CACHE_TTL", tt.env)

			cfg, err := config.Load("READER_PORT", "8081")
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "CACHE_TTL=") {
					t.Fatalf("err = %v, want an error naming CACHE_TTL", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if cfg.CacheTTL != tt.want {
				t.Errorf("CacheTTL = %v, want %v", cfg.CacheTTL, tt.want)
			}
		})
	}
}

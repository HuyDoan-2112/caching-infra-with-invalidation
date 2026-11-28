package config_test

import (
	"slices"
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

func TestLoadCacheLeaseTTL(t *testing.T) {
	tests := []struct {
		name    string
		env     string // "" means unset
		want    time.Duration
		wantErr bool
	}{
		{name: "unset uses the 2s default", env: "", want: 2 * time.Second},
		{name: "milliseconds", env: "500ms", want: 500 * time.Millisecond},
		{name: "not a duration", env: "abc", wantErr: true},
		{name: "zero would mean no expiry in Redis", env: "0s", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CACHE_LEASE_TTL", tt.env)

			cfg, err := config.Load("READER_PORT", "8081")
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "CACHE_LEASE_TTL=") {
					t.Fatalf("err = %v, want an error naming CACHE_LEASE_TTL", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if cfg.CacheLeaseTTL != tt.want {
				t.Errorf("CacheLeaseTTL = %v, want %v", cfg.CacheLeaseTTL, tt.want)
			}
		})
	}
}

func TestLoadKafkaBrokers(t *testing.T) {
	tests := []struct {
		name    string
		env     string // "" means unset: getenv treats empty as unset
		want    []string
		wantErr bool
	}{
		{name: "unset uses the default", env: "", want: []string{"localhost:9092"}},
		{name: "one broker", env: "kafka:9092", want: []string{"kafka:9092"}},
		{name: "several, with spaces", env: "a:9092, b:9092", want: []string{"a:9092", "b:9092"}},
		{name: "trailing comma", env: "a:9092,", want: []string{"a:9092"}},
		{name: "only commas", env: ",", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("KAFKA_BOOTSTRAP_SERVERS", tt.env)

			cfg, err := config.Load("INVALIDATOR_PORT", "8082")
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "KAFKA_BOOTSTRAP_SERVERS=") {
					t.Fatalf("err = %v, want an error naming KAFKA_BOOTSTRAP_SERVERS", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if !slices.Equal(cfg.Kafka.Brokers, tt.want) {
				t.Errorf("Kafka.Brokers = %q, want %q", cfg.Kafka.Brokers, tt.want)
			}
		})
	}
}

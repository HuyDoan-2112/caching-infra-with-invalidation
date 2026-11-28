// Package config reads service settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/kafka"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/mysqldb"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/redisdb"
)

// Config holds everything a service needs at startup.
type Config struct {
	HTTPPort string
	MySQL    mysqldb.Config
	Redis    redisdb.Config
	CacheTTL time.Duration
	// CacheLeaseTTL bounds how long a reader that took a fill lease and never
	// came back keeps other readers from filling that item.
	CacheLeaseTTL time.Duration
	Kafka         kafka.Config
}

// Load reads the environment, applies defaults, and reports every invalid
// value at once. portVar names this service's port variable (WRITER_PORT or
// READER_PORT), and defaultPort is used when it is unset.
func Load(portVar, defaultPort string) (Config, error) {
	cfg := Config{
		HTTPPort: getenv(portVar, defaultPort),
		MySQL: mysqldb.Config{
			Host:     getenv("MYSQL_HOST", "localhost"),
			Port:     getenv("MYSQL_PORT", "3306"),
			Database: getenv("MYSQL_DATABASE", "catalog"),
			User:     getenv("MYSQL_USER", "app"),
			Password: getenv("MYSQL_PASSWORD", "app"),
		},
		Redis: redisdb.Config{
			Host:     getenv("REDIS_HOST", "localhost"),
			Port:     getenv("REDIS_PORT", "6379"),
			Password: os.Getenv("REDIS_PASSWORD"), // empty means no auth
		},
		Kafka: kafka.Config{
			Brokers: splitList(getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")),
			Topic:   getenv("KAFKA_TOPIC", "cdc.catalog.items"),
			Group:   getenv("KAFKA_GROUP", "cache-invalidator"),
		},
	}

	var errs []error
	if err := checkPort(portVar, cfg.HTTPPort); err != nil {
		errs = append(errs, err)
	}
	if err := checkPort("MYSQL_PORT", cfg.MySQL.Port); err != nil {
		errs = append(errs, err)
	}
	if err := checkPort("REDIS_PORT", cfg.Redis.Port); err != nil {
		errs = append(errs, err)
	}

	raw := getenv("DB_MAX_OPEN_CONNS", "16")
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 { // 0 would mean "unlimited" to database/sql
		errs = append(errs, fmt.Errorf("DB_MAX_OPEN_CONNS=%q must be an integer >= 1", raw))
	}
	cfg.MySQL.MaxOpenConns = n

	cfg.CacheTTL, err = positiveDuration("CACHE_TTL", "10m")
	if err != nil {
		errs = append(errs, err)
	}
	cfg.CacheLeaseTTL, err = positiveDuration("CACHE_LEASE_TTL", "2s")
	if err != nil {
		errs = append(errs, err)
	}

	if len(cfg.Kafka.Brokers) == 0 {
		errs = append(errs, fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS=%q must list at least one host:port",
			os.Getenv("KAFKA_BOOTSTRAP_SERVERS")))
	}

	// More members than partitions is allowed: the extra ones join the group and
	// wait idle, as in the reference.
	raw = getenv("KAFKA_CONCURRENCY", "3")
	cfg.Kafka.Concurrency, err = strconv.Atoi(raw)
	if err != nil || cfg.Kafka.Concurrency < 1 {
		errs = append(errs, fmt.Errorf("KAFKA_CONCURRENCY=%q must be an integer >= 1", raw))
	}

	cfg.Kafka.Balancer = getenv("KAFKA_BALANCER", kafka.BalancerCooperativeSticky)
	if b := cfg.Kafka.Balancer; b != kafka.BalancerCooperativeSticky && b != kafka.BalancerRange {
		errs = append(errs, fmt.Errorf("KAFKA_BALANCER=%q must be %q or %q",
			b, kafka.BalancerCooperativeSticky, kafka.BalancerRange))
	}

	return cfg, errors.Join(errs...) // nil when errs is empty
}

// getenv returns the variable's value, or def when it is unset or empty.
func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// checkPort rejects anything that is not a TCP port number.
func checkPort(key, v string) error {
	p, err := strconv.Atoi(v)
	if err != nil || p < 1 || p > 65535 {
		return fmt.Errorf("%s=%q is not a port (1-65535)", key, v)
	}
	return nil
}

// positiveDuration reads key as a Go duration (def when unset) and rejects
// anything that does not parse or is not > 0: Redis would read 0 as "no expiry".
func positiveDuration(key, def string) (time.Duration, error) {
	raw := getenv(key, def)
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s=%q must be a duration > 0, like 10m, 30s or 500ms", key, raw)
	}
	return d, nil
}

// splitList splits a comma-separated list, trimming spaces and dropping empty entries.
func splitList(s string) []string {
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

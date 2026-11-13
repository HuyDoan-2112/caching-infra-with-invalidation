// Package config reads service settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/mysqldb"
	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/platform/redisdb"
)

// Config holds everything a service needs at startup.
type Config struct {
	HTTPPort string
	MySQL    mysqldb.Config
	Redis    redisdb.Config
	CacheTTL time.Duration
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

	rawTTL := getenv("CACHE_TTL", "10m")
	ttl, err := time.ParseDuration(rawTTL)
	if err != nil || ttl <= 0 {
		errs = append(errs, fmt.Errorf("CACHE_TTL=%q must be a duration > 0, like 10m or 30s", rawTTL))
	}
	cfg.CacheTTL = ttl

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

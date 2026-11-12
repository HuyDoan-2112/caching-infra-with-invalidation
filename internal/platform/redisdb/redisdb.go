// Package redisdb opens the Redis connection pool used by the reader's cache.
package redisdb

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config is what Open needs to reach Redis.
type Config struct {
	Host     string
	Port     string
	Password string // empty when Redis has no auth, as in local Compose
}

// Open builds the client (a connection pool) and pings Redis once, so a bad
// config or a down Redis fails at startup instead of on the first request.
func Open(ctx context.Context, cfg Config) (*redis.Client, error) {
	addr := net.JoinHostPort(cfg.Host, cfg.Port)

	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: cfg.Password,

		// Redis is a cache in front of MySQL: when it is slow or down, the
		// reader should give up quickly and read MySQL instead. The defaults
		// (5s timeouts, 3 command retries, 5 dial attempts) would make a
		// request wait seconds before falling back.
		DialTimeout:   250 * time.Millisecond,
		ReadTimeout:   200 * time.Millisecond,
		WriteTimeout:  200 * time.Millisecond,
		MaxRetries:    -1, // -1 disables retries; 0 would mean "default (3)"
		DialerRetries: 1,  // one dial attempt per new connection; <= 0 means "default (5)"

		// Also stop waiting when the request's ctx is cancelled or expires.
		ContextTimeoutEnabled: true,
	})

	// One ping, bounded by the dial timeout above. No retry loop: Compose
	// starts the reader only after Redis reports healthy.
	pingCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping %s: %w", addr, err)
	}
	return rdb, nil
}

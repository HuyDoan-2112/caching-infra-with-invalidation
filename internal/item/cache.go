package item

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cache serves items from Redis, falling back to inner (MySQL) on a miss and
// storing what it read for ttl. Redis failures are logged, never returned.
type Cache struct {
	inner Getter
	rdb   *redis.Client
	log   *slog.Logger
	ttl   time.Duration
}

// NewCache returns a Cache in front of inner.
func NewCache(inner Getter, rdb *redis.Client, log *slog.Logger, ttl time.Duration) *Cache {
	return &Cache{inner: inner, rdb: rdb, log: log, ttl: ttl}
}

// cacheKey is the Redis key for item id. The v1 prefix is bumped whenever
// the cached JSON shape changes, so old entries are never read.
func cacheKey(id int64) string {
	return "v1:item:" + strconv.FormatInt(id, 10)
}

// Get returns the item with id, from Redis when cached and from inner
// otherwise, caching what inner returned. The bool reports a cache hit.
// Errors come only from inner (ErrNotFound or a MySQL failure); Redis
// problems are logged and Get falls back to inner.
func (c *Cache) Get(ctx context.Context, id int64) (*Item, bool, error) {
	key := cacheKey(id)

	cached, redisOK := c.lookup(ctx, key)
	if cached != nil {
		return cached, true, nil
	}

	it, err := c.inner.Get(ctx, id)
	if err != nil {
		return nil, false, err
	}

	if redisOK {
		c.save(ctx, key, it)
	}

	return it, false, nil
}

// lookup reads key from Redis. It returns the cached item, or nil on a miss.
// The bool is false when Redis itself failed, so Get can skip the SET too.
func (c *Cache) lookup(ctx context.Context, key string) (*Item, bool) {
	b, err := c.rdb.Get(ctx, key).Bytes()

	if errors.Is(err, redis.Nil) {
		return nil, true
	}

	if err != nil {
		c.log.Warn("redis get failed", "key", key, "err", err)
		return nil, false
	}

	var it Item
	if err := json.Unmarshal(b, &it); err != nil {
		c.log.Warn("corrupt cache entry", "key", key, "err", err)
		return nil, true
	}

	return &it, true
}

func (c *Cache) save(ctx context.Context, key string, it *Item) {
	b, err := json.Marshal(it)
	if err != nil {
		c.log.Warn("encode cache entry", "key", key, "err", err)
		return
	}

	// this may store a row that changed after we read it.
	err = c.rdb.Set(ctx, key, b, c.ttl).Err()
	if err != nil {
		c.log.Warn("redis set failed", "key", key, "err", err)
		return
	}
}

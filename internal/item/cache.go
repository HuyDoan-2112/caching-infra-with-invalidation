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

// invalidateTimeout bounds one DEL. The write is already committed, so a slow
// Redis must not hold up the response for long.
const invalidateTimeout = 500 * time.Millisecond

// Cache serves items from Redis, falling back to inner (MySQL) on a miss and
// storing what it read for ttl. Redis failures are logged, never returned.
type Cache struct {
	inner   Getter
	rdb     *redis.Client
	log     *slog.Logger
	ttl     time.Duration
	metrics *CacheMetrics
}

// NewCache returns a Cache in front of inner.
func NewCache(inner Getter, rdb *redis.Client, log *slog.Logger, ttl time.Duration, metrics *CacheMetrics) *Cache {
	return &Cache{inner: inner, rdb: rdb, log: log, ttl: ttl, metrics: metrics}
}

// Invalidator drops cached copies of an item after a write.
type Invalidator interface {
	Invalidate(ctx context.Context, id int64)
}

// RedisInvalidator invalidates items by deleting their reader cache keys.
type RedisInvalidator struct {
	rdb *redis.Client
	log *slog.Logger
}

// NewRedisInvalidator returns a RedisInvalidator that deletes keys in rdb and
// logs failures to log.
func NewRedisInvalidator(rdb *redis.Client, log *slog.Logger) *RedisInvalidator {
	return &RedisInvalidator{rdb: rdb, log: log}
}

// Invalidate deletes item id's cache key. It runs even if ctx is cancelled
// (the client hung up after the commit), bounded by invalidateTimeout.
// Failures are logged, not returned: MySQL already has the change, and the
// stale copy expires with its TTL.
func (r *RedisInvalidator) Invalidate(ctx context.Context, id int64) {
	key := cacheKey(id)

	ctx = context.WithoutCancel(ctx)
	ctx, cancel := context.WithTimeout(ctx, invalidateTimeout)
	defer cancel()

	if err := r.rdb.Del(ctx, key).Err(); err != nil {
		r.log.Warn("redis del failed", "key", key, "err", err)
	}
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
		c.metrics.requests.WithLabelValues("hit").Inc()
		return cached, true, nil
	}

	it, err := c.inner.Get(ctx, id)
	switch {
	case errors.Is(err, ErrNotFound):
		c.metrics.requests.WithLabelValues("not_found").Inc()
		return nil, false, err
	case err != nil:
		c.metrics.requests.WithLabelValues("error").Inc()
		return nil, false, err
	}

	if redisOK {
		c.save(ctx, key, it)
	}

	c.metrics.requests.WithLabelValues("miss").Inc()
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
		c.metrics.redisErrors.WithLabelValues("get").Inc()
		c.log.Warn("redis get failed", "key", key, "err", err)
		return nil, false
	}
	var it Item
	if err := json.Unmarshal(b, &it); err != nil {
		c.metrics.redisErrors.WithLabelValues("decode").Inc()
		c.log.Warn("corrupt cache entry", "key", key, "err", err)
		return nil, true
	}

	return &it, true
}

// save stores it under key for c.ttl. Failures are logged and counted, and
// otherwise ignored: the caller already has the item.
func (c *Cache) save(ctx context.Context, key string, it *Item) {
	b, err := json.Marshal(it)
	if err != nil {
		c.log.Warn("encode cache entry", "key", key, "err", err)
		return
	}

	// Known race: this may store a row that changed after we read it.
	if err := c.rdb.Set(ctx, key, b, c.ttl).Err(); err != nil {
		c.metrics.redisErrors.WithLabelValues("set").Inc()
		c.log.Warn("redis set failed", "key", key, "err", err)
	}
}

package item

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cache serves items from Redis, falling back to inner (MySQL) on a miss. A
// miss stores what it read for ttl only while the reader holds the item's
// lease, so a row read before an invalidation is never cached after it.
// Redis failures are logged, never returned.
type Cache struct {
	inner    Getter
	rdb      *redis.Client
	log      *slog.Logger
	ttl      time.Duration
	leaseTTL time.Duration
	metrics  *CacheMetrics
}

// NewCache returns a Cache in front of inner.
func NewCache(inner Getter, rdb *redis.Client, log *slog.Logger, ttl, leaseTTL time.Duration, metrics *CacheMetrics) *Cache {
	return &Cache{inner: inner, rdb: rdb, log: log, ttl: ttl, leaseTTL: leaseTTL, metrics: metrics}
}

// CacheKey is the Redis key holding item id's cached copy. The reader SETs it,
// and the invalidator DELs it, so both must use this one function.
func CacheKey(id int64) string {
	return "v1:item:" + strconv.FormatInt(id, 10)
}

// LeaseKey is the Redis key for item id's fill lease: the token of the one
// reader allowed to cache it. The invalidator deletes it with CacheKey(id),
// so a fill that started before a change is refused.
func LeaseKey(id int64) string {
	return "v1:lease:" + strconv.FormatInt(id, 10)
}

// Get returns the item with id, from Redis when cached and from inner
// otherwise. On a miss, only the reader holding the item's lease caches what
// inner returned; other readers return it without caching. The bool reports a
// cache hit.
// Errors come only from inner (ErrNotFound or a MySQL failure); Redis
// problems are logged and Get falls back to inner.
func (c *Cache) Get(ctx context.Context, id int64) (*Item, bool, error) {
	key := CacheKey(id)

	cached, redisOK := c.lookup(ctx, key)
	if cached != nil {
		c.metrics.requests.WithLabelValues("hit").Inc()
		return cached, true, nil
	}

	// The lease must be taken before reading MySQL: an invalidation that
	// lands after this point deletes it, and the fill below is refused.
	var token string
	var holder bool
	if redisOK {
		token, holder = c.takeLease(ctx, id)
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

	if holder {
		c.fill(ctx, id, token, it)
	}

	c.metrics.requests.WithLabelValues("miss").Inc()
	return it, false, nil
}

// lookup reads key from Redis. It returns the cached item, or nil on a miss.
// The bool is false when Redis itself failed, so Get can skip the lease and
// the fill too.
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

// takeLease tries to take item id's fill lease for c.leaseTTL. It returns the
// token and true when this reader now holds the lease and may fill the cache.
// It returns false when another reader holds it (busy) or Redis failed;
// either way the caller reads MySQL and does not fill.
func (c *Cache) takeLease(ctx context.Context, id int64) (string, bool) {
	key := LeaseKey(id)
	token := rand.Text()

	ok, err := c.rdb.SetNX(ctx, key, token, c.leaseTTL).Result()
	if err != nil {
		c.metrics.redisErrors.WithLabelValues("lease").Inc()
		c.log.Warn("redis lease failed", "key", key, "err", err)
		return "", false
	}
	if !ok {
		c.metrics.lease.WithLabelValues("busy").Inc()
		return "", false // busy: another reader holds the lease
	}

	c.metrics.lease.WithLabelValues("acquired").Inc()
	return token, true
}

// fillScript stores the row only if the reader still holds the item's lease.
// KEYS[1] = item key, KEYS[2] = lease key.
// ARGV[1] = token, ARGV[2] = row JSON, ARGV[3] = item TTL in milliseconds.
// Returns 1 when stored, 0 when the lease is gone or belongs to another reader.
var fillScript = redis.NewScript(`
	if redis.call('GET', KEYS[2]) ~= ARGV[1] then
			return 0
	end
	redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
	redis.call('DEL', KEYS[2])
	return 1
`)

// fill caches it for c.ttl, but only if this reader still holds item id's
// lease (token). A refused fill means the item changed, or the lease
// expired, after the lease was taken. Failures are logged and counted, and
// otherwise ignored: the caller already has the item.
func (c *Cache) fill(ctx context.Context, id int64, token string, it *Item) {
	key := CacheKey(id)

	b, err := json.Marshal(it)
	if err != nil {
		c.log.Warn("encode cache entry", "key", key, "err", err)
		return
	}

	n, err := fillScript.Run(ctx, c.rdb, []string{key, LeaseKey(id)}, token, b, c.ttl.Milliseconds()).Int()
	if err != nil {
		c.metrics.redisErrors.WithLabelValues("set").Inc()
		c.log.Warn("redis fill failed", "key", key, "err", err)
		return
	}
	if n == 0 {
		c.metrics.lease.WithLabelValues("fill_rejected").Inc()
	}
}

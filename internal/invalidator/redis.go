package invalidator

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/HuyDoan-2112/cdc-cache-invalidation/internal/item"
)

// RedisDeleter deletes the reader's cache keys.
type RedisDeleter struct {
	rdb *redis.Client
}

// NewRedisDeleter returns a Deleter backed by rdb.
func NewRedisDeleter(rdb *redis.Client) *RedisDeleter {
	return &RedisDeleter{rdb: rdb}
}

// Delete removes item id's cache key. A key that is already gone is not an
// error: DEL is idempotent, which is what makes redelivery safe.
func (d *RedisDeleter) Delete(ctx context.Context, id int64) error {
	return d.rdb.Del(ctx, item.CacheKey(id)).Err()
}

var _ Deleter = (*RedisDeleter)(nil) // compile error if RedisDeleter stops matching Deleter

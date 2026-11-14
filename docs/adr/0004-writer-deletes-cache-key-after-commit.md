# 4. The writer deletes the cache key after commit (dual write)

Status: accepted (Stage 2). Stage 3 moves the delete out of the writer.

## Context

The reader caches items in Redis (`v1:item:{id}`, cache-aside, TTL `CACHE_TTL`). With
only a TTL, every `PUT` or `DELETE` left readers on the old value for up to the TTL.

The simplest fix is for the writer to invalidate the cache itself. That makes each write
touch two systems, MySQL and Redis, with no transaction covering both.

## Decision

1. **DEL, not SET.** After a write the writer deletes the key; the next read reloads it
   from MySQL. Two writers that SET could land in Redis in the opposite order from their
   commits and leave the older value cached. A DEL carries no value, so its order does not
   matter.
2. **After the commit, before the response.** Deleting before the commit lets a reader
   cache the old row again before the new one exists. Deleting before responding means a
   client that writes and then reads sees its own write.
3. **A failed DEL does not fail the write.** It is logged (`redis del failed`) and the
   client gets `200`/`204`. The commit already happened; an error would make the client
   believe the write failed and retry a write that succeeded. The cost is a stale copy
   for up to the TTL.
4. **The DEL outlives the request.** It runs on `context.WithoutCancel` with a 500ms
   bound, so a client hanging up after the commit does not cancel it. The client's
   200ms read/write timeouts (`redisdb.Open`) cut it off first.

`TestWriteInvalidatesCache` and `TestWriteWithRedisDown` pin points 1-3.

## Consequences

Known gaps, each leaving MySQL new and Redis old until the TTL or the next write to the
item (measured in `learning/stage-2/results.md`):

- **Crash between COMMIT and DEL.** Nothing records that a DEL is owed. The client gets
  no response at all.
- **Redis unreachable.** The DEL is lost. With AOF on, the stale key also survives a
  Redis restart.
- **Read-repopulate race.** A reader that read the old row before the commit SETs it
  after the DEL, with a fresh full TTL. `TestReadRepopulateRaceCachesStaleRow` forces it.
  No placement of the DEL in the writer closes it.
- **A timed-out DEL may still run.** When the bytes were already sent, Redis can execute
  it after the client gave up. The log says "failed" for a DEL that happened, so logs
  over-count lost invalidations. Harmless for a DEL (it is idempotent), but it means
  "timeout" must be read as "unknown".
- Every write costs one more Redis round trip (about 1 ms locally), and up to the 200ms
  timeout when Redis hangs.

## Alternatives considered

- **SET the new value on write.** Rejected: writer ordering (point 1).
- **Return an error when the DEL fails.** Rejected: point 3.
- **Retry the DEL in the writer.** Rejected for now: retries in a request only shrink the
  Redis-down gap, add latency, and still lose the DEL on a crash. A durable record of
  "this item changed" is the real fix, which is Stage 3 (the binlog).
- **Delayed double delete, versioned compare-and-set, leases.** These target the race;
  left to Stage 4.

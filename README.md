# cdc-cache-invalidation

A Go service that keeps a Redis cache in sync with MySQL by reading MySQL's own change
log (the **binlog**), instead of trusting application code to update the cache.

> Built pair-coding with **Claude** (Anthropic) through design, reviews, and tests.

## What is this

Caching makes reads fast, but it creates a hard problem: **when data changes in the
database, the cached copy becomes wrong (stale).** Something has to remove it.

The common approach is for the code that writes to the database to also delete the cache
entry. That breaks in subtle ways: the process can crash between the two steps, the cache
can be unreachable at that moment, or concurrent requests can interleave badly.

This project instead uses **change data capture (CDC)**: MySQL already records every
committed change in its binlog, so a separate service reads that log and deletes the
affected cache keys. If the database committed a change, the invalidation will happen,
even if the writer crashed right after.

## How it works

```
                 POST / PUT / DELETE
 client ───────────────────────────────▶ writer ──────────▶ MySQL
                                        (:8080)   SQL       │
                                                            │ binlog (every committed row change)
                                                            ▼
                                                        Debezium
                                                            │ change events
                                                            ▼
                                                          Kafka
                                                  topic cdc.catalog.items
                                                            │
                                                            ▼
                                                       invalidator ──── DEL v1:item:{id} ───┐
                                                         (:8082)                            │
                                                                                            ▼
 client ◀───────────────────────────── reader ◀──── cache hit ──────────────────────────  Redis
          GET /api/v1/items/{id}       (:8081)
          X-Cache: HIT | MISS             │
                                          └── cache miss: read MySQL, then store in Redis (TTL 10m)
```

Three paths:

- **Write path** - the writer only talks to MySQL. It never touches the cache.
- **Read path** - the reader checks Redis first (cache-aside). On a miss it reads MySQL
  and stores the result in Redis with a TTL.
- **Invalidation path** - Debezium streams binlog changes into Kafka. The invalidator
  consumes them and deletes the matching Redis key, so the next read fetches fresh data.

The result is **eventual consistency**: shortly after a write commits, readers see the
new value. The known edge cases and how they are handled are documented in
[docs/adr/](docs/adr/).

## Project structure

```
cdc-cache-invalidation/
├── cmd/                  One folder per runnable program (the folder name is the binary name)
│   ├── writer/           HTTP API for create / update / delete, MySQL only
│   ├── reader/           HTTP API for reads, cache-aside through Redis
│   ├── invalidator/      Kafka consumer that deletes stale cache keys
│   └── loadgen/          Open-loop load generator for the writer and reader
├── internal/             Shared Go packages used by the programs above
│                         (Go forbids importing these from outside this module)
├── deploy/               Docker Compose, Kubernetes and infrastructure config
│   ├── docker-compose.yml
│   ├── k8s/              Kustomize manifests for a local kind cluster (make k8s-up)
│   ├── connect/          Debezium connector config, shared by Compose and kind
│   └── mysql/            MySQL image with the binlog settings and the schema built in
├── docs/
│   └── adr/              Architecture Decision Records: why each design choice was made
├── .github/workflows/    CI: `make check` and a Compose smoke test
├── Dockerfile            Builds any program: --build-arg SERVICE=writer|reader|invalidator
├── Makefile              Developer commands: check, up, down, kind-up, k8s-up, tools
├── go.mod                Go module definition
└── README.md
```

## Status

Work in progress. The writer (`:8080`) serves `POST /api/v1/items` and
`PUT`/`DELETE /api/v1/items/{id}`. The reader (`:8081`) serves `GET /api/v1/items/{id}`
through a Redis cache (cache-aside, TTL from `CACHE_TTL`, default 10 minutes) and marks
each response `X-Cache: HIT` or `MISS`. If Redis fails, the reader falls back to MySQL.
Both answer health checks (`/healthz`, `/readyz`); the reader also serves Prometheus
metrics on `/metrics`.

The writer only writes MySQL. Debezium reads the MySQL binlog and publishes each row change
to Kafka (`cdc.catalog.items`), and the invalidator (`:8082`) deletes the item's Redis key
for every change, including changes made outside the writer. It commits its Kafka offset only
after the delete, and it retries while Redis is down. A read right after a write can still
return the old value for about 0.3s at p50 and 0.9s at p99. The invalidator also serves
`/healthz`, `/readyz` and `/metrics`. `make up` starts the whole stack and registers the
Debezium connector.

A reader that misses takes a short **fill lease** first (`v1:lease:{id}`, `CACHE_LEASE_TTL`,
default 2s), and caches the row only if a Lua script finds its lease still in place. The
invalidator deletes the lease together with the item key, so a reader that read the old row
before a change can no longer cache it after the change's delete (the read-repopulate race).
Other readers that miss while the lease is held read MySQL without caching.

`cmd/loadgen` sends load on a fixed timetable (open loop): request `i` goes out at
`start + i × interval` whether or not earlier ones were answered, and its latency counts
from when it was due, so a stall shows up in every request it delayed instead of in one.
It seeds items through the writer, mixes reads, updates and creates, and reports p50 to
max per kind (read HIT, read MISS, update, create), drops, errors and the hit ratio, with
the warmup reported on its own. On one 16-CPU machine running the whole stack, with the
default 80/10/10 mix, writes are the first limit, at about 1,000 commits a second
(6,000 req/s), because every commit waits for a disk flush; reads hold until somewhere
between 6,000 and 8,000 req/s, when every CPU is busy. The reader also exports its Redis
pool's waits and timeouts on `/metrics`.

The same system also runs on Kubernetes, in a local kind cluster with one control-plane
node and two workers. MySQL, Kafka, Kafka Connect and Redis run as StatefulSets with their
own disks. Two writers and three readers sit behind CPU autoscalers (2 to 6 and 3 to 10
pods), three invalidators share the topic's 6 partitions, and ingress-nginx routes
`writer.cache.local` and `reader.cache.local` from the host's port 80.

## How to run

Prerequisites: Go 1.27+, Docker with Compose v2, make.

```bash
make up                          # build images, start MySQL, Redis, writer, reader
curl localhost:8080/readyz       # writer: "ready" once MySQL is reachable
curl localhost:8081/readyz       # reader
curl -i localhost:8081/api/v1/items/1   # X-Cache: MISS the first time, HIT after
curl localhost:8081/metrics      # reader_cache_* counters, MySQL pool stats
make down                        # stop everything (data is kept in volumes)
```

Development:

```bash
make tools                       # once: install the pinned linter and load tester
make check                       # formatting, vet, lint, tests with the race detector
make test-integration            # also the API tests against the Compose MySQL and Redis (after make up)
make run-writer                  # run the writer on your machine against the Compose MySQL
```

Load (after `make up`):

```bash
go run ./cmd/loadgen -h                                  # every flag and its default
go run ./cmd/loadgen -rate=2000 -duration=30s            # 10s warmup, then 30s measured
go run ./cmd/loadgen -rate=2000 -dist=zipf -window=10s   # hot items; p99 per 10s window
```

### Kubernetes (kind)

Prerequisites: [kind](https://kind.sigs.k8s.io/) v0.33+, kubectl, and the host's port 80 free.

```bash
make kind-up       # kind cluster (1 control plane + 2 workers, host :80 mapped), ingress-nginx, metrics-server
make k8s-images    # build the four images: MySQL with the schema, writer, reader, invalidator
make k8s-load      # copy them into the kind nodes, so no registry is needed
make k8s-up        # kubectl apply -k deploy/k8s, wait for every rollout and the connector Job
```

From no cluster to a running system takes about 4 minutes on one machine. Point the two
host names at the ingress, then run the same round trip as with Compose:

```bash
echo "127.0.0.1 writer.cache.local reader.cache.local" | sudo tee -a /etc/hosts

curl -s -X POST http://writer.cache.local/api/v1/items \
  -H 'Content-Type: application/json' \
  -d '{"name":"widget","description":"shiny","price_cents":1999,"currency":"USD","stock":7}'
curl -si http://reader.cache.local/api/v1/items/1 | grep -i x-cache   # MISS, now cached
curl -si http://reader.cache.local/api/v1/items/1 | grep -i x-cache   # HIT
curl -s -X PUT http://writer.cache.local/api/v1/items/1 \
  -H 'Content-Type: application/json' \
  -d '{"name":"widget v2","description":"shinier","price_cents":2499,"currency":"USD","stock":5}'
curl -si http://reader.cache.local/api/v1/items/1 | grep -i x-cache   # MISS: the invalidator deleted the key
```

After rebuilding an image, run `make k8s-images k8s-load`, then
`kubectl rollout restart deploy/<name>`: the tag is still `dev`, so the pods don't restart
on their own.

```bash
make k8s-down      # delete the namespace and everything in it, data included
make kind-down     # delete the cluster
```

ingress-nginx is retired: its maintainers stopped releasing it in March 2026. `kind-up` pins
its last release, controller-v1.15.1. Its successor is the Gateway API.

## Acknowledgements

Pair-coded with **Claude** (Anthropic) for design discussions, code reviews, and tests.

## Team

- [HuyDoan-2112](https://github.com/HuyDoan-2112)

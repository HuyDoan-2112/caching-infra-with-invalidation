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
                                                         (:8090)                            │
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
│   └── invalidator/      Kafka consumer that deletes stale cache keys
├── internal/             Shared Go packages used by the programs above
│                         (Go forbids importing these from outside this module)
├── deploy/               Docker Compose and infrastructure config
│   ├── docker-compose.yml
│   └── mysql/            MySQL image with binlog settings, and the schema
├── docs/
│   └── adr/              Architecture Decision Records: why each design choice was made
├── .github/workflows/    CI: `make check` and a Compose smoke test
├── Dockerfile            Builds any program: --build-arg SERVICE=writer|reader|invalidator
├── Makefile              Developer commands: check, up, down, run-writer, tools
├── go.mod                Go module definition
└── README.md
```

## Status

Work in progress. The writer (`:8080`) and reader (`:8081`) start, connect to MySQL, and
answer health checks (`/healthz`, `/readyz`). The item API, the Redis cache, and
binlog-driven invalidation are not built yet.

## How to run

Prerequisites: Go 1.27+, Docker with Compose v2, make.

```bash
make up                          # build images, start MySQL, Redis, writer, reader
curl localhost:8080/readyz       # writer: "ready" once MySQL is reachable
curl localhost:8081/readyz       # reader
make down                        # stop everything (data is kept in volumes)
```

Development:

```bash
make tools                       # once: install the pinned linter and load tester
make check                       # formatting, vet, lint, tests with the race detector
make run-writer                  # run the writer on your machine against the Compose MySQL
```

## Acknowledgements

Pair-coded with **Claude** (Anthropic) for design discussions, code reviews, and tests.

## Team

- [HuyDoan-2112](https://github.com/HuyDoan-2112)

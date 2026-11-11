# syntax=docker/dockerfile:1
# Builds one Go service. Build context is the repo root:
#   docker build --build-arg SERVICE=writer -t cdc-writer .

# --- build: compile a static binary ---
FROM golang:1.27.3 AS build
WORKDIR /src

# Modules first, in their own step: editing code does not re-download dependencies.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY cmd ./cmd
COPY internal ./internal

ARG SERVICE
# CGO_ENABLED=0 gives a pure Go binary with no libc dependency, so it runs on distroless/static.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    test -n "$SERVICE" || { echo "set --build-arg SERVICE=writer|reader|invalidator"; exit 1; }; \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/$SERVICE

# --- runtime: only the binary ---
# debug-nonroot adds busybox (/busybox/wget) for the Compose healthcheck, like the reference.
FROM gcr.io/distroless/static-debian13:debug-nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]

# Developer commands. `make check` must pass before every PR.

GOLANGCI_LINT_VERSION := v2.15.0
HEY_VERSION           := v0.1.4
COMPOSE               := docker compose --env-file .env -f deploy/docker-compose.yml

.DEFAULT_GOAL := check
.PHONY: check fmt fmt-check vet lint test test-integration tools up down run-writer

# check runs everything CI runs: formatting, vet, lint, tests with the race detector.
check: fmt-check vet lint test

fmt:
	gofmt -w cmd internal

fmt-check:
	@files=$$(gofmt -l cmd internal); \
	if [ -n "$$files" ]; then echo "not formatted (run: make fmt):"; echo "$$files"; exit 1; fi

vet:
	go vet ./...

lint:
	golangci-lint run ./...

test:
	go test -race ./...

# test-integration also runs the tests tagged `integration`, against the Compose MySQL (`make up` first).
test-integration: .env
	set -a && . ./.env && set +a && go test -race -count=1 -tags integration ./...

# tools installs pinned dev tools into $(go env GOPATH)/bin.
tools:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	go install github.com/rakyll/hey@$(HEY_VERSION)

.env:
	cp .env.example .env

up: .env
	$(COMPOSE) up -d --wait --build

down:
	$(COMPOSE) down

run-writer: .env
	set -a && . ./.env && set +a && go run ./cmd/writer

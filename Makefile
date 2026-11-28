# Developer commands. `make check` must pass before every PR.

GOLANGCI_LINT_VERSION := v2.15.0
HEY_VERSION           := v0.1.4
COMPOSE               := docker compose --env-file .env -f deploy/docker-compose.yml

.DEFAULT_GOAL := check
.PHONY: check fmt fmt-check vet lint test test-integration tools up down run-writer connector \
	kind-up kind-down k8s-images k8s-load k8s-up k8s-down

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
	$(MAKE) connector

down:
	$(COMPOSE) down

# connector creates the change topic, then registers (or updates) the Debezium connector.
# Both steps are idempotent: safe to rerun. The topic is created here because Debezium
# creates it only with its first event, which on an empty table is the first write.
connector:
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic cdc.catalog.items --partitions 6 --replication-factor 1
	curl -fsS -X PUT -H 'Content-Type: application/json' \
		--data @deploy/connect/catalog-items.json \
		http://localhost:$${CONNECT_HOST_PORT:-8083}/connectors/catalog-items/config
	@echo

run-writer: .env
	set -a && . ./.env && set +a && go run ./cmd/writer

# --- kind (local Kubernetes) ---
# kind-up, k8s-images, k8s-load, k8s-up: a cold start from nothing to the whole system.

KIND_CLUSTER           := cdc-cache-invalidation
K8S_DIR                := deploy/k8s
K8S_NS                 := cdc-cache-invalidation
KUBECTL                := kubectl --context kind-$(KIND_CLUSTER)
INGRESS_NGINX_VERSION  := controller-v1.15.1
METRICS_SERVER_VERSION := v0.9.1
K8S_IMAGES             := cdc-mysql:8.4 cdc-writer:dev cdc-reader:dev cdc-invalidator:dev

# kind-up creates the cluster (if missing) and the two add-ons the manifests need:
# ingress-nginx, pinned to its last release and to the node that gets the host's port 80,
# and metrics-server, for the HPAs, which can't verify kind's self-signed kubelet
# certificates without --kubelet-insecure-tls (apply resets the args, so the flag is
# added back only if missing). Safe to rerun.
kind-up:
	kind get clusters | grep -qx $(KIND_CLUSTER) || kind create cluster --config $(K8S_DIR)/kind-cluster.yaml
	$(KUBECTL) apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/$(INGRESS_NGINX_VERSION)/deploy/static/provider/kind/deploy.yaml
	$(KUBECTL) -n ingress-nginx patch deploy ingress-nginx-controller --type=merge \
		-p '{"spec":{"template":{"spec":{"nodeSelector":{"ingress-ready":"true","kubernetes.io/os":"linux"}}}}}'
	$(KUBECTL) apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/$(METRICS_SERVER_VERSION)/components.yaml
	$(KUBECTL) -n kube-system get deploy metrics-server -o jsonpath='{.spec.template.spec.containers[0].args}' \
		| grep -q kubelet-insecure-tls || $(KUBECTL) -n kube-system patch deploy metrics-server --type=json \
		-p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
	$(KUBECTL) -n ingress-nginx rollout status deploy/ingress-nginx-controller --timeout=5m
	$(KUBECTL) -n kube-system rollout status deploy/metrics-server --timeout=5m

# kind-down deletes the whole cluster, data included.
kind-down:
	kind delete cluster --name $(KIND_CLUSTER)

# k8s-images builds the four images the manifests use; k8s-load copies them into every
# kind node (there is no registry). After loading a new build under the same tag, run
# `kubectl rollout restart deploy/<name>`: the pod spec didn't change, so nothing restarts.
k8s-images:
	docker build -t cdc-mysql:8.4 deploy/mysql
	for s in writer reader invalidator; do \
		docker build --build-arg SERVICE=$$s -t cdc-$$s:dev . || exit 1; \
	done

k8s-load:
	kind load docker-image $(K8S_IMAGES) --name $(KIND_CLUSTER)

# k8s-up applies everything and waits until it's running and the connector is registered.
k8s-up:
	$(KUBECTL) apply -k $(K8S_DIR)
	$(KUBECTL) -n $(K8S_NS) rollout status sts/mysql sts/kafka sts/kafka-connect sts/redis --timeout=5m
	$(KUBECTL) -n $(K8S_NS) rollout status deploy/writer deploy/reader deploy/invalidator --timeout=5m
	$(KUBECTL) -n $(K8S_NS) wait --for=condition=complete job/register-connector --timeout=5m

# k8s-down deletes the namespace and everything in it, disks (PVCs) included.
k8s-down:
	$(KUBECTL) delete -k $(K8S_DIR) --ignore-not-found

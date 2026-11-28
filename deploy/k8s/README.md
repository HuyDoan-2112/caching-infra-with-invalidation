# Kubernetes deployment

The whole system on a local [kind](https://kind.sigs.k8s.io/) cluster, as a flat Kustomize
base: `kubectl apply -k deploy/k8s` installs everything into the `cdc-cache-invalidation`
namespace. The number prefixes are only for reading order; Kustomize sorts objects by kind.

```
deploy/k8s/
├── kind-cluster.yaml          kind config: 1 control plane + 2 workers, host :80/:443 to the control plane
├── kustomization.yaml         entry point: namespace, shared labels, image tags, the file list
├── 00-namespace.yaml          the namespace, Pod Security level baseline
├── 01-secret.yaml             MySQL credentials (dev values, the same as .env.example)
├── 02-configmap.yaml          app-env: hosts, ports, cache TTLs, Kafka topic and group
├── 11-mysql.yaml              StatefulSet + headless Service + 5Gi disk, image cdc-mysql (my.cnf and schema built in)
├── 15-kafka.yaml              single-node KRaft StatefulSet + headless Service + 5Gi disk
├── 16-kafka-connect.yaml      Debezium Kafka Connect StatefulSet + Service on :8083
├── 17-debezium-connector.yaml Job: create the 6-partition topic, then PUT the connector config
├── 20-redis.yaml              StatefulSet + headless Service + 1Gi disk, AOF and LRU as in Compose
├── 30-writer.yaml             Deployment + Service on :8080
├── 31-reader.yaml             Deployment + Service on :8081
├── 32-invalidator.yaml        Deployment, 3 pods in one consumer group, no Service
├── 40-hpa.yaml                CPU autoscalers: writer 2 to 6 pods, reader 3 to 10
└── 99-ingress.yaml            ingress-nginx: writer.cache.local and reader.cache.local on port 80
```

The connector config itself is `deploy/connect/catalog-items.json`, the file Compose uses
too. `deploy/connect/kustomization.yaml` turns it into the `debezium-connector` ConfigMap,
and `kustomization.yaml` includes that folder.

## Data flow

```
host :80 ──► ingress-nginx ──► writer ──INSERT/UPDATE/DELETE──► MySQL
                    │                                             │ binlog
                    │                                             ▼
                    │                                   Kafka Connect (Debezium)
                    │                                             │
                    │                                             ▼
                    │                                  Kafka topic cdc.catalog.items (6 partitions)
                    │                                             │
                    │                                             ▼
                    │                              invalidator pods (group cache-invalidator)
                    │                                             │ DEL v1:item:{id}
                    ▼                                             ▼
                 reader ──── GET, then SET on a miss ─────────► Redis
                    └── on a miss ──► MySQL
```

## Start order

`kubectl apply` creates everything at once and waits for nothing. Each pod that needs
another service starts with an init container that waits until that service's port
answers (`nc -z`): the writer waits for MySQL, the reader for MySQL and Redis, the
invalidator for Kafka and Redis, and Kafka Connect for Kafka. The connector Job retries
instead: Kubernetes restarts its topic step until Kafka answers, and its curl retries
every 3s until Connect does. So a cold start has no crash loops.

## Run it

From the repo root (prerequisites and the full round trip are in the
[main README](../../README.md#kubernetes-kind)):

```bash
make kind-up       # cluster, ingress-nginx (pinned to controller-v1.15.1), metrics-server
make k8s-images    # build cdc-mysql, cdc-writer, cdc-reader, cdc-invalidator
make k8s-load      # copy the images into the kind nodes (no registry)
make k8s-up        # apply -k, wait for every rollout and the connector Job
```

## Operations

`make k8s-up` sets nothing in your kubeconfig; pass `-n cdc-cache-invalidation`, or once:
`kubectl config set-context --current --namespace=cdc-cache-invalidation`.

```bash
kubectl get pod,svc,hpa,ingress                       # everything at a glance
kubectl top pod                                       # CPU and memory per pod (metrics-server)

# Debezium connector state, and the change stream itself
kubectl exec kafka-connect-0 -- curl -s localhost:8083/connectors/catalog-items/status
kubectl exec kafka-0 -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic cdc.catalog.items --from-beginning --max-messages 5

# which invalidator owns which partition, and the lag on each
kubectl exec kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --describe --group cache-invalidator

# one invalidator's counters (each pod has its own)
kubectl exec <invalidator-pod> -- /busybox/wget -qO- localhost:8082/metrics | grep '^invalidator_'

# cache and database
kubectl exec redis-0 -- redis-cli --scan --pattern 'v1:item:*'
kubectl exec mysql-0 -- mysql -uapp -papp catalog -e 'SELECT COUNT(*) FROM items'

# logs from every pod of a service at once
kubectl logs -l app.kubernetes.io/name=invalidator --prefix --tail=20
```

To push a change to `catalog-items.json`, run `make k8s-up` again. The connector Job
deletes itself 10 minutes after it finishes, so the next apply creates it again and the
PUT updates the connector. Within those 10 minutes, `kubectl delete job register-connector`
first.

## Stop and reset

```bash
make k8s-down      # delete the namespace and everything in it, disks included
make kind-down     # delete the cluster
```

## Differences from a production setup

Everything runs on one machine: the three kind nodes are containers sharing its CPUs, so
more pods only help while the machine has CPU to spare. Each stateful service is a single
pod with replication factor 1. The disks are folders on one kind node (`local-path`), so
MySQL, Kafka and Redis can't move to another node. Secrets are plain dev values.
ingress-nginx is retired (no releases after March 2026); its successor is the Gateway API.

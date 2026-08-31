# kave

A Raft-backed distributed key-value store with MVCC, leases, watches, and Oblivious Transfer-based secret storage.
Built as a university thesis project, inspired by [etcd](https://github.com/etcd-io/etcd) with an HTTP API instead of gRPC.

## Overview

`kave` is a distributed systems project implementing a consistent, fault-tolerant key-value store with snapshot isolation. It uses HashiCorp's Raft library for consensus, BoltDB for durable storage, and exposes a RESTful HTTP API.

The project was designed and deployed on Kubernetes (Civo cloud) as part of a thesis assignment. The core distributed systems functionality is complete and tested;
the deployment infrastructure and observability stack were functional but have room for improvement, see [Known Issues](#known-issues).
Additionally the project was definetly a learning project, so revising the kuberenteset deployment infrastructure to be more generally applicable
to other environemtns, both local and cloud would be a nice touch.

### Key features

- [MVCC]: Multi-Version Concurrency Control with monotonic (main, sub) revisions, snapshot-isolated reads, and historical queries
- [Leases]: TTL-based leases with key attachment, distributed expiry through Raft, and leader-driven checkpoint scheduling
- [Watches]: Push-based watch API over WebSocket with synced/unsynced watcher pools and automatic catch-up
- [Oblivious Transfer]: 1-out-of-N OT protocol using Ristretto255 elliptic curves for privacy-preserving secret storage
- [Raft consensus]: Linearizable and serializable reads, leader election, log replication, snapshot/restore
- [mTLS]: Mutual TLS for inter-node Raft transport via cert-manager
- [Prometheus metrics]: Metrics across all subsystems (KV, Raft, Lease, OT, Watch, Backend)
- [Kubernetes deployment]: Helm chart with StatefulSet, PodDisruptionBudget, Let's Encrypt TLS, Traefik ingress

## Architecture

```
kave
├── cmd/
│   ├── kave/               Server entrypoint
│   └── client/             Simple CLI test client
├── internal/
│   ├── node/               Top-level orchestrator: wires all subsystems together
│   ├── command/            Raft command types (using gob encoding)
│   ├── fsm/                Raft FSM implementation + leadership event watcher
│   ├── mvcc/               MVCC engine: KvStore, Writer, Reader, Snapshot
│   ├── kv/                 Entry types, revision management, B-tree key index
│   ├── lease/              Lease manager, min-heap, checkpoint scheduler, expiry loop
│   ├── watch/              Watcher, WatchHub, Stream, Session (WebSocket), UnsyncedLoop
│   ├── ot/                 Oblivious Transfer manager + AES-GCM token codec
│   ├── compaction/         Periodic compaction scheduler
│   ├── service/            Business logic layer (KV, Lease, OT, Raft services)
│   ├── transport/http/     HTTP server, middleware (leader proxy, rate limiting, CORS)
│   ├── storage/            Backend interface + BoltDB and in-memory implementations
│   ├── config/             CLI flags + JSON config loading
│   ├── peer/               Peer discovery (static list + Kubernetes DNS)
│   ├── mtls/               mTLS StreamLayer for Raft transport
│   └── metrics/            Prometheus metrics per subsystem
├── test/
│   ├── integration/        In-process multi-node cluster tests
│   └── smoke/              End-to-end tests on a real Kind cluster
├── ui/                     SvelteKit web frontend
├── charts/kave/            Helm chart
└── docs/                   Design notes (Hungarian) + Mermaid diagrams used for the thesis paper
```

### Data flow

```
Client (HTTP/WS)
    |
    ˇ
HTTP Server --> Service Layer --> Raft Propose --> FSM Apply
    |                                                |
    |                                                ˇ
    |                                    MVCC Engine (Writer → Reader)
    |                                    Lease Manager
    |                                    OT Manager
    |                                                |
    |                                                ˇ
    |                                    BoltDB (Backend)
    |
    ---> Read (serializable: local, linearizable: leader + VerifyLeader())
```

## HTTP API

All endpoints are under `/v1/`. Requests and responses are JSON.

### KV

| Method   | Endpoint        | Description |
|----------|-----------------|---------------------------------------------------|
| `POST`   | `/v1/kv/range`  | Read keys (point or range, at revision or latest) |
| `POST`   | `/v1/kv/put`    | Write a key-value pair |
| `DELETE` | `/v1/kv/delete` | Delete one or more keys |
| `POST`   | `/v1/kv/txn`    | Execute a transaction (multiple ops atomically) |

### Leases

| Method   | Endpoint               | Description |
|----------|------------------------|--------------------------------------|
| `POST`   | `/v1/lease/grant`      | Create a lease with a TTL |
| `DELETE` | `/v1/lease/revoke`     | Delete a lease and its attached keys |
| `POST`   | `/v1/lease/keep-alive` | Reset a lease's TTL |
| `POST`   | `/v1/lease/lookup`     | Look up a lease by ID |

### Oblivious Transfer

| Method | Endpoint           | Description |
|--------|--------------------|--------------------------------------------------|
| `POST` | `/v1/ot/init`      | Start OT protocol (returns public point + token) |
| `POST` | `/v1/ot/transfer`  | Complete OT transfer (returns encrypted slots) |
| `POST` | `/v1/ot/write-all` | Update all secret slots (Raft-replicated) |

### Watch

| Method | Endpoint    | Description |
|--------|-------------|----------------------------------------------|
| `GET`  | `/v1/watch` | WebSocket upgrade for real-time watch events |

### Admin & Debug

| Method | Endpoint                 | Description |
|--------|--------------------------|-------------|
| `POST`   | `/v1/admin/join`       | Join a node to the cluster |
| `DELETE` | `/v1/admin/kill`       | Stop a node |
| `POST`   | `/v1/admin/compaction` | Trigger manual compaction |
| `GET`    | `/stats`               | Node statistics |
| `GET`    | `/metrics`             | Prometheus metrics |
| `GET`    | `/livez`               | Liveness probe |
| `GET`    | `/readyz`              | Readiness probe |
    
### Consistency models

- [Serializable] (default reads): Served locally on any node. Faster, but may return stale data.
- [Linearizable] (strong reads): Routed to leader with `VerifyLeader()` verification. Consistent, but higher latency.

## Getting started

Prerequisites:
- Go 1.25+
- Docker
- kubectl + Helm (for Kubernetes deployment)
- Kind (for local smoke tests)
- later, nix for stuff like golangci-lint

### Build and run locally

```sh
# build
make build-go

# run a 3-node cluster with Docker Compose
make up3

# or build the image first, then run
make up3build
```

### Run tests

```sh
make test-unit   # unit tests with race detector
make test-integ  # integration tests (in-process multi-node clusters)
make test-smoke  # full Kind cluster + Helm + end-to-end tests
make test-race   # race detector tests (3x count)
```

### Deploy to Kubernetes

```sh
# validate the Helm chart
make helm-validate

# deploy with Traefik ingress
make helm-upgrade-install

# deploy without Traefik
make helm-upgrade-install-no-traefik

# full wipe and redeploy
make wipe-recreate-cluster
```

Or directly with Helm:

```sh
helm upgrade --install kave ./charts/kave \
  --namespace kave \
  --create-namespace \
  --set image.tag=<tag> \
  --set config.adminAuthToken=<token> \
  --wait --timeout 3m
```

### Lint

```sh
# golangci-lint run
make lint
# golangci-lint fmt
make fmt
```

## Tech stack

- [Language]: Go 1.25
- [Consensus]: [HashiCorp Raft](https://github.com/hashicorp/raft) with `raft-boltdb` backend
- [Storage]: [BoltDB](https://github.com/etcd-io/bbolt) (persistent), in-memory B-tree (testing)
- [Crypto]: [cloudflare/circl](https://github.com/cloudflare/circl) (Ristretto255 for OT, AES-GCM for token encryption)
- [HTTP]: net/http with custom middleware chain (leader proxy, rate limiting, CORS, panic recovery)
- [WebSocket]: [coder/websocket](https://github.com/coder/websocket)
- [Metrics]: [prometheus/client_golang](https://github.com/prometheus/client_golang)
- [Indexing]: [google/btree](https://github.com/google/btree) for in-memory key index
- [Testing]: testify, in-process Raft clusters, Kind, golangci-lint
- [Deployment]: Kubernetes, Helm, cert-manager, Traefik, Let's Encrypt
- [UI]: interactive SvelteKit for presentation
- [CI/CD]: GitHub Actions (lint > test > build > deploy > UI)

## Known issues

There are some things i noted that should be revisited like dead code, old configs, etc.

- [TRACK.md]: this might contain old todos or chores.
- [Makefile cleanup]: Contains dev commands (k9s, golangci-lint, manual kubectl operations). The `bin/` directory with project-specific tools could be replaced by a Nix flake or similar.
- [`cmd/client/main.go`]:Uses old route paths (`/v1/kv/get` instead of `/v1/kv/range`)
- [Hungarian comments]: Doc comments are mixed between hungarian and english, and all design docs (`docs/notes/`) are in Hungarian. The existing TRACK.md also contains this as a chore.
- [Observability gap]: Prometheus metrics are instrumented across all subsystems, but no Grafana dashboards are actively used. The Helm chart includes Grafana templates and a kave.json dashboard, but this was not the focus of the thesis.
- [Missing LICENSE file]: Only `LICENSE-etcd` exists (Apache 2.0, for borrowed KV index code from etcd). The project's own license is not defined.
- [BoltSnapshotMetrics TODO]: Snapshot metrics are not yet implemented (noted in code and TRACK.md).
- [`BatchingFSM`]: Listed as a future optimization in the TODO, batching FSM applies to reduce per-command overhead.
- [`wire.go` bug]: `serverErrorPayload.MarshalJSON` has an inverted nil check on the `Cause` field (prints error message when it should be nil, and vice versa).
- [`LeaseManager.newManager`]: Contains `m.rwlock.Lock()` instead of `m.rwlock.Unlock()` in the active leases metric callback (deadlock risk on metric collection).
- [Civo-specific CI/deployment]: The GitHub Actions workflows and Helm chart reference Civo cloud infrastructure. There is no generalized deployment path for other cloud providers. Ideally the Helm chart should work with any `--kubeconfig` without Civo-specific assumptions.

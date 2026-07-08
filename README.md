# raft-kv — a linearizable, fault-tolerant key-value store

A complete Raft consensus implementation and replicated key-value store in
Go, built from scratch: segmented write-ahead log, dynamic membership,
exactly-once client sessions, linearizable reads, and a chaos test suite
that verifies linearizability under partitions, crashes, and message-level
faults.

## Feature highlights

**Consensus core**
- Leader election with randomized timeouts, **pre-vote** (thesis §9.6) and
  **check-quorum** auto-stepdown
- Log replication with conflict-term fast backtracking
- Snapshots + log compaction + `InstallSnapshot` for lagging followers
- **Leadership transfer** (`TimeoutNow`)
- **Dynamic membership**: `AddServer`/`RemoveServer` single-server changes
  with catch-up phase, joining-mode protection, and config recovery from
  WAL/snapshots (thesis §4)
- Event-driven core — no polling loops in the hot paths

**Storage**
- Segmented, append-only **write-ahead log** with per-record CRC32-C,
  O(delta) appends, group-fsync of entries + hard state
- Torn-write recovery at the tail; loud failure on real corruption
- Checkpointing snapshots with crash safety at every interruption point
- Transparent migration from the legacy JSON state format

**KV semantics**
- `Set`, `Delete`, `CompareAndSwap` (typed protobuf commands), `Get` (local,
  possibly stale) and `GetLinearizable` (ReadIndex quorum barrier, §8-correct)
- **Exactly-once application** via replicated client sessions: retries with
  the same `(client_id, sequence)` are deduplicated and return the original
  result

**Operations**
- Prometheus-format `/metrics`, JSON `/status`, `/healthz` — zero extra
  dependencies
- `ClusterStatus` admin RPC; CLI for kv ops, status, and membership changes
- Benchmark tool with throughput and p50/p90/p95/p99 latency
- Docker Compose 3-node cluster; GitHub Actions CI with race detector

**Verification**
- WAL unit tests including crash-window and corruption cases
- Integration suite over real gRPC clusters (restarts, partitions,
  transfers, membership)
- [Porcupine](https://github.com/anishathalye/porcupine) linearizability
  checking
- **Chaos suite**: seeded fault injection (partitions, drops on request and
  response paths, duplication, delays, crash/restart with WAL recovery
  under load) with linearizability + state-machine-convergence assertions.
  It caught three real consensus bugs during development — see
  [docs/CORRECTNESS.md](docs/CORRECTNESS.md).

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for component diagrams, the
WAL format, the ReadIndex implementation, and design rationale.
[docs/CORRECTNESS.md](docs/CORRECTNESS.md) covers the guarantee list, the
testing strategy, and known limitations.

## Quick start (Windows PowerShell)

```powershell
./scripts/setup-tools.ps1        # installs Go, protoc, optionally Docker
go mod tidy
./scripts/run-local.ps1          # builds, starts a node, runs a smoke test
```

Live 3-node demo with continuous workload and streaming logs:

```powershell
./scripts/demo-live.ps1 -DurationSeconds 180 -IntervalMs 250
./scripts/stop-demo.ps1          # cleanup
```

## Run with Docker

```powershell
docker compose up --build
```

Each node persists its WAL in a named volume and exposes metrics on ports
51051–51053:

```powershell
curl http://localhost:51051/status
curl http://localhost:51051/metrics
```

## Using the CLI

```powershell
go run ./cmd/client -op set -key name -value raft-kv
go run ./cmd/client -op lin-get -key name
go run ./cmd/client -op cas -key name -expect raft-kv -value v2
go run ./cmd/client -op del -key name
go run ./cmd/client -op status

# membership (target the leader; joining node runs with RAFT_JOIN=1)
go run ./cmd/client -op add-server    -peer 127.0.0.1:50054
go run ./cmd/client -op remove-server -peer 127.0.0.1:50054
```

## Tests

```powershell
go test ./raft/ ./tests/            # full suite (~30s)
go test ./tests/ -run TestChaos -v  # chaos suite only
CHAOS_SEED=7 go test ./tests/ -run TestChaos   # reproduce a fault pattern
```

On chaos failures, the raw history and an interactive porcupine
visualization are written to `logs/chaos-seed-*.{txt,html}`.

## Benchmark

```powershell
go run ./cmd/bench -addrs 127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053 `
    -concurrency 8 -duration 30s -read-ratio 0.5 [-linearizable]
```

Reference numbers from a 3-node cluster on one laptop-class Windows machine
(every write individually fsynced on leader and followers):

| Operation | Throughput | p50 | p99 |
|-----------|-----------:|----:|----:|
| `set` (durable, replicated) | ~670/s | 10.9 ms | 26 ms |
| `get` (local read) | ~676/s* | <1 ms | 1.5 ms |
| `lin-get` (ReadIndex barrier) | ~1,245/s | 3.3 ms | 10.7 ms |

\* mixed 50/50 workload; local reads are not the bottleneck.

## Configuration

| Variable | Purpose | Default |
|----------|---------|---------|
| `PORT` | gRPC listen port | `50051` |
| `NODE_ID` | unique numeric node id | `1` |
| `PEERS` | comma-separated peer addresses | — |
| `RAFT_ADVERTISE_ADDR` | own address as peers reach it (required for membership changes) | — |
| `RAFT_JOIN` | start outside the cluster, wait to be added via AddServer | `0` |
| `METRICS_PORT` | observability HTTP port (`0` disables) | `PORT+1000` |
| `RAFT_DATA_DIR` | WAL/snapshot root directory | `./data` |
| `RAFT_DISABLE_PERSIST=1` | run without persistence | — |
| `RAFT_HEARTBEAT_MS` | heartbeat interval | `80` |
| `RAFT_ELECTION_MIN_MS` / `RAFT_ELECTION_MAX_MS` | election timeout range | `250`/`500` |
| `RAFT_READ_QUORUM_TIMEOUT_MS` | linearizable read budget | `500` |
| `RAFT_SNAPSHOT_THRESHOLD` | applied entries between snapshots | `512` |
| `RAFT_MAX_APPEND_ENTRIES` | max entries per AppendEntries | `64` |
| `RAFT_ENABLE_PREVOTE` / `RAFT_ENABLE_CHECK_QUORUM` | election safety features | `1`/`1` |
| `RAFT_STRICT_APPLY_ACKS` | ack writes only after state-machine apply | `1` |

Invalid `PORT`, `NODE_ID`, or malformed peer addresses fail fast at startup.

## Regenerate protobuf bindings

```powershell
./scripts/gen-proto.ps1
```

## Honest limitations

No TLS/authn, address-based member identity, single-message snapshot
transfer, and seeded (not fully deterministic) chaos testing. Details and
reasoning in [docs/CORRECTNESS.md](docs/CORRECTNESS.md#known-limitations).

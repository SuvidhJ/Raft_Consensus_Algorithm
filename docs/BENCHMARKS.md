# Benchmarks

How every number in the README was measured, and how to reproduce it.
Nothing is hand-picked: the failover and chaos numbers are complete runs,
and each performance figure pools every run of that configuration.

## Environment

| | |
|---|---|
| CPU | Intel Core i7-13650HX (14 cores / 20 threads) |
| OS | Windows 11, Go 1.27 |
| Disk | Samsung MZAL8512 NVMe SSD (512 GB), NTFS; all nodes share it |
| Cluster | 3 nodes = 3 separate OS processes on one machine, gRPC over loopback |
| Config | defaults: heartbeat 80 ms, election timeout 250–500 ms, pre-vote + check-quorum on, snapshot every 512 entries |
| Durability | every log append is fsynced before it is acknowledged (leader and followers) |

Loopback removes network latency, so these numbers are a lower bound on
latency for a real multi-host deployment; they are dominated by fsync.

## 1. Leader-crash failover (`cmd/failover`)

**Method.** The harness launches three node processes and runs four
sessioned writers, each with one write in flight, retried with the same
`(client_id, sequence)` until acknowledged. Every trial:

1. waits 1.5 s of steady load, confirms all nodes agree on one leader;
2. **hard-kills the leader process** (`TerminateProcess`, the Windows
   equivalent of `SIGKILL`: no graceful shutdown, no flush);
3. measures **election time** (kill → a survivor reports itself leader of a
   newer term, polled every ~1 ms) and **write unavailability** (kill → first
   write acknowledged by a survivor);
4. pauses the writers with no write in flight and checks **every key**
   against its last acknowledged value: local reads on each survivor after
   it has applied the leader's commit index, plus linearizable reads through
   the leader;
5. restarts the killed node from its WAL and measures **boot** (→ answers
   RPCs) and **catch-up** (→ applied index reaches the leader's commit
   index), then checks every key on it too;
6. records **leader churn** (extra elections after the first post-crash
   leader) and whether the rejoining node disturbed the leader.

Keys are 4 writers × 512 slots overwritten round-robin. At most 56 writes
per writer happened between a crash and its verification, far fewer than
512, so every write acknowledged in the seconds before each crash is still the
current value of its key when it is checked: a lost write cannot hide
behind a later overwrite. Percentiles are nearest-rank. Clock resolution
was measured at ~0.3 ms.

```powershell
go build -o raftkv.exe .
go run ./cmd/failover -bin ./raftkv.exe -trials 200 -csv failover.csv
```

**Results: 200 leader crashes.**

| Metric | p50 | p90 | p99 | max |
|---|---:|---:|---:|---:|
| Election (crash → new leader) | 346 ms | 429 ms | 508 ms | 511 ms |
| Write unavailability (crash → first ack) | 353 ms | 436 ms | 514 ms | 519 ms |
| Restarted node: boot → serving RPCs | 57 ms | 88 ms | 102 ms | 186 ms |
| Restarted node: boot → fully caught up | 456 ms | 554 ms | 608 ms | 809 ms |

- **0 lost or phantom writes**: 1,638,400 key checks across all replicas,
  after 177,568 acknowledged writes.
- **0 leader churn**: no extra elections after the first post-crash
  leader in any of the 200 trials.
- **0 disruptions on rejoin**: the restarted node never forced an
  election (pre-vote).

Failover time is almost entirely the election timeout itself. The first
of the two survivors' randomized timeouts (250–500 ms) expires after a
median of ~323 ms, plus ~5 ms of election-ticker granularity; the measured
median election of 346 ms leaves ~20 ms for pre-vote, vote, the vote fsync
and detection. A shorter timeout range would shorten failover
proportionally, at the cost of more spurious elections.

**Before/after the election fixes** (bugs 3–4 in
[CORRECTNESS.md](CORRECTNESS.md#bugs-the-failover-benchmark-caught)). The
baseline is the original election code with only the two safety fixes
applied, because the unmodified code stalled within 10 crashes in both
attempts (bugs 1–2) and cannot finish a run. Same harness and machine;
before = 50 crashes, after = the 200-crash run above:

| Write unavailability | Before | After | Change |
|---|---:|---:|---:|
| p50 | 417 ms | 353 ms | −15% |
| p90 | 1,088 ms | 436 ms | −60% |
| p99 | 1,140 ms | 514 ms | −55% |
| Failovers over 700 ms | 14% | 0% | |
| Trials with leader churn | 38% (38 extra elections) | 0% | |

## 2. Chaos suite (`tests/chaos_test.go`)

**Method.** Each run is a 5-node cluster on an in-memory transport with
real WALs, 3 sessioned clients (writes retried until committed, plus
linearizable reads from random nodes), and a seeded fault injector that
changes the environment every 100–250 ms: partition a node, heal, crash a
node, restart it (WAL recovery under load), add packet loss/duplication/
jitter, or quiet the network. After 6 s the network heals and the run
asserts one leader, identical commit/applied indexes, **byte-identical state
machines** (data and session tables) on all five nodes, and a
**linearizable** history (porcupine). Each seed runs in its own process,
four at a time.

```bash
go test -c -o chaos.test.exe ./tests/
for s in $(seq 1 500); do CHAOS_SEED=$s ./chaos.test.exe -test.run TestChaos -test.v; done
```

**Results: seeds 1–500, 500 passed, 0 failed.** Totals from the per-run
`chaos stats` lines:

| | Total | Per run |
|---|---:|---:|
| Client operations checked for linearizability | 118,948 | 238 |
| Node crashes / restarts with WAL recovery | 1,426 / 1,171 | 2.9 / 2.3 |
| Network partitions | 2,830 | 5.7 |
| Packet-loss / duplication / jitter phases | 2,706 | 5.4 |
| Messages dropped (loss, partitions, crashed peers) | 194,925 | 390 |
| Replies dropped after processing (forces duplicate delivery) | 11,285 | 23 |
| Messages duplicated | 8,066 | 16 |
| Leader elections | 1,315 | 2.6 |

Every run ended with byte-identical state machines on all five nodes and a
history porcupine verified as linearizable.

## 3. Throughput and latency (`cmd/bench`)

**Method.** A fresh 3-process cluster per run, a 3 s warm-up, then a timed
closed-loop run: N workers each issue one operation at a time. Writes are
sessioned `Set`s with 64-byte values over 64 keys; linearizable reads use
the ReadIndex barrier on the leader; local reads are served by any node.
Throughput counts only successful operations; latency is per operation.
The raw fsync ceiling of the same disk (append 160 B + `fsync`, single
thread) is measured separately for context.

```powershell
go run ./cmd/bench -addrs 127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053 `
    -concurrency 16 -duration 20s -read-ratio 0
```

**Variance.** This is a laptop, and its performance drifts: within one
session the disk's raw fsync rate measured anywhere from 599/s to 981/s,
and three back-to-back local-read runs gave 107k, 154k and 198k ops/s. So
every headline figure below is the **median of all runs** of that
configuration, with the full range shown. Treat single-run numbers here as
±25%. The sweep and the first three runs of each six-run row used an
earlier `cmd/bench` whose workers all advanced a shared leader hint on any
failure (now a compare-and-swap). That only wastes time retrying against
followers, so it can only have lowered those runs' write and
linearizable-read throughput; local reads do not use the hint.

**Results.**

| Workload | Clients | Runs | Throughput: median (range) | p50 | p99 |
|---|---:|---:|---|---:|---:|
| Durable write | 1 | 2 × 10 s | 314/s (314–314) | 3.2 ms | 7.7 ms |
| Durable write | 16 | 6 × 20 s | 470/s (406–541) | 32 ms | 57 ms |
| Linearizable read | 128 | 6 × 20 s | 60.8k/s (52.7k–74.4k) | 1.8 ms | 5.6 ms |
| Linearizable read | 256 | 6 × 20 s | 81.4k/s (62.0k–97.1k) | 2.7 ms | 9.5 ms |
| Local (possibly stale) read | 64 | 6 × 20 s | 101k/s (94k–198k) | 0.5 ms | 2.4 ms |

Sub-millisecond percentiles are at the Windows clock's ~0.3–0.5 ms
resolution; read them as "< 1 ms".

**Write concurrency sweep** (one 10 s run each), showing the
throughput/latency trade-off:

| Clients | Throughput | p50 | p99 |
|---:|---:|---:|---:|
| 1 | 314/s | 3.2 ms | 7.9 ms |
| 4 | 583/s | 6.6 ms | 12.5 ms |
| 16 | 657/s | 23.8 ms | 33.0 ms |
| 64 | 594/s | 106.5 ms | 163.0 ms |

**What limits it.**

- **Writes are fsync-bound.** The leader appends and fsyncs each proposal
  on its own while holding the node mutex, so leader-side writes are
  serialized; only followers batch. Past ~16 clients, extra concurrency
  adds latency, not throughput. Leader-side group commit (one fsync per
  batch of queued proposals) is the obvious next optimization.
- **Linearizable reads are cheap because they batch.** Each read waits for
  its own quorum round (peer contact stamped after the read began), but
  one heartbeat round confirms every read waiting on it, so throughput
  scales with concurrency, and reads do no disk I/O.
- **Reads queue behind writes in mixed workloads.** At 50/50 with 16
  clients the cluster does 978 ops/s (lin-read p50 11 ms), but at 64
  clients it drops to 570 ops/s with lin-read p50 96 ms: the read barrier
  needs the same mutex the leader holds during each fsync. Group commit
  would help here too.

# Correctness

What this system guarantees, why, and how the guarantees are tested.

## Guarantees

1. **Election safety** — at most one leader per term. Votes are persisted
   (fsynced) before they are granted; a node votes at most once per term.
2. **Log matching** — if two logs contain an entry with the same index and
   term, the logs are identical up to that entry. Enforced by the
   AppendEntries prev-index/prev-term check with conflict-term backtracking.
3. **Leader completeness** — a candidate cannot win without a log at least
   as up-to-date as a majority's, so committed entries survive elections.
4. **State machine safety** — entries are applied in log order, exactly once
   per index, only after they are committed, and only if the entry's term
   still matches the log (`ShouldApplyEntry` guards against applying
   superseded entries queued before a truncation).
5. **Linearizable writes** — a positive `Set/Delete/CAS` ack means the
   command was committed by a quorum *in the proposing term* and applied to
   the state machine before the ack was sent.
6. **Linearizable reads** — `GetLinearizable` performs a ReadIndex barrier:
   current-term commit requirement (Raft §8), quorum confirmation with
   RPC-start time stamping, and apply-wait up to the read index.
7. **Exactly-once application** — retried commands with a client session are
   applied at most once; duplicates return the original result. The dedup
   table is replicated and snapshot-included, with deterministic eviction.
8. **Durability** — every acknowledgement is backed by fsynced WAL records
   on a quorum. Torn writes at the WAL tail are recovered; corruption
   elsewhere fails loudly rather than serving wrong data.
9. **Membership safety** — single-server changes only, one in flight at a
   time, so any two active quorums always intersect.

## Testing strategy

Three layers, in increasing order of adversarial power:

### 1. Unit tests (`raft/wal_test.go`)

The storage engine in isolation: round-trips, implicit suffix truncation,
torn-tail discard, mid-segment corruption detection, checkpoint crash
windows (snapshot file ahead of WAL meta), legacy JSON migration, segment
rotation.

### 2. Integration tests (`tests/`)

Real gRPC clusters on loopback: replication, restarts and WAL replay,
snapshot install, leadership transfer, quorum-loss stepdown, linearizable
read rejection on followers and during partitions, exactly-once retries,
CAS/Delete semantics, dynamic membership (add, remove, restart-with-config,
joining-node isolation), plus a porcupine linearizability check under a
quorum-loss partition.

### 3. Chaos suite (`tests/chaos_test.go`)

A 5-node cluster on an in-memory transport hub with seeded fault injection:

- symmetric partitions
- message drops on the request path *and* the response path (the latter
  forces duplicate deliveries on retry)
- random delays and duplicated deliveries
- process crashes and restarts with real WAL recovery, under load

Concurrent sessioned clients run writes (retried until definitely committed)
and linearizable reads against random nodes. After the run, the network
heals and the suite asserts:

1. the cluster converges to a single leader with all nodes applied to the
   same commit index,
2. all five replicated state machines are **byte-identical** (data and
   session tables), and
3. the full operation history is **linearizable** (checked by
   [porcupine](https://github.com/anishathalye/porcupine)). Indeterminate
   writes (in flight when the run ended) are modeled as pending operations
   that may linearize anywhere after their invocation.

On failure the suite dumps the raw history and an interactive porcupine
visualization to `logs/chaos-seed-*.{txt,html}`.

## Bugs the chaos suite caught during development

Kept here because they are the strongest argument for this style of testing —
all three survived the full unit + integration suites:

1. **Stale reads from a new leader (Raft §8).** The read barrier required
   `applied ≥ commitIndex` and quorum contact, but not that the leader had
   committed an entry *in its own term*. A freshly elected leader's
   commitIndex can lag entries its predecessor committed and acknowledged,
   so a barrier read could miss them. Fix: the barrier's read index is
   `max(commitIndex, termStartIndex)` where `termStartIndex` is the
   leader's no-op.

2. **Pre-vote livelock.** Pre-votes were rejected while `leader != 0 &&
   now < electionDeadline` — but each node resets its own election deadline
   after a failed pre-vote. After certain partition sequences, all nodes
   ended up followers taking turns failing pre-vote against each other's
   freshly-reset deadlines, leaving the cluster leaderless indefinitely.
   Fix: rejection is based on the time of the last *genuine leader contact*
   (AppendEntries/InstallSnapshot accepted from the current leader).

3. **Stale leadership evidence in the read barrier.** Peer contact was
   stamped at RPC *response receipt*. A slow response to a heartbeat sent
   *before* the barrier began could satisfy the quorum check, even though
   the follower's acknowledgement predated the read. Fix: contact is
   stamped with the RPC *start* time — evidence only counts if the round
   trip began after the barrier did.

A fourth bug was found by inspection while building the WAL: the proposer
acked any commit reaching its index, even if leadership had changed and a
*different* entry from a newer term committed there. The commit wait now
verifies the term at the index.

## Hardening from the line-by-line audit

A subsequent full-code audit found and fixed these additional defects:

- `NotifyApplied` could advance `lastApplied` past `commitIndex` if handed a
  bogus index (e.g. the deferred notification for an entry rejected by
  `ShouldApplyEntry`), which would falsely satisfy read barriers. It now
  ignores notifications beyond the commit index (regression-tested).
- The election triggered by `TimeoutNow` ran on an untracked goroutine that
  ignored node shutdown and joining mode; it now re-checks both.
- Replication kept streaming to peers that had been removed from the
  configuration until the in-flight call drained naturally; the loop now
  re-checks membership (with an explicit exception for `AddServer`
  catch-up, which intentionally targets a non-member).
- CAS/Delete could fabricate an answer (`swapped=false`/`existed=false`)
  if the per-index apply result had been evicted from the result window;
  they now return an explicit failure instead of lying.
- Incoming `AppendEntries` batches are capped (senders are capped at 1024
  by guardrails; receivers reject >4096) to bound allocations.
- Stale `snapshot.snap.tmp` files from interrupted snapshot writes are
  removed at WAL open; the `replicating` bookkeeping no longer leaks
  entries for removed members.

## Known limitations

Stated plainly, because honesty is part of correctness:

- **Windows clock granularity.** Go's monotonic clock on Windows ticks
  coarsely (~0.5–15.6 ms). All correctness-critical comparisons use
  conservative directions, but sub-tick timing in tests can be flaky on
  Windows; CI runs on Linux.
- **No TLS or authentication** on gRPC or the metrics endpoint.
- **Membership identity is the advertised address**; changing a node's
  address requires remove + re-add.
- **Pre-vote livelock fix trades a small disruption window**: a node that
  recently heard a leader still rejects pre-votes for `electionTimeoutMin`,
  which bounds (not eliminates) disruption by removed or partitioned nodes.
- **The chaos suite is seeded fault injection, not deterministic
  simulation** — goroutine scheduling still varies between runs. Seeds
  reproduce fault *patterns*, not exact interleavings.
- **Snapshot transfer is single-message**, fine for moderate state sizes;
  chunked InstallSnapshot would be needed for very large state machines.

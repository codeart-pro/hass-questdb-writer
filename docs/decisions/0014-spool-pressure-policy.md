# ADR 0014: Never evict pending rows; pause persistence on storage pressure

Status: accepted.

Date: 2026-09-19

## Context

The durable spool is a journal of accepted events, not a cache of the latest
state. `DEDUP UPSERT KEYS(last_updated, entity_id)` (ADR 0005) makes a replayed
event idempotent, but an event that was never written cannot be replayed at all.

Measured production profile ([../benchmarks/real-world-qss-data.md](../benchmarks/real-world-qss-data.md)):
median ~13 events/s, p99 ~27, peak ~61, about 253 B per row in the QuestDB
format. The provisional limits (100,000 pending rows / 64 MiB) therefore cover
about 2.1 hours at the median rate, ~1 hour at p99 and ~27 minutes at the peak,
while a 24-hour outage produces 1.1M (median) to 5.3M (peak) events: the hard
limit is reached long before the outage ends, so what happens at the limit is a
production behaviour, not an edge case.

The payload counters also cannot see the storage the writer actually consumes.
They count serialized bytes; the database pages, the freelist, the WAL and
everything else on the same filesystem (recorder, logs, backups) are outside
them, and a filesystem that runs out of space turns into `SQLITE_FULL`, a
generic `SpoolError` and a dead worker. ADR 0002 recorded both a free-space
guard and a tested overflow policy as required safeguards this decision now
implements.

## Decision

1. **Pending rows are never evicted.** The spool stays FIFO with a hard limit.
   The oldest accepted events are the ones worth keeping: they are the start of
   the outage window, and a replay missing its beginning is not a replay - a
   chart cannot distinguish "the sensor did not change" from "the events were
   dropped", and active and quiet entities lose different amounts of history.
2. **Reaching the hard limit pauses persistence, not the worker.** The state
   becomes `BLOCKED` with `block_reason="spool_full"`, accepted events stay in
   the ingress queue, and the writer keeps retrying. Once the queue is full,
   new events are dropped at `submit()` and counted in `overflowed_events`, so
   loss is bounded, explicit and attributed.
3. **Free space is checked before every durable write.** `FilesystemGuard`
   reads the filesystem that holds the spool (one `statvfs`; the durable
   transaction it precedes costs milliseconds, so a timer would only add lag).
   The reserve is `max(spool_min_free_bytes, spool_min_free_ratio * filesystem
   size)`: the floor protects small disks, the share protects large ones, and
   neither requires knowing the disk size in advance.
4. **Consuming the reserve pauses persistence** with `block_reason="disk_space"`,
   and the guard fails closed: an unreadable filesystem counts as blocked,
   because assuming there is room is exactly what turns a full disk into a lost
   queue.
5. **Storage failures that a retry can clear are classified** in the spool:
   `SpoolDiskFullError` (`SQLITE_FULL`) and `SpoolReadOnlyError` (the
   `SQLITE_READONLY` family, including the extended codes SQLite reports when
   the filesystem or database file changed underneath an open connection).
   They block the worker with `block_reason="disk_full"` or `"readonly"` instead
   of ending it. Every other `sqlite3.Error` remains a plain `SpoolError` and
   stays fatal: a broken invariant must not loop forever.
6. **The worker recovers by itself.** The first durable write that succeeds
   after a storage block clears the reason, returns the state to `RUNNING` and
   counts a recovery. There is no manual step and no reload. The space may have
   to be reclaimed from the spool first, which
   [ADR 0015](0015-spool-space-reclamation.md) implements: pages freed by
   delivered rows stay inside the SQLite file and would otherwise keep the
   filesystem below its reserve for good.
7. **Loss is measurable.** `overflowed_events` counts what the ingress queue
   refused; diagnostics expose `storage_blocks`, `storage_recoveries`,
   `disk_free_bytes` and `disk_reserve_bytes` next to the block reason, so an
   operator can tell a QuestDB outage (pending backlog grows) from a storage
   problem (blocks grow, free space is at the reserve).

Scope note: delivery also writes (it deletes delivered rows and stores attempt
metadata), so a storage failure there is classified the same way and pauses the
delivery attempt instead of killing its task. Delivery is deliberately not
gated on the guard: draining the spool frees space, which is the direction we
want while the disk is tight.

## Consequences

Positive:

- a full disk, a read-only filesystem or a full spool no longer kill the writer;
- the outage window is never rewritten to keep fresher rows;
- the writer's own consumption is bounded independently of the disk size;
- every lost event has a counter and a state transition that explains it.

Negative:

- at the hard limit the newest events are dropped while the oldest are kept, so
  dashboards can be stale until the outage ends;
- the reserve is a mitigation, not an exact model of the space SQLite needs:
  `synchronous=FULL` plus WAL still means the real requirement exceeds the
  payload counters;
- two further options appear in the advanced step of the options flow.

## Verification completed

In the native ARM64 Home Assistant 2026.7.2 container (Python 3.14.6, SQLite
3.53.2):

- `PRAGMA max_page_count` on the spool's own connection plus a payload larger
  than the WAL reproduces `SQLITE_FULL` (error code 13) without filling a disk,
  and `enqueue_many` reports `SpoolDiskFullError`;
- `PRAGMA query_only = ON` reproduces the read-only path and reports
  `SpoolReadOnlyError`;
- an unclassified `sqlite3.Error` still surfaces as a plain `SpoolError`;
- the worker stays `RUNNING`-capable while `BLOCKED`: the disk-full, read-only
  and reserve tests each assert that the thread stays alive, the block reason is
  the expected one and the accepted event is persisted after the condition
  clears, with `storage_recoveries` incremented;
- a blocked guard never reaches the write (`enqueue_many` call count stays 0
  while the reserve is consumed);
- 283 unit tests pass, and three deliberate mutations of the new code (the
  storage-error branch, the guard call, the recovery transition) each turn the
  new tests red, so the tests are not vacuous.

## Required verification before production

- repeat on the exact production filesystem: an outage longer than the spool
  window with the reserve actually reached, not merely crossed;
- disk-full and read-only transitions on that filesystem, including automatic
  recovery once space returns;
- measure `.db + -wal` growth against `pending_bytes` over a long outage and
  select `spool_min_free_bytes` / `spool_min_free_ratio` from it instead of the
  provisional defaults (ADR 0002 keeps the same open item).

## References

- ADR 0002 (`0002-durable-sqlite-spool.md`) - the spool this policy protects;
- ADR 0006 (`0006-dead-letter-retention.md`) - separate bounded store for
  rejected rows;
- [../benchmarks/real-world-qss-data.md](../benchmarks/real-world-qss-data.md) -
  event rate and payload sizes behind every number here;
- SQLite error codes: <https://sqlite.org/rescode.html>;
- `shutil.disk_usage`: <https://docs.python.org/3.14/library/shutil.html#shutil.disk_usage>.

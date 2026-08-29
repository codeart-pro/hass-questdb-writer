# ADR 0002: Use a bounded local SQLite spool

Status: accepted for implementation; production limits remain unselected.

Date: 2026-08-29

## Context

Home Assistant must continue accepting relevant state changes while QuestDB,
DNS, the network, or the NAS is temporarily unavailable. An in-memory queue
does not survive a Home Assistant restart or process crash. A custom append-only
journal would make this project responsible for atomic commits, crash-tail
recovery, indexing, acknowledgement deletion, compaction, and migrations.
External brokers such as Redis, NATS, or Kafka add another service whose
availability would become part of the ingestion path.

## Decision

Use one private SQLite database per config entry as a bounded local spool:

- the writer thread creates and exclusively owns one SQLite connection;
- WAL journal mode is required on a local filesystem;
- `synchronous=FULL` protects committed enqueue transactions across operating
  system crashes and power loss, subject to the guarantees of the filesystem
  and storage hardware;
- all state transitions use explicit transactions;
- ingress events can be persisted in batches with one commit;
- pending and dead-letter rows and payload bytes have separate hard limits;
- transactionally maintained counters are reconciled from source rows on open;
- an event ID already retained in pending or dead-letter state is idempotent
  only when its payload and timestamp are identical;
- uncertain remote delivery is sticky because a later connection error cannot
  remove the possibility of an earlier committed QuestDB write.

Payload limits count only serialized payload bytes. They do not represent the
full database file size. Filesystem free-space monitoring and a tested overflow
policy are separate required safeguards.

## Why not Home Assistant Recorder

Recorder is an application database owned by Home Assistant. Coupling exporter
delivery state to Recorder's schema, purge policy, and configured backend would
violate ownership boundaries and would not provide a private acknowledgement
queue.

## Consequences

Positive:

- no runtime service or third-party Python dependency is added;
- committed events survive process restart;
- FIFO replay, retry metadata, and dead-letter transitions are queryable and
  transactional;
- schema versioning and invariant checks make incompatible state explicit.

Negative:

- WAL is unsuitable for a network filesystem;
- `synchronous=FULL` adds an fsync cost to each transaction;
- database, WAL, index, and metadata overhead exceed payload-byte counters;
- SQLite corruption, disk-full, read-only filesystem, and migration behavior
  require explicit failure tests;
- a single writer is intentional and limits horizontal write concurrency.

## Verification completed

In the native ARM64 Home Assistant 2026.7.2 container using Python 3.14.6 and
SQLite 3.53.2:

- committed rows survived abrupt process exit without `close()`;
- an uncommitted insert disappeared after abrupt process exit;
- both reopen paths returned `PRAGMA integrity_check = ok`;
- retry metadata and delivery uncertainty survived close/reopen;
- invalid multi-row transitions rolled back atomically;
- counter reconciliation repaired deliberately incorrect cached counters;
- 31 unit tests passed with the spool included.

Performance evidence and limitations are recorded in
[../benchmarks/sqlite-spool.md](../benchmarks/sqlite-spool.md).

## Required verification before production

- repeat the benchmark on the exact production storage path;
- stop power or kill Home Assistant during sustained enqueue and replay;
- test disk-full and read-only transitions without blocking the HA event loop;
- test WAL checkpoint and database growth during a long QuestDB outage;
- select explicit queue, payload, dead-letter, and free-space limits from the
  measured event rate and desired outage window;
- verify backup and restore handling for the database plus active WAL state.

## References

- Python `sqlite3` transaction control:
  <https://docs.python.org/3.14/library/sqlite3.html#transaction-control>
- SQLite WAL:
  <https://sqlite.org/wal.html>
- SQLite synchronous modes:
  <https://sqlite.org/pragma.html#pragma_synchronous>

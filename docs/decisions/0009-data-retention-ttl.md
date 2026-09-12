# ADR 0009: Data retention (TTL) as an advanced option

Status: accepted.

Date: 2026-08-29

## Context

The events table grows without bound (~5.3 GB/month at the measured row
size). Users want to cap the stored history. QuestDB Open Source provides
TTL (time-to-live): the server drops whole partitions whose time range
fell entirely outside the retention window, without any cron jobs or
manual cleanup.

The integration owns the table (ADR-0005), so the retention belongs in
its configuration: an advanced option, `retention_days` (0 = unlimited).

## Decision

- Add `retention_days` to the advanced options step (dropdown with common
  values plus free-form entry, coerced to `int` in `[0, 3650]`).
- `IlpSchemaManager` receives `retention_days` and aligns the table TTL on
  every `ensure` (startup and reload):
  - unset/0  -> `ALTER TABLE <t> SET TTL 0 DAYS` when a TTL is present;
  - set to N -> `ALTER TABLE <t> SET TTL N DAYS` when the current TTL
    differs;
  - otherwise no-op (no redundant DDL).
- The current TTL is read from `SELECT table_name, ttlValue, ttlUnit FROM
  tables()`, never from `SHOW CREATE TABLE`.

## Verified findings (QuestDB 10.0.1)

- `ALTER TABLE ... SET TTL n DAYS` works on WAL tables with
  `DEDUP UPSERT KEYS` — the earlier suspicion of incompatibility was
  wrong.
- `SHOW CREATE TABLE` does **not** render the TTL clause for WAL DEDUP
  tables even though the TTL is active — a display trap. `tables()` is
  the authoritative source.
- `CREATE TABLE ... DEDUP UPSERT KEYS (...) TTL ...` is a syntax error;
  TTL must be applied with `ALTER TABLE ... SET TTL`.
- TTL changes on WAL tables apply asynchronously (WAL apply queue):
  `tables()` may report the old value for up to a couple of seconds.
- `DELETE FROM <t> WHERE ...` is not available over the REST `/exec`
  endpoint in 10.0.1 (`unexpected token [FROM]`); TTL is the retention
  mechanism.

## Consequences

- A retention cap is now set entirely through the UI; changing it
  re-applies on reload with no data migration.
- TTL drops whole day partitions (our `PARTITION BY DAY`), so the
  retention granularity is one day — acceptable for sensor history.
- Because TTL applies asynchronously, a rapid second `ensure` may issue
  a redundant (idempotent) `SET TTL`; harmless.

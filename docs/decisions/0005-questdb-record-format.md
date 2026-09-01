# ADR 0005: QuestDB record format with a deduplicated `last_updated` timestamp

Status: accepted for implementation.

Date: 2026-08-29

## Context

The provisional data model in `docs/architecture.md` and ADR 0004 sent the Home
Assistant event time (`event.time_fired`) as the QuestDB designated timestamp
and relied on the `event_id` column for duplicate detection. Three questions
remained open before the format could be frozen:

1. `entity_id` as SYMBOL versus VARCHAR;
2. designated timestamp semantics;
3. the `STATE_UNKNOWN` policy.

A review of QSS
([CM000n/qss](https://github.com/CM000n/qss)) and of 137M rows of real QSS data
from a production installation
([real-world-qss-data.md](../benchmarks/real-world-qss-data.md)) provided the
evidence: 2,720 distinct entity IDs over 6.5 months, sensor attributes around
140 B (p99 191 B), a peak of about 61 events/s, zero duplicate
`(timestamp, entity_id)` pairs over the full range, and `unavailable` as a
small but real signal (0.75% of rows).

QuestDB's own delivery-semantics documentation describes the exactly-once
pattern as at-least-once delivery plus server-side deduplication on a key that
covers row identity, where the designated timestamp is always part of the key
and key columns must be derived deterministically from the source event, not
from wall-clock time at the moment of sending.

## Decision

The target QuestDB table schema:

```sql
CREATE TABLE hass (
    last_updated TIMESTAMP,
    entity_id SYMBOL,
    domain SYMBOL,
    state VARCHAR,
    attributes VARCHAR,
    event_id VARCHAR,
    context_id VARCHAR,
    ingested_at TIMESTAMP,
    last_changed TIMESTAMP
) TIMESTAMP(last_updated) PARTITION BY DAY WAL
DEDUP UPSERT KEYS(last_updated, entity_id);
```

- **Designated timestamp is the HA state object's `last_updated`**, not
  `event.time_fired`. A row then means "from this moment the state object
  (state + attributes) is as recorded", which matches Grafana time-series
  semantics (`SAMPLE BY ... LAST()`, `LATEST ON`). In steady state
  `time_fired` and `last_updated` differ by microseconds; they diverge only for
  restored states at Home Assistant startup, where `last_updated` is the
  semantically correct value. `event.time_fired` is not stored; `ingested_at`
  covers the integration acceptance time.
- **`DEDUP UPSERT KEYS(last_updated, entity_id)`** makes retried delivery
  idempotent at the database level. The spool stores the exact serialized
  payload, so a retry row is byte-identical to the original and QuestDB's
  full-row check treats it as a no-op. HA semantics guarantee at most one state
  object per entity per `last_updated`, so no legitimate rows share the key.
  This removes the uncertain-retry duplicate window without a downstream
  deduplication process.
- **`entity_id` stays SYMBOL.** Real-world cardinality is a few thousand
  distinct values (2,720 measured, growing slowly), well inside the SYMBOL
  comfort zone: 4 B/row versus roughly 30 B/row for VARCHAR, and fast
  `DISTINCT`/`WHERE`/`LATEST ON` queries for Grafana.
- **`domain` stays SYMBOL** (about 20 distinct values; `sensor` alone carries
  96.6% of real rows).
- **`state` is VARCHAR without numeric coercion.** Real states are arbitrary
  strings (`0.99`, `""`, `unavailable`); numeric interpretation is left to SQL
  casts in queries.
- **`attributes` is VARCHAR** holding the full Home Assistant JSON
  serialization (attribute allow-list remains a future option).
- **`event_id` is retained as VARCHAR** for analysis and duplicate inspection;
  deduplication responsibility moves to QuestDB.
- **`context_id` is retained as a nullable VARCHAR** and omitted from ILP when
  absent.
- **`STATE_UNKNOWN` is skipped at the listener; `unavailable` is written.**
  `unknown` means "no value yet" and only adds noise to Grafana value panels;
  `unavailable` is a real signal (0.75% of real rows). The policy later moves
  into the include/exclude filter options.

## Consequences

Positive:

- retries and restored-state replays cannot create duplicates;
- the time axis is semantically correct for state-value dashboards;
- symbol storage and query performance match the measured cardinality;
- real payload sizes (p99 191 B) leave the provisional 64 KiB event cap with
  ~340x margin, and the 1,000-row batch bound binds before the byte bound.

Negative:

- the integration must own the `CREATE TABLE` DDL (with `DEDUP UPSERT`) and
  validate the table schema at startup;
- dedup maintains an index per partition; the measured low timestamp
  collision and low key cardinality keep the cost small, but ingestion
  overhead must be confirmed on the bench;
- out-of-order replay (spool drain after an outage) interacts with dedup and
  O3 commit and needs verification;
- existing unit and integration tests that assert the old `time_fired`
  designated-timestamp mapping are updated together with the implementation;
- at ~253 B/row the format adds about 61% to archived data volume versus the
  minimal QSS format (see the real-world analysis).

## Verification completed

- Real-world QSS data analysis over 137M rows: cardinality, rates, payload
  distribution, zero duplicates, outage gaps. See
  [real-world-qss-data.md](../benchmarks/real-world-qss-data.md).
- Dedup behavior on the local QuestDB 10.0.1 bench: the DDL is accepted; two
  identical ILP rows with the same `(last_updated, entity_id)` key collapse to
  one row; a later row with the same key replaces the value (upsert); the row
  count stays one.
- Schema ownership on the same bench: `CREATE TABLE IF NOT EXISTS` is a silent
  no-op on an existing table, so validation always follows creation;
  `SHOW COLUMNS` truthfully reports the `designated` flag and the dedup
  `upsertKey` flags for DDL-created tables, and the integration validates
  against exactly those; an ILP field whose name collides with the designated
  column is rejected by QuestDB, so `last_updated` is sent only as the row
  timestamp, never as a named field.
- `ALTER TABLE ... DEDUP ENABLE UPSERT KEYS` works on a WAL table, but
  `SHOW COLUMNS` does not reliably report the upsert keys afterwards; the
  integration therefore owns table creation and validation and does not
  auto-repair existing tables.

## Required verification before production

- dedup behavior under out-of-order spool replay (long outage followed by
  drain) on the local bench;
- Home Assistant restart with restored states: no duplicate or wrongly timed
  rows;
- batch byte-limit calibration against the real payload distribution;
- `entity_id` symbol capacity at the expected multi-year cardinality growth.

## References

- QuestDB delivery semantics and exactly-once:
  <https://questdb.com/docs/concepts/delivery-semantics>
- QuestDB deduplication:
  <https://questdb.com/docs/concepts/deduplication>
- QSS: <https://github.com/CM000n/qss>
- Real-world data analysis:
  [../benchmarks/real-world-qss-data.md](../benchmarks/real-world-qss-data.md)

# ADR 0006: Bounded dead-letter retention with FIFO eviction

Status: accepted for implementation.

Date: 2026-08-29

## Context

The dead-letter store had hard capacity limits and `move_to_dead_letter`
raised `DeadLetterFullError` when they were reached. The worker treated that
as a permanent block: delivery stopped until a config-entry reload, even for
unrelated good rows behind the bad ones. Nothing in the runtime could free
dead-letter space (`delete_dead_letters` exists on the spool but no repair UI
or retention policy calls it), so a burst of deterministically bad rows (for
example after a schema change) wedged delivery of the entire queue.

Separately, the sticky `delivery_uncertain` flag lived only on pending and
dead-letter rows. When a row was eventually delivered successfully,
`mark_delivered` deleted it together with the flag, erasing the only
observable evidence that a retry may have duplicated the row in QuestDB.

## Decision

1. **The dead-letter store is a bounded ring buffer.** When a move would
   exceed the configured row or payload-byte limits, the oldest dead-letter
   rows are evicted first, atomically within the same transaction.
   `move_to_dead_letter` returns `(moved, evicted)`; the worker accumulates
   the evictions in the `dead_letter_evicted_events` snapshot counter, so
   automatic dropping is never silent. The newest failures always remain
   inspectable through `peek_dead_letters` and future diagnostics.
2. **Dead-letter capacity is no longer a blocking condition.** The defensive
   `DeadLetterFullError` paths (which the ring buffer makes unreachable) now
   schedule a retry with backoff instead of entering the permanent block.
   The worker's `blocked` state is reserved for genuinely permanent
   conditions: authentication failures and permanent HTTP errors that require
   a configuration change and reload.
3. **Uncertain deliveries remain observable after success.** Rows whose
   `delivery_uncertain` flag was set by an earlier retry are counted in the
   lifetime `uncertain_delivered_events` snapshot counter when they are
   finally delivered. With the ADR 0005 `DEDUP UPSERT` table this is a
   network-health signal rather than a duplicate warning; on any non-dedup
   schema it marks rows that may exist twice and can be reconciled by
   `event_id`.

## Consequences

Positive:

- deterministically bad rows can never wedge the delivery queue;
- the dead-letter store is self-maintaining within its bounds and always
  holds the most recent failures;
- eviction and uncertain deliveries are explicit, counted, and visible in
  diagnostics;
- `blocked` now means exactly "fix the configuration", matching operator
  expectations.

Negative:

- the oldest dead-letter rows are discarded without operator review; the
  eviction counter and rate-limited logging are the only trail;
- the snapshot grows by two counters;
- `delete_dead_letters` remains the only manual removal path until a repair
  UI exists.

The pending store keeps its hard limits without eviction: pending rows are
real, undelivered data, and their overflow must remain an explicit event.

## Verification completed

- unit tests: row-limit and byte-limit eviction with order and counter
  assertions; worker test proving three bad rows beyond a one-row dead-letter
  capacity are all dead-lettered (two evictions) while a good row behind them
  is delivered; uncertain-retry counter test;
- full suite: 75 unit and 7 integration tests pass on the local bench.

## References

- ADR 0002 (spool limits): [0002-durable-sqlite-spool.md](0002-durable-sqlite-spool.md)
- ADR 0005 (record format and dedup): [0005-questdb-record-format.md](0005-questdb-record-format.md)

# ADR 0015: Reclaim spool space so a storage pause can end

Status: accepted.

Date: 2026-09-19

## Context

[ADR 0014](0014-spool-pressure-policy.md) pauses persistence when the spool
filesystem reaches its reserve, and promises that the writer resumes by itself
once there is space again. The pressure benchmark
([../benchmarks/spool-pressure.md](../benchmarks/spool-pressure.md)) measured
that promise on a filesystem with a real limit and found it broken: deleting
delivered rows frees pages *inside* the SQLite file, the filesystem keeps the
space, free space stays below the reserve for good, and the writer stays paused
indefinitely. In the measured run the spool had delivered every row it held and
still occupied 46.9 MB of a 64 MiB filesystem, with `storage_recoveries` at 0.

The same run exposed two costs around that state: `storage_blocks` counted
attempts rather than pauses (284,249 for a single pause, because the guard is
re-evaluated on every persist attempt), and a shutdown whose flush could not
proceed retried with `asyncio.sleep(0)` - 93 % of one core for the whole timeout.

## Decision

1. **New spools are created with `PRAGMA auto_vacuum = INCREMENTAL`.** It is set
   before anything is written to the database - switching the journal mode to WAL
   already initializes the file, so setting it afterwards is silently ignored -
   and it is what makes `PRAGMA incremental_vacuum` able to return pages.
2. **The worker reclaims before accepting a pause.** When the guard reports free
   space below the reserve, the worker asks the owned spool for its freed pages
   and the WAL (`SpoolHandle.reclaim`), re-measures, and pauses only if the space
   did not come back. Reclamation never raises: it reports an error and the pause
   continues.
3. **One pass is bounded to 256 pages and passes are spaced by a second.** Moving
   pages writes through the WAL, so a pass consumes space before it returns any:
   a 4,096-page pass produced 16.9 MB of WAL and filled a test filesystem that was
   already at its reserve. 256 pages is at most ~1 MiB of extra WAL, and the
   interval caps reclamation at ~1 MiB/s, above the measured production payload
   rate of 0.03-0.12 MiB/s.
4. **Every pass truncates the WAL.** In WAL mode a freed page reaches the
   filesystem only after a checkpoint, so `wal_checkpoint(TRUNCATE)` follows the
   vacuum; without it the pages stay in the log file.
5. **A spool that predates this decision gets one rewrite per pause.** SQLite can
   change `auto_vacuum` only through a full rewrite, and a rewrite needs room for
   the live data, which a full disk does not have. `SpoolHandle.compact()`
   attempts it once per pause, leaves the database in incremental mode for good,
   and reports failure without raising.
6. **`storage_blocks` counts pauses, not attempts.** The attempt count moves to
   `storage_block_attempts`, and the pause is tracked as an explicit episode: the
   worker state changes for reasons of its own (shutdown moves it to `STOPPING`),
   and a counter derived from the state counts one pause many times.
7. **A flush that cannot proceed waits instead of spinning.** The stopping branch
   of the persist loop waits a 50 ms slice (bounded by the shutdown deadline)
   between attempts; the flush still takes a window that opens inside the slice.

## Consequences

- The promise of [ADR 0014](0014-spool-pressure-policy.md) §6 now holds where it
  was measured: against a real destination, the shipping guard and a bounded
  filesystem, the writer ended four pauses by itself, truncated a 4.9 MiB WAL to
  zero, flushed the events that had been waiting and reported
  `stopped_cleanly: true` in 2.6 s instead of burning the whole timeout.
- Reclamation runs only under pressure. While the filesystem has room, the spool
  file keeps its high-water mark and later writes reuse the freed pages instead of
  growing it. The disk the reserve has to cover is measured at ~2.0 bytes per
  payload byte, 4.6 KB per 2 KB row.
- A filesystem with no headroom at all can still refuse a pass, because the WAL
  needs room to move pages; the pass reports the error, the pause continues, and
  the next pass a second later tries again. The writer never fails on it.
- Legacy spools migrate on their first pause and behave as before until then.
- While a pause is open the persist loop re-checks the guard on every wake-up,
  which costs about 0.5 ms per attempt: measured at 16.9 % of one core with 300
  events/s offered (accelerated), i.e. roughly 3 % at the measured production
  peak of 61 events/s and under 1 % at the median rate. It is a cost proportional
  to the offered rate, not a spin.

## Verification

- Unit: incremental mode is set on new spools and read back; `reclaim` drains the
  freelist and truncates the WAL; `compact` rewrites a legacy database and leaves
  it in incremental mode; both report failures instead of raising; the worker
  reclaims before pausing and ends a pause once space returns; the pause counter
  stays at one while attempts grow; a blocked shutdown re-checks a handful of
  times instead of thousands. Six mutations of the fix turn the new tests red.
- Benchmark: `benchmarks/spool_pressure.py`; before and after numbers in
  [../benchmarks/spool-pressure.md](../benchmarks/spool-pressure.md).

# ADR 0013: Idle persist polling bound

Status: accepted.

Date: 2026-09-12

## Context

The persist loop waited on its wake events with a hard-coded 50 ms timeout:

```python
self._wake_persist.clear()
await self._wait_events([self._wake_persist, self._stop_async], 0.05)
```

That value had no recorded rationale - not in an ADR, not in a comment. Measured
with the service completely idle ([benchmarks/idle-path.md](../benchmarks/idle-path.md)),
it produced ~39 `SQLiteSpool.stats()` calls per second and 1.0-1.7 % of one CPU core:
a constant cost for a worker with nothing to do, which Raspberry-Pi-class hardware
pays several times over. Tracked as issue #2.

Issue #2 inferred that the 50 ms guarded a lost-wakeup race between
`_wake_persist.clear()` and a `submit()` that signals the same event. **That
inference was tested and is refuted.** `submit()` signals through
`_signal_loop` -> `loop.call_soon_threadsafe(self._wake_persist.set)`
(`worker.py:413`, `:576-583`), and a callback scheduled this way only runs when the
loop regains control. The persist loop does not yield between its drain and the
wait - drain, persist and `spool.stats()` are synchronous - so the queued `set`
always executes *after* the `clear()` and wakes the waiter immediately. Measured
with the fallback deliberately set to 1 s and a `submit()` fired from inside the
persist loop's iteration, the event was persisted after **1.07 ms** (that is the
pre-change order; the same measurement with a clear-before-drain order gives
0.93 ms, inside the noise).

So the 50 ms was nothing but a poll interval. It is not a guard, and no reordering
of the clear is needed: the change is the interval itself.

## Decision

Replace the literal with an explicit, validated setting:
`WorkerSettings.persist_idle_poll_seconds`, production default
`PROVISIONAL_PERSIST_IDLE_POLL_SECONDS = 1.0` (`const.py`), exposed in the options
flow's advanced step like every other worker timing.

It is documented as a **recovery fallback for a dropped signal**, not as a latency
parameter: `_signal_loop` swallows `RuntimeError` from an event loop that is already
closed (`worker.py:576-583`), and that is the case the bound exists for. The loop
order stays as it was, with a comment recording why clearing there is safe.

## Rejected alternatives

- **Keep 50 ms and document it.** The measured idle cost stays; ~20 wake-ups per
  second buy nothing that the signal path does not already provide.
- **Reorder the clear before the drain.** Measured to make no difference (1.07 ms
  versus 0.93 ms, both far below the bound). Rejected as churn without effect - and
  the measurement is recorded here so it is not re-proposed.
- **Wait without any timeout.** Cheaper still, but one dropped signal would strand
  accepted events in memory until the next event arrives - an unbounded loss window
  in exchange for about three wake-ups per second.
- **SQLite triggers or change notifications to learn about new rows.** The writer
  already knows when it persisted rows (`_wake_deliver.set()` right after
  persisting); notifications would add write-path cost to the durability-critical
  spool for no benefit. Also rejected in issue #2.
- **Raising `flush_interval_seconds` instead of adding a setting.** That is a
  delivery-latency parameter; conflating the two would silently change delivery
  timing for users.

## Consequences

- The idle cost drops to the delivery loop's own cadence, which is what remains:
  measured ~3 `stats()` calls per second and below 0.2 % of one core, from ~39 calls
  and 1.0-1.7 % (`docs/benchmarks/idle-path.md` carries both measurements).
- Persistence latency is unchanged: a new event is persisted as soon as `submit()`
  signals, independently of the bound.
- `WorkerSettings` gains a required field, so every construction site (production and
  tests) states it explicitly - consistent with the existing convention that all
  timings are required constructor inputs, and it keeps the value out of the worker.
- Shutdown behaviour is unchanged: the stopping branch still drains immediately on
  each iteration and still raises `WorkerShutdownError` once `_shutdown_deadline`
  passes (`worker.py:659-669`).

## Verification

- `tests/unit/test_worker.py::WriterServiceTests::test_a_submit_during_persistence_is_not_lost`
  pins the invariant that makes a long bound safe: a `submit()` fired from inside
  the persist loop's iteration (only that loop clears the wake event, so the test
  targets it by inspecting the running coroutine) is visible in the snapshot well
  inside 0.5 s while the fallback is 1 s. The test passes against both loop orders,
  which is exactly what the refuted hypothesis predicts - it guards the property, not
  a code shape.
- The latency measurement above (1.07 ms / 0.93 ms) came from a throwaway probe
  against the real service; the same construction lives in
  `benchmarks/idle_path.py`.
- The settings validation covers the new field (positive, finite).
- `benchmarks/idle_path.py` re-run before and after the change in the same two
  environments.
- Full unit and integration suites, plus the existing shutdown-deadline test.

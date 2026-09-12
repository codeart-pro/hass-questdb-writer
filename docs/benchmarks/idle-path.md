# Idle path benchmark

Date: 2026-09-12

## Question

What does the writer service cost while it has nothing to do - no ingress, empty
spool, nothing to deliver - and which of the micro-optimizations an external review
proposed for the worker are real?

## Environment

| Environment | Python | Platform |
|---|---|---|
| Development host | 3.11.15 | macOS arm64 |
| Development container | 3.14.6 | Linux aarch64 |

Two windows of 5 s each, plus 20,000 `snapshot()` calls, in both environments. Both
runs start the real `WriterService` with a counting spool proxy
(`benchmarks/idle_path.py`), `flush_interval_seconds = 1.0`, an empty spool and a
transport that raises when it is called, so a delivery during the measurement fails
instead of skewing the numbers.

The writer stack (worker/spool/event/ilp/transport/schema) imports no Home Assistant
module - only `__init__.py` does - so the benchmark runs without Home Assistant
installed. That is also its limit: **no Home Assistant code is executed here**.
Statements about Home Assistant behaviour below are cited from the source of the
pinned version (`hacs.json` -> 2025.1.0, paths under
`homeassistant/helpers/entityfilter.py`) rather than measured.

## Results

### Idle wake-ups and processor cost

| Metric | Host run 1 | Host run 2 | Container run 1 | Container run 2 |
|---|---:|---:|---:|---:|
| `SQLiteSpool.stats()` calls per second | 38.8 | 39.2 | 38.8 | 38.8 |
| `stats()` mean | 70.2 us | 57.2 us | 97.8 us | 85.3 us |
| `stats()` p99 | 781.0 us | 325.8 us | 611.8 us | 1128.1 us |
| CPU while idle | 0.95 % | 1.11 % | 1.67 % | 1.34 % |

### `snapshot()` cost (what the health sensors read)

| Metric | Host | Container |
|---|---:|---:|
| p50 | 2.3 us | 1.7 us |
| p99 | 6.1 us | 3.5 us |
| max (20,000 calls) | 216.4 us | 293.4 us |

Command (run from the repository root; no Home Assistant needed):

```text
python -m benchmarks.idle_path --idle-seconds 5 --repeats 2
```

## Interpretation

- An idle writer calls `stats()` **~39 times per second** and burns **1.0-1.7 % of
  one core** on this hardware. The cost is constant rather than proportional to the
  configuration: it is the same whether one entity or two thousand change state.
  Raspberry-Pi-class hardware, which is several times slower on this path, pays
  several times more.
- The cost is the call rate, not the query. `spool_stats` is maintained by SQLite
  triggers on both tables (`spool.py:138`, `:148`, `:158`, `:168`), so `stats()` is
  a point lookup of the singleton row (`spool.py:391-398`) - cheap per call, called
  too often.
- Two loops produce the rate on one thread: the persist loop polls its wake events
  with a fixed 50 ms bound (`worker.py:671-672`, `_wait_events(..., 0.05)`) and
  reads the counters (`worker.py:655`), and the delivery loop reads them again
  whenever it is woken (`worker.py:721`, `:756`). The 50 ms value has no documented
  rationale; it bounds the lost-wakeup race between `_wake_persist.clear()`
  (`worker.py:670`) and a `submit()` that signals the same event (`worker.py:413`).
- `snapshot()` is not a problem: p50 1.7-2.3 us, so the health sensors reading it
  every 30 s cost nothing measurable.
- The wake-up rate is tracked as issue #2; this document's numbers and command are
  the "before" measurement for that fix.

## Claims reviewed and rejected

The external review listed eight "trade-offs & potential issues". Four of them
describe a different mechanism than the code, two are correct but insignificant, and
two were real (idle wake-ups -> #2, eviction statement count -> #4). They are
recorded here so that the refuted ones do not return in every review round.

| # | Claim | Verdict | Evidence (current `main`) |
|---|---|---|---|
| 1 | `snapshot()` holds a lock that blocks event submission | Mechanism wrong, impact negligible | 21 `with self._lock:` blocks in `worker.py` (356, 369, 376, 392, 400, 422, 438, 469, 473, 484, 509, 520, 536, 569, 630, 677, 831, 932, 962, 1006, 1032), none of which performs blocking work - checked by extracting every block body and searching for spool/transport/JSON calls. All blocking work sits outside them: `spool.stats()` (508, 573, 635, 655, 721, 756), `EventEnvelope.from_bytes(...).to_ilp(...)` (871), `move_to_dead_letter` (879, 997), `transport.send_batch` via `asyncio.to_thread` (945), `record_attempt` (956, 984), `mark_delivered` (1030). Measured p50 1.7-2.3 us |
| 2 | `fnmatchcase()` runs on every state change with no caching | **Incorrect** | `entity_filter` is Home Assistant core's `EntityFilter` (`runtime.py:27`, `:89`, used at `:366-367`), not project code: globs are combined into one compiled regex (`helpers/entityfilter.py:148-162`), results are memoised with `@lru_cache(maxsize=MAX_EXPECTED_ENTITY_IDS)` (`entityfilter.py:208`, `:227`, `:247`, `:271`; `MAX_EXPECTED_ENTITY_IDS = 16384` in `homeassistant/const.py:1126`), and an empty filter degenerates to `return bool` (`entityfilter.py:199`). `fnmatchcase` is project code on a different path: `attribute_filter.py:29`, `:33` |
| 3 | The attribute filter builds a new dict for every event | Fact right, common case already skipped | When both lists are empty the filter is `None` (`__init__.py:202-207`) and the whole block is skipped (`runtime.py:374-381`), so only configurations with filters pay for the comprehension |
| 4 | `spool.stats()` runs on every delivery-loop iteration | **Real -> #2** | Measured ~39 calls/s and 1.0-1.7 % of one core while idle; call sites `worker.py:655`, `:721`, `:756`; poll bound `worker.py:671-672` |
| 5 | The error text is held in memory across retries | Correct, negligible | Truncated at 4 KiB (`worker.py:43`, `:282-286`); the spool also keeps `last_error TEXT` per row (`spool.py:107`, `:123`) |
| 6 | Jitter uses `random.random()`, so every integration gets its own seed | **Incorrect** | `random_source` defaults to the module-level `random.random` (`worker.py:302`; `_Backoff` at `worker.py:249-264`), so one Mersenne Twister is shared per Home Assistant process, not per integration. The draws still differ, so jitter works - the stated rationale does not |
| 7 | Every event is JSON-encoded twice | Correct | `json_dumps(attributes)` (`runtime.py:388`) -> `EventEnvelope.to_bytes()` encodes the whole envelope (`event.py:81-89`) -> `from_bytes` on delivery (`event.py:92`, called at `worker.py:871`). The review's "binary serialization would save 20-30 %" has no source and must not be repeated as a measurement |
| 8 | Dead-letter eviction is O(n) and should use a FIFO ring buffer | Code read correctly, the fix describes what is already there -> #4 | Eviction deletes the oldest row through its primary-key index (`spool.py:114`, `ORDER BY dead_letter_id` at `spool.py:692`) inside the same transaction as the insert (`spool.py:652`). The remaining improvement is batching the statements, not a ring buffer |

### The "dead-letter store full" guard is defensive, not a live failure mode

`spool_stats` is maintained by SQLite triggers on both tables (`spool.py:138`,
`:148`, `:158`, `:168`), so the counters follow inserts and deletes even when rows
are changed outside the spool API. `tests/unit/test_spool.py` proves it by deleting
rows through a second connection and observing the counters correct themselves
(`SQLiteSpoolDefensiveBranchTests::test_counters_follow_out_of_band_changes`).
`DeadLetterFullError` in `move_to_dead_letter` therefore only fires on a corrupted
database, never through normal eviction.

## Reproduction

From the repository root, with any Python that can import the component's
Home-Assistant-free modules:

```text
python -m benchmarks.idle_path --idle-seconds 5 --repeats 2
```

The script loads the component's modules by path under a synthetic package, so it
does not need `homeassistant` installed and does not touch a running Home Assistant.
Sample counts and CPU time are process-wide for the measurement window; the two
loops run on the same worker thread, which is why their rates add up on one core.

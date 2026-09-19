# Spool pressure benchmark

Date: 2026-09-19

> This document holds two runs of the same harness. The **incident run** records
> what the writer did before the fix: a pause that never ends, a confused pause
> counter, and a shutdown that spins (findings 1-4, all reproduced on 2026-09-19).
> The **same-conditions run** repeats the incident with the baseline revision and
> with the fix under identical parameters; that comparison, not the incident run,
> is the evidence for [ADR 0015](../decisions/0015-spool-space-reclamation.md).

## Question

[ADR 0014](../decisions/0014-spool-pressure-policy.md) decides what happens when
the spool cannot grow: pending events are never evicted, persistence pauses, and
the writer is supposed to resume by itself once there is space again. The ADR
left two things to verify on a real filesystem:

1. does the free-space guard stop the writer before the disk is consumed, and
   does the reserve mean what it says;
2. does the writer actually resume when the space comes back.

This benchmark runs that incident end to end - real spool, real worker, real
transport, real QuestDB - on a filesystem with a small and real capacity, and
measures what the installation looks like from the outside.

## Environment

- Home Assistant container `ghcr.io/home-assistant/home-assistant:2026.7.2`,
  native ARM64 under Podman/libkrun, Python 3.14.6, SQLite 3.53.2.
- Spool on a tmpfs of 64 MiB: a real filesystem with a real limit, small enough
  that the incident fits in half a minute.
- Guard reserve `max(16 MiB, 25 % of 64 MiB)` = 16 MiB, so the spool may consume
  at most 48 MiB.
- Spool limits deliberately above the filesystem (200,000 rows / 128 MiB) so the
  free-space guard, not the payload counter, is what trips.
- Payload 2,011 bytes per event requested; the serialized spool payload measures
  2,289 B, which is the number the disk accounting uses.
- Production reference: 13 events/s median, 61 events/s peak
  ([real-world-qss-data.md](real-world-qss-data.md)).

## Method

The outage is not simulated with a fake transport. The writer gets the real
`IlpHttpTransport` pointing at `127.0.0.1:9000`, where nothing listens, so every
delivery fails with a real `ConnectionRefusedError` and the real retryable
classification. The recovery opens a TCP forwarder on that same address, which
forwards to the real QuestDB on the compose network: the very same transport
object then delivers into a live server.

Events are built by the same `EventEnvelope`/`to_spool_event()` code the listener
uses, so the payload stored in SQLite is the real one. A sampler records every
second: pending rows and bytes, the size of the spool file and its WAL, free
space, the process CPU time, and the counters the runtime publishes.

The verdict is computed from **event identity**, not from row counts:

- the ids the harness accepted are remembered (`submit()` returned true);
- after the stop, the spool is read directly (`pending.event_id`) and the
  destination is asked for `count()`, `count_distinct(event_id)` and the
  membership of **every** accepted id that is not in the spool - in chunks of
  300, because a single response carrying 13,000 ids is refused by the
  transport. A chunk that reports fewer ids than it asked about is re-read
  (`select distinct event_id ...`) and the difference is recorded, so the result
  names the missing events, not just how many are missing;
- the verdict requires `unaccounted_events == 0` (accepted minus delivered minus
  durable), `duplicate_rows_in_table == 0`, `counts_agree` and an empty
  `missing_events`, and it is invalidated outright if the table reset or any
  verification query fails.

Counting rows could not see a loss that another event's re-delivery compensated;
comparing the full set of distinct ids can. The result also carries its own
provenance - harness digest and revision, component revision, dirty flag and the
complete argument list (`environment.arguments`) - so the comparison below can be
audited from the two result files without trusting this document.

What the sender does when a batch cannot be acknowledged locally, and what the
destination then holds, is measured against the real QuestDB in the integration
suite (`test_a_resend_after_a_failed_local_ack_keeps_one_row`): the batch is sent
again on the delivery backoff, and the table keeps exactly one row and one
distinct event id for that state change.

Two properties are probed rather than assumed:

- **The startup condition on real storage.** After every timing above has been
  measured, the harness fills the tmpfs until the write itself fails with
  `ENOSPC`, opens a fresh spool on it and records what the spool reports
  (`startup_on_full_filesystem`). The probe is last on purpose: it is destructive
  for the filesystem, so it must not sit inside a window whose duration is
  published. A test cannot do this at all: `PRAGMA max_page_count` is per
  connection, and a test cannot fill a filesystem.
- **What the writer did while it was measured.** Every sample carries the writer's
  state, so the result is reported per state (`state_seconds`,
  `cpu_seconds_in_blocked_state`) instead of attributing a whole window to one
  state. The attribution is as fine as the sampling interval, which these runs
  set to 0.25 s (`--sample-seconds`): a transition inside an interval counts for
  the later state, so a residency figure is accurate to one interval per
  transition.

## Incident run (before the fix)

One run, every sample taken from its CSV:

| t (s) | state | pending rows | pending MiB | db MiB | wal MiB | fs free MiB | delivered |
|---:|---|---:|---:|---:|---:|---:|---:|
| 0 | running | 0 | 0.0 | 0.0 | 0.0 | 63.93 | 0 |
| 3 | retry_wait | 2,993 | 6.5 | 10.8 | 4.4 | 48.72 | 0 |
| 6 | retry_wait | 5,998 | 13.1 | 22.9 | 4.4 | 36.65 | 0 |
| 9 | retry_wait | 8,883 | 19.4 | 33.7 | 4.4 | 25.82 | 0 |
| 12 | blocked | 11,279 | 24.6 | 44.7 | 4.7 | 14.57 | 0 |
| 21 | blocked | 11,279 | 24.6 | 44.7 | 4.7 | 14.57 | 0 |
| 27 | stopping | 0 | 0.0 | 44.7 | 4.9 | **14.41** | 11,279 |

| Quantity | Value |
|---|---|
| Free space when the guard paused persistence | 15,106,048 B (14.4 MiB) against the 16 MiB reserve |
| Pending at that moment | 11,279 rows / 25,817,631 B (24.6 MiB) |
| On disk at that moment | 44.7 MiB database + 4.9 MiB WAL = 49.6 MiB |
| Disk cost | 2.02 B per payload byte, 4,617 B per row | 2.00 B per payload byte, 4,581 B per row |
| Events the guard let through while blocked | 0; 8,923 were refused at the full ingress queue and counted |
| Events moved to the dead letter | **0** |
| Delivered after the destination returned | 11,279 rows in 3.4 s |
| Rows found in QuestDB afterwards | 11,279 |

The guard itself works as designed: persistence stops one `statvfs` after free
space falls under the reserve, the bounded ingress queue absorbs the difference,
overflow is counted instead of silently dropped, and nothing reaches the dead
letter. The spool cost 2.01 bytes of disk per payload byte, so a spool limit of
64 MiB of payload is about 130 MiB of disk plus the WAL.

### Baseline finding 1: the pause never ends, because the space never comes back

The reserve is measured against the filesystem, and the spool is what consumed
it. When delivery drains the spool, SQLite frees pages *inside* the file: the
file keeps its size, the filesystem keeps the space, free space stays at 14.4 MiB
under the 16 MiB reserve, and the guard keeps refusing. In the run above pending
fell to 0 and all 11,279 rows reached QuestDB, while `db_bytes` stayed at 44.7 MiB
and free space stayed at 14.41 MiB to the last sample. `storage_recoveries`
stayed 0: no durable write ever became possible again, so the writer would have
stayed paused indefinitely, with an operator having to free space by hand.

A direct probe on the same data set (12,000 rows of 2,011 bytes, fully delivered,
49 MB file with 12,027 free pages) shows the mechanisms:

| Mechanism | Result |
|---|---|
| `PRAGMA wal_checkpoint(TRUNCATE)` | releases the WAL (4.9 MiB in the run above) |
| `PRAGMA incremental_vacuum(256)` × 60 | drains the freelist in 31 ms - **only if the database was created with `auto_vacuum=INCREMENTAL`**; on a database created without it, it is a no-op |
| `VACUUM` | compacts the file (49 MB to 8 KB on an emptied database); the new size is visible after the following checkpoint, and it needs room for the live data while it runs |

So the promise in the README ("the writer resumes by itself once ... the
filesystem" has space) and in [ADR 0014](../decisions/0014-spool-pressure-policy.md)
§6 ("The worker recovers by itself") did not hold on a filesystem whose space is
mostly held by the spool itself - which is exactly the case the guard exists for.
The writer drains everything it can, then stays paused forever. The reclaim work
in [ADR 0015](../decisions/0015-spool-space-reclamation.md) is the answer.

### Baseline finding 2: what "accepted" means at shutdown

1,100 events of the 12,379 accepted were still in the in-memory queue or the held
batch when the writer stopped, and the shutdown flush had nowhere to write them:
the guard was still blocking. That is consistent with at-least-once - those events
were never durable - but the exposure was real: at shutdown a blocked writer
loses up to `ingress_queue_capacity` plus one persist batch of accepted events.
The same-conditions run below shows 0 such events with the fix, in a run that
accepts *more* events.

### Baseline finding 3: `storage_blocks` counts attempts, not pauses

[architecture.md](../architecture.md) says `storage_blocks` "counts the pauses".
The counter incremented on every blocked persist attempt: 23.2 per second in the
measured steady blocked state, 284,249 by the end of the run, while the number of
pauses in that run was one. The rate-limited warning reads "cumulative pauses:
284249" as a result. Attempts are now counted separately in
`storage_block_attempts`, and `storage_blocks` is one per pause.

### Baseline finding 4: a blocked writer is not idle, and a blocked shutdown is a hot loop

- Blocked steady state in the same-conditions run: 17.7 % of one core for the
  whole process over a 10 s window (the harness's own submission loop is 3.5-6 µs
  per envelope, [listener-cost.md](listener-cost.md), i.e. under 1 % at 1,000
  events/s). The production rates are measured separately in the section below.
- Shutdown while blocked: `stop(timeout_seconds=25)` used the full 25.0 s and
  **91.1 % of one core**, and reported `stopped_cleanly: false`. The persist
  loop's stopping branch retried with `await asyncio.sleep(0)`, which yields to
  the event loop without waiting, so a flush that could not proceed spun until
  the deadline. On a Home Assistant host that is a full core spent during reload
  or shutdown, at the moment the disk is already full.

## Same-conditions run: baseline and fix, identical parameters

Both revisions were run with the same harness, the same 64 MiB tmpfs, the same
parameters (1,000 events/s offered, 10 s blocked window, 25 s stop timeout,
`payload 2,011 B`), differing only in the revision under test: the baseline is
the commit that measured the incident (`58896f4`), mounted read-only and pointed
at with `--component-dir`. The result files record both revisions and the full
argument list, so this is a property of the artifacts rather than of the text.

| Quantity | Baseline | With the fix |
|---|---|---|
| Verdict | **`nothing_lost: false`** | **`nothing_lost: true`** |
| Accepted events | 12,144 | 13,213 |
| Persisted events | 11,044 | 13,213 |
| Distinct events in QuestDB | 11,044 | 12,446 (+ 767 still durable in the spool) |
| Accepted events unaccounted for anywhere | **1,100** | **0** |
| Accepted ids verified at the destination and missing | **1,100 of 12,144** | **0 of 12,446** |
| Duplicate rows in the destination | 0 | 0 |
| Pauses ended by the writer (`storage_recoveries`) | **0** | **8** |
| `storage_blocks` in the blocked window | 209 (one pause, counted per attempt: ~20/s) | 3 (8 pauses in the run) |
| CPU in the blocked state, in the window | 1.37 s over 9.30 s = **14.8 % of one core** | 1.96 s over 9.14 s = **21.4 %** |
| A spool opened while the filesystem is full | `SpoolError: failed to initialize SQLite spool` (**unclassified**) | **`SpoolDiskFullError: database or disk is full`** |
| Shutdown with a flush that could not proceed | 25.0 s, **90.1 %** of one core, `stopped_cleanly: false` | **1.4 s, 11.0 %**, `stopped_cleanly: true` |
| Draining after the destination returned | 3.1 s | 3.0 s |
| Disk cost | 2.01 B per payload byte, 4,611 B per row | 2.00 B per payload byte, 4,580 B per row |

The startup row comes from the probe described below: the harness fills the tmpfs
until the write itself fails (`OSError: ENOSPC`), then opens a fresh spool and
records what it produces. The baseline answers with an unclassified `SpoolError`,
the fixed revision with the storage condition it is, which is the difference the
setup failure has to carry to the operator.

The two runs do not accept exactly the same number of events: the offered rate
and the window are identical, but what each revision can ingest before its
bounded queue fills depends on how it uses the disk it has. That is why the
comparison is made on exposure and recovery metrics, not on absolute counts: the
baseline ends with 1,100 accepted events that never became durable and cannot
deliver them (they were only in memory), while the fix ends with 13,278 of 13,278
persisted, 12,611 already delivered and 667 durable in the spool for the next
delivery pass.

The last row of the earlier version of this document reported 233 re-delivered
rows; that number came from comparing row counts and is not reproducible as an
identity statement. The current verdict compares distinct event ids, finds no
duplicate rows and no unaccounted event, so nothing is lost and nothing is
counted twice.

## Blocked state at the production rates

Measured, not extrapolated: the filesystem is filled at 300 events/s (so the run
fits in a minute) and the offered rate is dropped to the production peak and
median for a 20 s window. Each run reports what the writer actually did in that
window, **per state** (`blocked_steady_state.state_seconds`), so no window is
attributed to a state it did not spend time in.

| Quantity | 61 events/s (peak) | 13 events/s (median) |
|---|---|---|
| Time in the blocked state inside the window | 11.8 s of 20 s | **0 s** |
| CPU in the blocked state | 1.53 s over 11.8 s = **13.0 % of one core** | — (the window contains no pause) |
| New pauses inside the window | 9 (10 in the whole run) | 0 (1 in the whole run, before the window) |
| Time in delivery `retry_wait` in the run | 41.9 s at 33.0 % of one core | 59.2 s at 24.3 % of one core |
| Accepted / delivered | 12,616 / 12,539 (+ 77 durable) | 11,548 / 11,548 |
| Verdict | `nothing_lost: true` | `nothing_lost: true` |
| Shutdown | 0.60 s, 6.4 % of one core | 0.008 s |

Two claims have to be separated here, because an earlier version of this document
conflated them:

- **A pause that is open costs CPU in proportion to the offered rate.** At the
  production peak the blocked state cost 13.0 % of one core, and in the
  same-conditions runs at 1,000 events/s offered the same state cost 14.8 %
  (baseline) and 21.4 % (fixed): the cost tracks how often the writer is asked to
  persist, not how full the disk is.
- **At the median rate the writer is not in a pause.** The 20 s window at
  13 events/s contained no blocked time at all: the reclamation keeps the disk
  usable, and the run is spent in delivery `retry_wait` (24.3 % of one core),
  waiting for the destination rather than for the disk. Quoting a CPU figure from
  that window as "the cost of a storage pause" would have been wrong, which is
  exactly why the per-state breakdown exists.

## What this does not cover

- The production filesystem: here the spool owns the disk. Nothing is measured
  about a disk shared with the recorder, logs and backups beyond the reserve
  arithmetic itself.
- The payload is a single size (2,011 B requested) and the events are one entity.
- Long outages: tens of seconds here, not the multi-hour outage in which the
  recorder's own growth matters.

## Reproduction

Same-conditions run (baseline and fix differ only in `--component-dir`):

```text
git worktree add --detach dev/scratch/baseline-58896f4 58896f4
podman run --rm --network hass-questdb-writer-devstack_default \
    --tmpfs /spool:rw,size=64m -v "$PWD":/repo:ro -v "$PWD/dev/scratch/baseline-58896f4":/baseline:ro \
    -v "$PWD/dev/scratch/pressure-run":/out -e PYTHONDONTWRITEBYTECODE=1 -w /repo \
    --entrypoint python3 ghcr.io/home-assistant/home-assistant:2026.7.2 \
    -m benchmarks.spool_pressure --questdb-host questdb --event-rate 1000 \
    --blocked-rate 1000 --blocked-seconds 10 --stop-timeout-seconds 25 \
    --component-dir /baseline/custom_components/hass_questdb_writer \
    --samples-csv /out/samples-baseline.csv
```

Drop `--component-dir` for the fixed revision; use `--event-rate 300
--blocked-rate 61` (or `13`) for the production-rate windows. `--blocked-rate`
separates the fill rate from the rate offered while blocked, which is what makes
a production-rate measurement fit in a minute.

The isolated probes for the reclaim mechanisms are `dev/scratch/vac_probe.py`,
`dev/scratch/auto_vacuum_probe.py` and `dev/scratch/incremental_vacuum_probe.py`
(same container, same sized filesystem).

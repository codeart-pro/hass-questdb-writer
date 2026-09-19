# Spool pressure benchmark

Date: 2026-09-19

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
- Payload 2,011 bytes per event: the measured maximum attributes size of real
  Home Assistant data ([real-world-qss-data.md](real-world-qss-data.md)).
- 1,000 events/s offered. The production profile is 13 events/s median and 61
  events/s peak, so the offered rate is accelerated to reach the limit inside the
  window; every other timing is a shipping default.

## Method

The outage is not simulated with a fake transport. The writer gets the real
`IlpHttpTransport` pointing at `127.0.0.1:9000`, where nothing listens, so every
delivery fails with a real `ConnectionRefusedError` and the real retryable
classification. The recovery opens a TCP forwarder on that same address, which
forwards to the real QuestDB on the compose network: the very same transport
object then delivers into a live server, and the row count in QuestDB verifies
what actually arrived.

Events are built by the same `EventEnvelope`/`to_spool_event()` code the listener
uses, so the payload stored in SQLite is the real one. A sampler records every
second: pending rows and bytes, the size of the spool file and its WAL, free
space, the process CPU time, and the counters the runtime publishes.

```text
python -m benchmarks.spool_pressure --questdb-host questdb --event-rate 1000 \
    --payload-bytes 2011 --samples-csv /samples/samples.csv
```

## Result

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
| Disk cost | **2.01 bytes per pending byte**, 4,607 bytes per row |
| Events the guard let through while blocked | 0; 8,923 were refused at the full ingress queue and counted |
| Events moved to the dead letter | **0** |
| Delivered after the destination returned | 11,279 rows in 3.4 s |
| Rows found in QuestDB afterwards | 11,279 - equal to what was persisted |

The guard itself works as designed: persistence stops one `statvfs` after free
space falls under the reserve, the bounded ingress queue absorbs the difference,
overflow is counted instead of silently dropped, and nothing reaches the dead
letter. The spool cost 2.01 bytes of disk per payload byte, so a spool limit of
64 MiB of payload is about 130 MiB of disk plus the WAL.

## Finding 1: the pause never ends, because the space never comes back

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
§6 ("The worker recovers by itself") does not hold today on a filesystem whose
space is mostly held by the spool itself - which is exactly the case the guard
exists for. The writer drains everything it can, then stays paused forever.

## Finding 2: what "accepted" means at shutdown

1,100 events of the 12,379 accepted were still in the in-memory queue or the held
batch when the writer stopped, and the shutdown flush had nowhere to write them:
the guard was still blocking. That is consistent with at-least-once - those events
were never durable - but the exposure is worth naming: at shutdown a blocked
writer loses up to `ingress_queue_capacity` plus one persist batch of accepted
events.

## Finding 3: `storage_blocks` counts attempts, not pauses

[architecture.md](../architecture.md) says `storage_blocks` "counts the pauses".
The counter increments on every blocked persist attempt: 23.2 per second in the
measured steady blocked state, 284,249 by the end of the run, while the number of
pauses in that run was one. The rate-limited warning reads "cumulative pauses:
284249" as a result. An operator cannot act on a number like that.

## Finding 4: a blocked writer is not idle, and a blocked shutdown is a hot loop

- Blocked steady state: 15.3 % of one core for the whole process over a 10 s
  window. The submission loop in the harness is bounded by the measured 3.5-6 µs
  per envelope ([listener-cost.md](listener-cost.md)), i.e. under 1 % at 1,000
  events/s, so the writer is the rest; at the production rate of 13-61 events/s
  the wake-ups are bounded by the 1 s persist poll instead. This is an upper
  bound, not a production figure.
- Shutdown while blocked: `stop(timeout_seconds=5)` used 5.0 s and **93 % of one
  core**, and reported `stopped_cleanly: false`. The persist loop's stopping
  branch retries with `await asyncio.sleep(0)`, which yields to the event loop
  without waiting, so a flush that cannot proceed spins until the deadline. On a
  Home Assistant host that is a full core spent during reload or shutdown, at the
  moment the disk is already full.

## What this does not cover

- The production filesystem: here the spool owns the disk. Nothing is measured
  about a disk shared with the recorder, logs and backups beyond the reserve
  arithmetic itself.
- The event rate: 1,000/s is accelerated, and the payload is a single size.
- Long outages: tens of seconds here, not the multi-hour outage in which the
  recorder's own growth matters.

## Reproduction

```text
podman run --rm --network hass-questdb-writer-devstack_default \
    --tmpfs /spool:rw,size=64m -v "$PWD":/repo:ro -v "$PWD/dev/scratch/pressure-run":/samples \
    -e PYTHONDONTWRITEBYTECODE=1 -w /repo --entrypoint python3 \
    ghcr.io/home-assistant/home-assistant:2026.7.2 -m benchmarks.spool_pressure \
    --questdb-host questdb --event-rate 1000 --payload-bytes 2011 \
    --samples-csv /samples/samples.csv
```

The isolated probe for the reclaim mechanisms is `dev/scratch/vac_probe.py` (same
container, same sized filesystem).

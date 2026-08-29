# SQLite spool benchmark

Date: 2026-08-29

## Question

Does `WAL + synchronous=FULL` provide enough local enqueue throughput, and how
much does a transaction batch reduce the durability cost?

## Environment

- Home Assistant container: `ghcr.io/home-assistant/home-assistant:2026.7.2`
- Architecture: native ARM64 under Podman/libkrun
- Python: 3.14.6
- SQLite: 3.53.2
- database location: temporary container-local filesystem
- journal mode: WAL
- synchronous mode: FULL
- events per sample: 20,000
- serialized payload: 256 bytes per event
- repeats: 3

The benchmark measures insertion into the durable pending spool. It does not
include Home Assistant event conversion, JSON serialization, ILP encoding, or
QuestDB delivery.

## Results

| Events per transaction | Runs, events/s | Median events/s |
|---:|---:|---:|
| 1 | 19,922 / 18,552 / 19,867 | 19,867 |
| 10 | 44,564 / 44,229 / 43,011 | 44,229 |
| 100 | 52,568 / 52,991 / 50,628 | 52,568 |
| 500 | 56,899 / 44,514 / 53,286 | 53,286 |

Command:

```text
python -m benchmarks.sqlite_spool \
  --events 20000 \
  --payload-bytes 256 \
  --batch-sizes 1 10 100 500 \
  --repeats 3
```

## Interpretation

The lowest measured sample was 18,552 events/s. The production Home Assistant
event rate has not yet been measured, so this result does not by itself prove
production capacity. A 100-event transaction increased median throughput by
about 2.65 times over one durable transaction per event. Increasing from 100 to
500 events provided only about 1.4% additional median throughput and showed
more variance.

This supports a batch-aware spool API and shows that SQLite is not a throughput
bottleneck in this local environment. It does not select the production batch
size: the latency threshold, shutdown behavior, ingress queue capacity, and the
actual production filesystem still need to be measured together.

## Reproduction

Run the module from the repository root in the exact target Python/container.
The script creates fresh temporary databases for every sample and validates the
inserted row count before closing each spool.

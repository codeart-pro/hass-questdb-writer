# Listener per-event cost benchmark

Date: 2026-09-19

## Question

A review asked whether the work the listener does inside the Home Assistant
event loop - serializing the attributes, building and validating the envelope,
and serializing the spool payload - is worth optimizing. The concern was the
`json.loads` round-trip `EventEnvelope.__post_init__` performs to validate the
attributes it just received.

This measurement answers it with numbers instead of intuition.

## Environment

- Home Assistant container: `ghcr.io/home-assistant/home-assistant:2026.7.2`
- Architecture: native ARM64 under Podman/libkrun
- Python: 3.14.6
- serializer: `homeassistant.helpers.json.json_dumps` (the call the runtime makes)
- attributes sizes: the harness targets 139 B, 191 B and 2011 B of serialized
  JSON - the measured average, p99 and maximum from
  [real-world-qss-data.md](real-world-qss-data.md) - and the padding lands at
  146 B, 197 B and 2017 B, which is what the table below reports. The target is
  the request, not the outcome; the raw result carries both
  (`target_attributes_bytes`, `measured_attributes_bytes`).
- samples per measurement: 20,000; repeats: 5; warm-up: 2,000 samples discarded
  before measuring

## Results

Per phase, in microseconds, over 5 repeats of 20,000 samples each after the
warm-up. Every cell is min / median / max **across the repeats** for that
statistic, so one lucky run is not the measurement:

| Attributes (B) | `json_dumps` p50 | `EventEnvelope(...)` p50 | `EventEnvelope(...)` p99 | `to_spool_event()` p50 | total mean |
|---:|---:|---:|---:|---:|---:|
| 146 | 0.67 / 0.67 / 0.67 | 3.50 / 3.54 / 3.58 | 10.8 / 12.2 / 14.3 | 7.67 / 7.71 / 7.75 | 13.2 / 13.3 / 13.5 |
| 197 | 0.71 / 0.75 / 0.75 | 3.83 / 3.88 / 3.92 | 4.8 / 13.2 / 14.2 | 8.04 / 8.08 / 8.13 | 14.0 / 14.1 / 14.5 |
| 2017 | 1.17 / 1.17 / 1.17 | 6.00 / 6.00 / 6.04 | 14.4 / 17.3 / 18.9 | 14.6 / 14.7 / 14.8 | 24.3 / 24.7 / 29.1 |

`total` is the sum of the three phases per event, not a fourth measurement.

Worst p99 over all repeats and sizes: **0.108 ms**, i.e. **0.66 %** of the
16.4 ms one event may occupy at the measured production peak (61 events/s; the
median rate is 13 events/s, i.e. 76.9 ms per event).

The tail of the two small sizes moves by a factor of three between repeats
(4.8-14.3 µs) while their medians stay within a few percent: that is the usual
shape of a micro-benchmark tail, and it is the reason the spread is reported
instead of a single run.

Command:

```text
python -m benchmarks.listener_cost --events 20000 --repeats 5 --warmup 2000
```

## Interpretation

The envelope construction - which includes the `json.loads` validation - costs
3.5-6.0 µs per event, and the whole listener path 13-29 µs on average. At the
measured rates that is a fraction of a percent of the available loop time, and
the event rate would have to grow by more than two orders of magnitude before
this path became the bottleneck.

The validation is therefore kept as it is: it is what turns a malformed
attributes payload into a rejected event at the point where it is created,
instead of an unreadable spool payload later. A rewrite that removes the
round-trip would trade a real guarantee for a cost we cannot measure in
practice.

The measurement is a micro-benchmark, not a system measurement: it does not
include Home Assistant's own event dispatch, the entity filter, or contention
with the rest of the installation. The load it is meant to bound
([real-world-qss-data.md](real-world-qss-data.md), 61 events/s peak) leaves
enough headroom that those costs would have to be two orders of magnitude
larger before the conclusion changed.

## Reproduction

Run the module from the repository root, in the exact target Python/container:

```text
podman exec -w /tmp/run -e PYTHONPATH=/tmp/run $C python3 -m benchmarks.listener_cost
```

Outside Home Assistant the module falls back to stdlib `json.dumps`, so the
numbers are comparable but the serializer differs; the run above used the Home
Assistant serializer, which is the one the runtime uses.

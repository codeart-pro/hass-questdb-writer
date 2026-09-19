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
- attributes sizes: 146 B, 197 B, 2017 B of serialized JSON, i.e. the measured
  average (139 B), p99 (191 B) and maximum (2011 B) from
  [real-world-qss-data.md](real-world-qss-data.md)
- samples per measurement: 20,000; repeats: 3

## Results

Median (p50) and tail (p99) per phase, in microseconds, first repeat:

| Attributes (B) | `json_dumps` | `EventEnvelope(...)` | `to_spool_event()` | total mean / p99 |
|---:|---:|---:|---:|---:|
| 146 | 0.67 / 0.96 | 3.54 / 5.25 | 7.75 / 60.9 | 13.5 / 71.0 |
| 197 | 0.75 / 1.08 | 3.83 / 8.42 | 8.08 / 66.3 | 14.6 / 79.5 |
| 2017 | 1.17 / 4.42 | 6.04 / 28.5 | 14.67 / 85.4 | 26.4 / 100.1 |

Worst p99 over all repeats and sizes: **0.13 ms**, which is **0.8 %** of the
16.4 ms of loop time one event may consume at the measured production peak
(61 events/s; the median rate is 13 events/s, i.e. 76.9 ms per event).

Command:

```text
python -m benchmarks.listener_cost --events 20000 --repeats 3
```

## Interpretation

The envelope construction - which includes the `json.loads` validation - costs
3.5-6.0 µs per event, and the whole listener path 14-26 µs on average. At the
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

#!/usr/bin/env python3
"""Measure the per-event cost of the listener path.

Answers one review question with numbers instead of intuition: is the JSON work
the listener does inside the Home Assistant event loop worth optimising?

For realistic payloads (attributes of 139 B average, 191 B p99, 2011 B max,
measured in docs/benchmarks/real-world-qss-data.md) it times, per event:

1. `attributes_json` serialization - Home Assistant's `json_dumps` when the HA
   package is importable, stdlib `json.dumps` otherwise (the same call the
   runtime makes);
2. `EventEnvelope(...)` construction, which validates the payload by parsing it
   again (`json.loads` in `__post_init__`) and checks the timestamps;
3. `to_spool_event()`, which serializes the versioned spool payload.

The result is printed next to the per-event budget the measured production rate
implies: 13 events/s median and 61 events/s peak, i.e. 76.9 ms and 16.4 ms of
loop time per event respectively.

The writer stack imports no Home Assistant module - only `__init__.py` does -
so this runs on a bare Python install too; inside the HA container it also
measures the shielded HA serializer.

Usage:

    python -m benchmarks.listener_cost
    python -m benchmarks.listener_cost --events 20000 --repeats 3
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import platform
import statistics
import sys
import time
import types
from pathlib import Path
from typing import Any, Callable

COMPONENT = (
    Path(__file__).resolve().parent.parent
    / "custom_components"
    / "hass_questdb_writer"
)

_LOADED_MODULES = ("const", "ilp", "event")

# Attribute JSON sizes to measure, from the real-world QSS analysis.
PAYLOAD_SIZES = (139, 191, 2_011)

EVENTS_PER_SECOND_MEDIAN = 13.0
EVENTS_PER_SECOND_PEAK = 61.0


def load_component() -> Any:
    """Import the component's Home-Assistant-free modules under a synthetic package.

    Modules already pulled in by a relative import are reused instead of executed
    again, so `event` keeps the module objects it captured.
    """
    package = types.ModuleType("hqw")
    package.__path__ = [str(COMPONENT)]
    sys.modules["hqw"] = package
    for name in _LOADED_MODULES:
        if f"hqw.{name}" in sys.modules:
            continue
        spec = importlib.util.spec_from_file_location(
            f"hqw.{name}", COMPONENT / f"{name}.py"
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[f"hqw.{name}"] = module
        spec.loader.exec_module(module)
    return sys.modules["hqw.event"]


def serializer() -> tuple[Callable[[Any], str], str, Callable[[Any], str] | None]:
    """Return (serializer, name, ha serializer or None)."""
    try:
        from homeassistant.helpers.json import json_dumps as ha_dumps
    except Exception:
        return json.dumps, "stdlib json.dumps", None
    return ha_dumps, "homeassistant.helpers.json.json_dumps", json.dumps


def attributes_of_size(target_bytes: int) -> dict[str, Any]:
    """A realistic attribute dict whose serialized form is about target_bytes."""
    attributes: dict[str, Any] = {
        "friendly_name": "Garden temperature",
        "unit_of_measurement": "°C",
        "device_class": "temperature",
        "state_class": "measurement",
        "supported_features": 0,
    }
    fixed = len(json.dumps(attributes, ensure_ascii=False))
    padding = max(0, target_bytes - fixed - 12)
    if padding:
        attributes["update_interval_note"] = "x" * padding
    return attributes


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * fraction))
    return ordered[index]


def measure_phase(
    call: Callable[[], Any], samples: int
) -> list[float]:
    durations: list[float] = []
    for _ in range(samples):
        started = time.perf_counter()
        call()
        durations.append(time.perf_counter() - started)
    return durations


def measure_size(
    event_module: Any,
    dumps: Callable[[Any], str],
    target_bytes: int,
    samples: int,
) -> dict[str, Any]:
    attributes = attributes_of_size(target_bytes)
    serialized = dumps(attributes)
    envelope_kwargs = {
        "event_id": "0" * 32,
        "entity_id": "sensor.garden_temperature",
        "state": "21.4",
        "attributes_json": serialized,
        "ingested_at_ns": 1_700_000_000_000_000_000,
        "last_changed_ns": 1_700_000_000_000_000_000,
        "last_updated_ns": 1_700_000_000_000_000_000,
        "context_id": "01J8Z0000000000000000000AA",
    }

    serialize = measure_phase(lambda: dumps(attributes), samples)
    build = measure_phase(lambda: event_module.EventEnvelope(**envelope_kwargs), samples)
    envelope = event_module.EventEnvelope(**envelope_kwargs)
    spool = measure_phase(envelope.to_spool_event, samples)

    totals = [a + b + c for a, b, c in zip(serialize, build, spool)]
    return {
        "target_attributes_bytes": target_bytes,
        "measured_attributes_bytes": len(serialized.encode("utf-8")),
        "serialize_us": {
            "p50": percentile(serialize, 0.5) * 1e6,
            "p95": percentile(serialize, 0.95) * 1e6,
            "p99": percentile(serialize, 0.99) * 1e6,
        },
        "envelope_us": {
            "p50": percentile(build, 0.5) * 1e6,
            "p95": percentile(build, 0.95) * 1e6,
            "p99": percentile(build, 0.99) * 1e6,
        },
        "spool_payload_us": {
            "p50": percentile(spool, 0.5) * 1e6,
            "p95": percentile(spool, 0.95) * 1e6,
            "p99": percentile(spool, 0.99) * 1e6,
        },
        "total_us": {
            "mean": statistics.fmean(totals) * 1e6,
            "p99": percentile(totals, 0.99) * 1e6,
            "max": max(totals) * 1e6,
        },
    }


def run(*, samples: int, repeats: int, warmup: int) -> dict[str, Any]:
    event_module = load_component()
    dumps, serializer_name, ha_dumps = serializer()

    ha_reference = None
    if ha_dumps is not None:
        attributes = attributes_of_size(PAYLOAD_SIZES[1])
        reference = measure_phase(lambda: ha_dumps(attributes), samples)
        ha_reference = {
            "p50_us": percentile(reference, 0.5) * 1e6,
            "p99_us": percentile(reference, 0.99) * 1e6,
        }

    # Warm-up: the first calls pay for imports, allocator growth and the caches
    # inside the serializer, so they are measured and thrown away.
    for size in PAYLOAD_SIZES:
        measure_size(event_module, dumps, size, warmup)

    measurements = []
    for repeat in range(repeats):
        for size in PAYLOAD_SIZES:
            result = measure_size(event_module, dumps, size, samples)
            result["repeat"] = repeat
            measurements.append(result)

    # Spread across repeats: a single repeat invites reading noise as a result,
    # so the report carries the min/median/max of the per-repeat percentiles.
    spread: dict[str, dict[str, dict[str, float]]] = {}
    for size in PAYLOAD_SIZES:
        rows = [m for m in measurements if m["target_attributes_bytes"] == size]
        for field, statistic in (
            ("serialize_us", "p50"),
            ("envelope_us", "p50"),
            ("envelope_us", "p99"),
            ("spool_payload_us", "p50"),
            ("total_us", "mean"),
        ):
            values = [row[field][statistic] for row in rows]
            spread.setdefault(str(size), {})[f"{field}_{statistic}"] = {
                "min": round(min(values), 2),
                "median": round(statistics.median(values), 2),
                "max": round(max(values), 2),
            }

    budget_median_ms = 1000.0 / EVENTS_PER_SECOND_MEDIAN
    budget_peak_ms = 1000.0 / EVENTS_PER_SECOND_PEAK
    worst = max(m["total_us"]["p99"] for m in measurements) / 1000.0

    return {
        "environment": {
            "python": platform.python_version(),
            "platform": f"{platform.system()}-{platform.machine()}",
            "serializer": serializer_name,
            "samples_per_measurement": samples,
            "warmup_samples": warmup,
            "repeats": repeats,
        },
        "production_reference": {
            "events_per_second_median": EVENTS_PER_SECOND_MEDIAN,
            "events_per_second_peak": EVENTS_PER_SECOND_PEAK,
            # The inverse of a rate is the interval between events, not the time
            # the loop has for one event: it is the yardstick a per-event cost is
            # compared against, nothing tighter.
            "inter_event_interval_at_median_ms": budget_median_ms,
            "inter_event_interval_at_peak_ms": budget_peak_ms,
        },
        "ha_json_dumps_reference": ha_reference,
        "measurements": measurements,
        "spread_across_repeats": spread,
        "verdict": {
            "worst_p99_ms": worst,
            "share_of_peak_interval_percent": worst / budget_peak_ms * 100,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=20_000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=2_000)
    args = parser.parse_args()

    result = run(samples=args.events, repeats=args.repeats, warmup=args.warmup)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Measure the idle-path cost of the writer service.

Answers three questions about a completely idle writer (no ingress, empty spool,
nothing to deliver):

1. how often does the service call `SQLiteSpool.stats()` per second,
2. how long do `stats()` and `snapshot()` take,
3. how much CPU does the worker thread burn while doing nothing.

The writer stack (worker/spool/event/ilp/transport/schema) imports no Home
Assistant module - only `__init__.py` does - so this runs on a bare Python
install. The service is started with a counting spool proxy and a transport that
raises when called, so a delivery during the measurement fails loudly instead of
polluting the numbers.

Usage:

    python -m benchmarks.idle_path
    python -m benchmarks.idle_path --idle-seconds 10 --repeats 3
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import platform
import resource
import statistics
import sys
import tempfile
import threading
import time
import types
from pathlib import Path
from typing import Any

COMPONENT = (
    Path(__file__).resolve().parent.parent
    / "custom_components"
    / "hass_questdb_writer"
)

_MEASURED_MODULES = ("const", "ilp", "event", "spool", "transport", "schema", "worker")


def load_component() -> tuple[Any, Any]:
    """Import the component's Home-Assistant-free modules under a synthetic package."""
    package = types.ModuleType("hqw")
    package.__path__ = [str(COMPONENT)]
    sys.modules["hqw"] = package
    for name in _MEASURED_MODULES:
        spec = importlib.util.spec_from_file_location(
            f"hqw.{name}", COMPONENT / f"{name}.py"
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[f"hqw.{name}"] = module
        spec.loader.exec_module(module)
    return sys.modules["hqw.spool"], sys.modules["hqw.worker"]


class CountingSpool:
    """Time every `stats()` call and remember which thread made it."""

    def __init__(
        self, delegate: Any, durations: list[float], lock: threading.Lock
    ) -> None:
        self._delegate = delegate
        self._durations = durations
        self._lock = lock

    def stats(self) -> Any:
        started = time.perf_counter()
        result = self._delegate.stats()
        elapsed = time.perf_counter() - started
        with self._lock:
            self._durations.append(elapsed)
        return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


class IdleTransport:
    """Fail loudly: an idle measurement must never deliver anything."""

    def send_batch(self, payload: bytes) -> None:
        raise AssertionError("idle run must not deliver a batch")

    def close(self) -> None:
        pass


def cpu_seconds() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * fraction))
    return ordered[index]


def measure_window(
    service: Any, durations: list[float], lock: threading.Lock, seconds: float
) -> dict[str, float]:
    with lock:
        durations.clear()
    cpu_before = cpu_seconds()
    started = time.perf_counter()
    time.sleep(seconds)
    elapsed = time.perf_counter() - started
    burned = cpu_seconds() - cpu_before
    with lock:
        samples = list(durations)
    return {
        "window_seconds": elapsed,
        "stats_calls": len(samples),
        "stats_calls_per_second": len(samples) / elapsed,
        "stats_mean_us": statistics.fmean(samples) * 1e6 if samples else 0.0,
        "stats_p99_us": percentile(samples, 0.99) * 1e6 if samples else 0.0,
        "cpu_seconds": burned,
        "cpu_percent_of_one_core": burned / elapsed * 100,
    }


def run(
    *,
    idle_seconds: float,
    repeats: int,
    snapshot_calls: int,
    flush_interval_seconds: float,
) -> dict[str, object]:
    spool_module, worker_module = load_component()
    SQLiteSpool = spool_module.SQLiteSpool
    settings_class = worker_module.WorkerSettings
    service_class = worker_module.WriterService

    durations: list[float] = []
    lock = threading.Lock()
    temporary = tempfile.TemporaryDirectory()
    path = Path(temporary.name) / "idle.db"

    def spool_factory() -> CountingSpool:
        # Called on the worker thread: SQLite connections are thread-bound.
        return CountingSpool(
            SQLiteSpool(
                path,
                max_pending_rows=100_000,
                max_pending_bytes=64 * 1024 * 1024,
                max_event_bytes=64 * 1024,
                max_dead_letter_rows=1_000,
                max_dead_letter_bytes=16 * 1024 * 1024,
                busy_timeout_seconds=1.0,
            ),
            durations,
            lock,
        )

    settings = settings_class(
        ingress_queue_capacity=1_000,
        max_serialized_event_bytes=64 * 1024,
        persist_batch_rows=100,
        delivery_batch_rows=1_000,
        delivery_batch_bytes=512 * 1024,
        flush_interval_seconds=flush_interval_seconds,
        retry_initial_seconds=1.0,
        retry_max_seconds=60.0,
        retry_multiplier=2.0,
        retry_jitter_ratio=0.2,
        flush_on_shutdown=False,
    )
    service = service_class(
        table="hass",
        settings=settings,
        spool_factory=spool_factory,
        transport_factory=IdleTransport,
    )
    service.start(timeout_seconds=5)

    snapshot_durations: list[float] = []
    for _ in range(snapshot_calls):
        started = time.perf_counter()
        service.snapshot()
        snapshot_durations.append(time.perf_counter() - started)

    windows = [
        measure_window(service, durations, lock, idle_seconds) for _ in range(repeats)
    ]
    service.stop(timeout_seconds=5)
    temporary.cleanup()

    return {
        "environment": {
            "python": platform.python_version(),
            "platform": f"{platform.system()}-{platform.machine()}",
            "flush_interval_seconds": flush_interval_seconds,
        },
        "idle_windows": windows,
        "snapshot": {
            "calls": snapshot_calls,
            "p50_us": percentile(snapshot_durations, 0.5) * 1e6,
            "p99_us": percentile(snapshot_durations, 0.99) * 1e6,
            "max_us": max(snapshot_durations) * 1e6,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--idle-seconds", type=float, default=5.0)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--snapshot-calls", type=int, default=20_000)
    parser.add_argument("--flush-interval-seconds", type=float, default=1.0)
    args = parser.parse_args()

    result = run(
        idle_seconds=args.idle_seconds,
        repeats=args.repeats,
        snapshot_calls=args.snapshot_calls,
        flush_interval_seconds=args.flush_interval_seconds,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

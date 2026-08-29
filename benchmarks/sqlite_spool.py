"""Measure durable SQLite enqueue cost for different transaction sizes."""

from __future__ import annotations

import argparse
from collections.abc import Iterator
from pathlib import Path
import tempfile
import time

from custom_components.hass_questdb_writer.spool import NewSpoolEvent, SQLiteSpool


def _event_batches(
    event_count: int, batch_size: int, payload_size: int
) -> Iterator[tuple[NewSpoolEvent, ...]]:
    payload = b"x" * payload_size
    for start in range(0, event_count, batch_size):
        yield tuple(
            NewSpoolEvent(f"event-{index}", payload, index)
            for index in range(start, min(start + batch_size, event_count))
        )


def _run_case(
    directory: Path,
    *,
    event_count: int,
    batch_size: int,
    payload_size: int,
    repeat: int,
) -> tuple[float, int]:
    path = directory / f"batch-{batch_size}-repeat-{repeat}.db"
    with SQLiteSpool(
        path,
        max_pending_rows=event_count,
        max_pending_bytes=event_count * payload_size,
        max_event_bytes=payload_size,
        max_dead_letter_rows=event_count,
        max_dead_letter_bytes=event_count * payload_size,
        busy_timeout_seconds=1,
    ) as spool:
        started = time.perf_counter()
        for batch in _event_batches(event_count, batch_size, payload_size):
            spool.enqueue_many(batch)
        elapsed = time.perf_counter() - started
        written = spool.stats().pending_rows
    return elapsed, written


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=5_000)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--batch-sizes", type=int, nargs="+", default=(1, 10, 100))
    parser.add_argument("--repeats", type=int, default=3)
    arguments = parser.parse_args()
    if arguments.events <= 0 or arguments.payload_bytes <= 0:
        parser.error("events and payload-bytes must be positive")
    if any(size <= 0 for size in arguments.batch_sizes):
        parser.error("batch sizes must be positive")
    if arguments.repeats <= 0:
        parser.error("repeats must be positive")

    with tempfile.TemporaryDirectory() as temporary_directory:
        directory = Path(temporary_directory)
        print("batch_size,repeat,events,elapsed_seconds,events_per_second")
        for batch_size in arguments.batch_sizes:
            for repeat in range(1, arguments.repeats + 1):
                elapsed, written = _run_case(
                    directory,
                    event_count=arguments.events,
                    batch_size=batch_size,
                    payload_size=arguments.payload_bytes,
                    repeat=repeat,
                )
                print(
                    f"{batch_size},{repeat},{written},{elapsed:.6f},"
                    f"{written / elapsed:.1f}"
                )


if __name__ == "__main__":
    main()

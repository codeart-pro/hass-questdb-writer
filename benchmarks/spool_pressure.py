#!/usr/bin/env python3
"""Measure what a QuestDB outage costs the filesystem, and what happens at the limit.

Reproduces the scenario ADR 0014 decides about, on a filesystem with a real and
small capacity: the writer keeps receiving events while delivery fails, the
durable spool grows, and the free-space guard is expected to stop persistence
before the disk - which on a real installation also holds the recorder, logs and
backups - is consumed.

The phases are the operator's view of the incident:

1. outage  - nothing listens on the writer's destination, so the transport fails
             with a real connection error while events keep arriving;
2. blocked - the guard stops persistence; the counters and the sample log show
             what the installation looks like from the outside;
3. recover - the destination comes back (a real TCP forwarder starts listening
             on the same address) and the spool must drain completely, with the
             filesystem returning to its previous occupancy.

What is measured per sample: pending bytes and rows, the size of the spool file
and its WAL, free space on the filesystem, and the counters the runtime
publishes to Home Assistant.

The listener path is not simulated - events are serialized by the same
`EventEnvelope`/`to_spool_event()` code the runtime uses, so the payload the
spool stores is the real one.

Usage:

    python -m benchmarks.spool_pressure --spool-dir /spool --questdb-host questdb
    python -m benchmarks.spool_pressure --event-rate 61 --payload-bytes 253
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import importlib.util
import json
from pathlib import Path
import platform
import resource
import select
import shutil
import socket
import sys
import threading
import time
import types
from typing import Any

COMPONENT = (
    Path(__file__).resolve().parent.parent
    / "custom_components"
    / "hass_questdb_writer"
)

_MEASURED_MODULES = (
    "const",
    "ilp",
    "event",
    "spool",
    "transport",
    "schema",
    "worker",
)


def load_component() -> dict[str, Any]:
    """Import the component's Home-Assistant-free modules under a synthetic package.

    Modules already pulled in by a relative import are reused instead of executed
    again: `worker` imports `event`, and re-executing a module would register a
    second module object whose classes no longer match the ones already captured.
    """
    package = types.ModuleType("hqw")
    package.__path__ = [str(COMPONENT)]
    sys.modules["hqw"] = package
    for name in _MEASURED_MODULES:
        if f"hqw.{name}" in sys.modules:
            continue
        spec = importlib.util.spec_from_file_location(
            f"hqw.{name}", COMPONENT / f"{name}.py"
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[f"hqw.{name}"] = module
        spec.loader.exec_module(module)
    return {name: sys.modules[f"hqw.{name}"] for name in _MEASURED_MODULES}


class QuestDbForwarder:
    """A TCP forwarder that is opened only when the outage is supposed to end.

    While it is closed nothing listens on the writer's destination, so the real
    `IlpHttpTransport` fails with a connection error. Opening it makes the very
    same transport object deliver into QuestDB again - no fake transport and no
    restart of the writer.
    """

    def __init__(
        self, listen_host: str, listen_port: int, target_host: str, target_port: int
    ) -> None:
        self._listen = (listen_host, listen_port)
        self._target = (target_host, target_port)
        self._server: socket.socket | None = None
        self._stopping = threading.Event()

    def open(self) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(self._listen)
        server.listen(16)
        self._server = server
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def close(self) -> None:
        self._stopping.set()
        if self._server is not None:
            with contextlib.suppress(OSError):
                self._server.close()

    def _accept_loop(self) -> None:
        assert self._server is not None
        while not self._stopping.is_set():
            try:
                client, _ = self._server.accept()
            except OSError:
                return
            threading.Thread(target=self._pump, args=(client,), daemon=True).start()

    def _pump(self, client: socket.socket) -> None:
        try:
            upstream = socket.create_connection(self._target, timeout=5)
        except OSError:
            client.close()
            return
        pair = (client, upstream)
        try:
            while not self._stopping.is_set():
                readable, _, _ = select.select(pair, (), (), 5)
                for source in readable:
                    data = source.recv(65_536)
                    if not data:
                        return
                    (upstream if source is client else client).sendall(data)
        except OSError:
            pass
        finally:
            for sock in pair:
                with contextlib.suppress(OSError):
                    sock.close()


class Sampler(threading.Thread):
    """Record the spool, the filesystem and the counters once per interval."""

    def __init__(
        self,
        *,
        service: Any,
        db_path: Path,
        spool_dir: Path,
        interval_seconds: float,
    ) -> None:
        super().__init__(daemon=True)
        self._service = service
        self._db_path = db_path
        self._wal_path = db_path.with_name(db_path.name + "-wal")
        self._spool_dir = spool_dir
        self._interval = interval_seconds
        self._stopping = threading.Event()
        self._started_at = time.monotonic()
        self.samples: list[dict[str, Any]] = []

    def stop(self) -> None:
        self._stopping.set()

    def run(self) -> None:
        while not self._stopping.is_set():
            self.samples.append(self._sample())
            self._stopping.wait(self._interval)

    def _sample(self) -> dict[str, Any]:
        snapshot = self._service.snapshot()
        usage = shutil.disk_usage(self._spool_dir)
        db_bytes = _size_of(self._db_path)
        wal_bytes = _size_of(self._wal_path)
        return {
            "t": round(time.monotonic() - self._started_at, 3),
            "state": snapshot.state.value,
            "pending_rows": snapshot.pending_rows,
            "pending_bytes": snapshot.pending_bytes,
            "db_bytes": db_bytes,
            "wal_bytes": wal_bytes,
            "disk_bytes": db_bytes + wal_bytes,
            "fs_free_bytes": usage.free,
            "submitted_events": snapshot.submitted_events,
            "persisted_events": snapshot.persisted_events,
            "delivered_events": snapshot.delivered_events,
            "retry_attempts": snapshot.retry_attempts,
            "dead_lettered_events": snapshot.dead_lettered_events,
            "overflowed_events": snapshot.overflowed_events,
            "block_reason": snapshot.block_reason or "",
            "storage_blocks": snapshot.storage_blocks,
            "storage_recoveries": snapshot.storage_recoveries,
            "cpu_seconds": cpu_seconds(),
        }


def cpu_seconds() -> float:
    """CPU time this process has burned, user plus system."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


def _size_of(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _reset_table(
    modules: dict[str, Any], args: argparse.Namespace, transport_kwargs: dict[str, Any]
) -> None:
    """Drop the verification table so the row count belongs to this run only."""
    transport = modules["transport"].IlpHttpTransport(
        args.questdb_host, args.questdb_port, **transport_kwargs
    )
    try:
        transport.exec_query(f"drop table if exists {args.table}")
    except Exception as exc:  # noqa: BLE001 - a missing table must not stop the run
        print(f"could not reset {args.table}: {type(exc).__name__}: {exc}")
    finally:
        transport.close()


def make_envelope(event_module: Any, index: int, payload_bytes: int) -> Any:
    """Build one event the way the listener does, at the requested payload size."""
    padding = max(0, payload_bytes - 150)
    attributes = {
        "friendly_name": "Garden temperature",
        "unit_of_measurement": "°C",
        "device_class": "temperature",
        "state_class": "measurement",
        "note": "x" * padding,
    }
    serialized = json.dumps(attributes, ensure_ascii=False)
    now = time.time_ns()
    return event_module.EventEnvelope(
        event_id=f"{index:032d}",
        entity_id="sensor.garden_temperature",
        state="21.4",
        attributes_json=serialized,
        ingested_at_ns=now,
        last_changed_ns=now,
        last_updated_ns=now,
        context_id=None,
    )


def run(args: argparse.Namespace) -> dict[str, Any]:
    modules = load_component()
    spool_dir = Path(args.spool_dir)
    db_path = spool_dir / args.db_name
    for leftover in (db_path, db_path.with_name(db_path.name + "-wal")):
        if leftover.exists():
            leftover.unlink()

    writes = threading.Lock()
    transport_kwargs = {
        "use_tls": False,
        "timeout_seconds": args.timeout_seconds,
    }

    def spool_factory() -> Any:
        # Called on the worker thread: SQLite connections are thread bound.
        with writes:
            return modules["spool"].SQLiteSpool(
                db_path,
                max_pending_rows=args.max_pending_rows,
                max_pending_bytes=args.max_pending_bytes,
                max_event_bytes=64 * 1024,
                max_dead_letter_rows=1_000,
                max_dead_letter_bytes=16 * 1024 * 1024,
                busy_timeout_seconds=args.timeout_seconds,
            )

    def transport_factory() -> Any:
        return modules["transport"].IlpHttpTransport(
            args.writer_host, args.writer_port, **transport_kwargs
        )

    settings = modules["worker"].WorkerSettings(
        ingress_queue_capacity=args.ingress_queue_capacity,
        max_serialized_event_bytes=64 * 1024,
        persist_batch_rows=args.persist_batch_rows,
        delivery_batch_rows=1_000,
        delivery_batch_bytes=512 * 1024,
        flush_interval_seconds=0.2,
        persist_idle_poll_seconds=0.25,
        retry_initial_seconds=0.5,
        retry_max_seconds=args.retry_max_seconds,
        retry_multiplier=2.0,
        retry_jitter_ratio=0.1,
        flush_on_shutdown=True,
        min_free_bytes=args.min_free_bytes,
        min_free_ratio=args.min_free_ratio,
    )
    service = modules["worker"].WriterService(
        table=args.table,
        settings=settings,
        spool_factory=spool_factory,
        transport_factory=transport_factory,
        schema_factory=None,
    )
    forwarder = QuestDbForwarder(
        args.writer_host,
        args.writer_port,
        args.questdb_host,
        args.questdb_port,
    )
    sampler = Sampler(
        service=service,
        db_path=db_path,
        spool_dir=spool_dir,
        interval_seconds=args.sample_seconds,
    )

    usage_before = shutil.disk_usage(spool_dir)
    _reset_table(modules, args, transport_kwargs)
    service.start(timeout_seconds=10)
    sampler.start()

    # Phase 1: delivery is impossible, events keep arriving.
    counters = {"attempted": 0, "submitted": 0, "overflowed": 0}

    def pump(baseline_time: float, baseline_attempted: int) -> None:
        """Submit every event the configured rate says is due by now.

        A real installation keeps firing events while its disk is full, so the
        blocked-state measurement below must keep submitting too.
        """
        due = int((time.monotonic() - baseline_time) * args.event_rate) - (
            counters["attempted"] - baseline_attempted
        )
        for _ in range(max(0, due)):
            envelope = make_envelope(
                modules["event"], counters["attempted"], args.payload_bytes
            )
            if service.submit(envelope):
                counters["submitted"] += 1
            else:
                counters["overflowed"] += 1
            counters["attempted"] += 1

    outage_started = time.monotonic()
    block_reason: str | None = None
    while True:
        pump(outage_started, 0)
        elapsed = time.monotonic() - outage_started
        snapshot = service.snapshot()
        if snapshot.block_reason is not None:
            block_reason = snapshot.block_reason
            break
        if counters["attempted"] >= args.max_events or elapsed >= args.max_seconds:
            break
        time.sleep(0.005)
    outage_seconds = time.monotonic() - outage_started
    blocked = service.snapshot()
    time.sleep(args.settle_seconds)

    # Phase 1b: the steady blocked state. This is what an installation sitting on
    # a full disk costs while it waits, with the destination still unreachable:
    # how often it re-checks, and how much CPU it burns doing that.
    blocked_started = time.monotonic()
    cpu_at_entry = cpu_seconds()
    blocks_at_entry = service.snapshot().storage_blocks
    blocked_entry_attempted = counters["attempted"]
    while time.monotonic() - blocked_started < args.blocked_seconds:
        pump(blocked_started, blocked_entry_attempted)
        time.sleep(0.005)
    blocked_seconds = time.monotonic() - blocked_started
    blocked = service.snapshot()
    cpu_while_blocked = cpu_seconds() - cpu_at_entry
    blocks_while_blocked = blocked.storage_blocks - blocks_at_entry

    # Phase 2: the destination comes back.
    recovery_started = time.monotonic()
    forwarder.open()
    drained = False
    deadline = recovery_started + args.drain_timeout_seconds
    while time.monotonic() < deadline:
        snapshot = service.snapshot()
        if (
            snapshot.pending_rows == 0
            and snapshot.delivered_events >= snapshot.persisted_events
        ):
            drained = True
            break
        time.sleep(0.2)
    recovery_seconds = time.monotonic() - recovery_started
    recovered = service.snapshot()

    transport = modules["transport"].IlpHttpTransport(
        args.writer_host, args.writer_port, **transport_kwargs
    )
    rows_in_questdb = None
    with contextlib.suppress(Exception):
        result = transport.exec_query(f"select count() from {args.table}")
        rows_in_questdb = int(result["dataset"][0][0])
    transport.close()

    stop_started = time.monotonic()
    stop_cpu_before = cpu_seconds()
    stopped_cleanly = service.stop(timeout_seconds=args.stop_timeout_seconds)
    shutdown_seconds = time.monotonic() - stop_started
    shutdown_cpu = cpu_seconds() - stop_cpu_before
    final = service.snapshot()
    sampler.stop()
    sampler.join(timeout=5)
    forwarder.close()
    samples = list(sampler.samples)

    final_db = _size_of(db_path)
    final_wal = _size_of(db_path.with_name(db_path.name + "-wal"))
    usage_after = shutil.disk_usage(spool_dir)
    peak = max(samples, key=lambda s: s["disk_bytes"]) if samples else None
    min_free = min((s["fs_free_bytes"] for s in samples), default=usage_after.free)

    ratio_bytes_per_pending_byte = None
    ratio_bytes_per_row = None
    if blocked.pending_bytes:
        ratio_bytes_per_pending_byte = round(
            (final_db + final_wal if peak is None else peak["disk_bytes"])
            / blocked.pending_bytes,
            4,
        )
    if blocked.pending_rows:
        ratio_bytes_per_row = round(
            (final_db + final_wal if peak is None else peak["disk_bytes"])
            / blocked.pending_rows,
            2,
        )

    samples_path = Path(args.samples_csv)
    if samples:
        with samples_path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(samples[0].keys()))
            writer.writeheader()
            writer.writerows(samples)

    nothing_lost = (
        recovered.pending_rows == 0
        and final.dead_lettered_events == 0
        and rows_in_questdb == final.persisted_events
    )
    return {
        "environment": {
            "python": platform.python_version(),
            "platform": f"{platform.system()}-{platform.machine()}",
            "spool_dir": str(spool_dir),
            "samples_csv": str(samples_path),
        },
        "parameters": {
            "event_rate_per_second": args.event_rate,
            "payload_bytes": args.payload_bytes,
            "max_pending_rows": args.max_pending_rows,
            "max_pending_bytes": args.max_pending_bytes,
            "min_free_bytes": args.min_free_bytes,
            "min_free_ratio": args.min_free_ratio,
            "ingress_queue_capacity": args.ingress_queue_capacity,
        },
        "filesystem": {
            "total_bytes": usage_before.total,
            "free_bytes_before": usage_before.free,
            "free_bytes_after": usage_after.free,
            "min_free_bytes_seen": min_free,
            "reserve_bytes": settings.min_free_bytes
            if usage_before.total * settings.min_free_ratio <= settings.min_free_bytes
            else int(usage_before.total * settings.min_free_ratio),
        },
        "outage": {
            "seconds": round(outage_seconds, 3),
            "attempted_events": counters["attempted"],
            "submitted_events": counters["submitted"],
            "overflowed_events_dropped_at_ingress": counters["overflowed"],
            "block_reason": block_reason,
            "state_at_block": blocked.state.value,
            "pending_rows_at_block": blocked.pending_rows,
            "pending_bytes_at_block": blocked.pending_bytes,
            "storage_blocks": blocked.storage_blocks,
            "retry_attempts": blocked.retry_attempts,
            "last_error": blocked.last_error,
            "samples_collected": len(samples),
        },
        "blocked_steady_state": {
            "seconds": round(blocked_seconds, 3),
            "storage_blocks_at_entry": blocks_at_entry,
            "storage_blocks_at_exit": blocked.storage_blocks,
            "storage_blocks_per_second": round(
                blocks_while_blocked / blocked_seconds, 1
            ),
            "cpu_seconds": round(cpu_while_blocked, 3),
            "cpu_percent_of_one_core": round(
                cpu_while_blocked / blocked_seconds * 100, 1
            ),
            "state": blocked.state.value,
        },
        "disk_cost": {
            "db_bytes_peak": peak["db_bytes"] if peak else None,
            "wal_bytes_peak": peak["wal_bytes"] if peak else None,
            "disk_bytes_peak": peak["disk_bytes"] if peak else None,
            "bytes_per_pending_byte": ratio_bytes_per_pending_byte,
            "bytes_per_pending_row": ratio_bytes_per_row,
        },
        "recovery": {
            "seconds_to_drain": round(recovery_seconds, 3),
            "drained": drained,
            "delivered_events": recovered.delivered_events,
            "dead_lettered_events": final.dead_lettered_events,
            "storage_recoveries": recovered.storage_recoveries,
            "rows_in_questdb": rows_in_questdb,
            "final_pending_rows": final.pending_rows,
            "final_db_bytes": final_db,
            "final_wal_bytes": final_wal,
            "space_returned_bytes": usage_after.free - usage_before.free,
            "accepted_but_never_persisted_events": counters["submitted"]
            - final.persisted_events,
            "dropped_at_ingress_events": counters["overflowed"],
        },
        "shutdown": {
            "seconds": round(shutdown_seconds, 3),
            "stopped_cleanly": stopped_cleanly,
            "cpu_seconds": round(shutdown_cpu, 3),
            "cpu_percent_of_one_core": round(
                shutdown_cpu / shutdown_seconds * 100, 1
            )
            if shutdown_seconds
            else None,
            "storage_blocks_total": final.storage_blocks,
        },
        "verdict": {
            "guard_blocked_persistence": block_reason is not None,
            "nothing_lost": nothing_lost,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spool-dir", default="/spool")
    parser.add_argument("--db-name", default="pressure.db")
    parser.add_argument("--writer-host", default="127.0.0.1")
    parser.add_argument("--writer-port", type=int, default=9000)
    parser.add_argument("--questdb-host", default="questdb")
    parser.add_argument("--questdb-port", type=int, default=9000)
    parser.add_argument("--table", default="hass_pressure_test")
    parser.add_argument("--event-rate", type=float, default=1_000.0)
    parser.add_argument("--payload-bytes", type=int, default=2_011)
    parser.add_argument("--max-events", type=int, default=2_000_000)
    parser.add_argument("--max-seconds", type=float, default=600.0)
    parser.add_argument("--settle-seconds", type=float, default=2.0)
    parser.add_argument("--blocked-seconds", type=float, default=10.0)
    parser.add_argument("--drain-timeout-seconds", type=float, default=300.0)
    parser.add_argument("--stop-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--sample-seconds", type=float, default=1.0)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--retry-max-seconds", type=float, default=2.0)
    parser.add_argument("--ingress-queue-capacity", type=int, default=1_000)
    parser.add_argument("--persist-batch-rows", type=int, default=100)
    parser.add_argument("--max-pending-rows", type=int, default=200_000)
    parser.add_argument("--max-pending-bytes", type=int, default=128 * 1024 * 1024)
    parser.add_argument("--min-free-bytes", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--min-free-ratio", type=float, default=0.25)
    parser.add_argument("--samples-csv", default="/tmp/spool_pressure_samples.csv")
    arguments = parser.parse_args()

    print(json.dumps(run(arguments), indent=2))


if __name__ == "__main__":
    main()

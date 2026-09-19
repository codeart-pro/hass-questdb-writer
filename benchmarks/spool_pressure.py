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
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import resource
import select
import shutil
import socket
import sqlite3
import subprocess
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

# Every n-th submitted event has its serialized spool payload measured, so the
# report can state the real size instead of the requested one.
_PAYLOAD_SAMPLE_EVERY = 200

# How many accepted ids are checked for actual presence at the destination. The
# enumeration of every id in one query returns a response the transport refuses,
# so membership is checked in chunks and aggregated on the server.
_IDS_PER_QUERY = 300


def load_component(component_dir: Path | None = None) -> dict[str, Any]:
    """Import the component's Home-Assistant-free modules under a synthetic package.

    Modules already pulled in by a relative import are reused instead of executed
    again: `worker` imports `event`, and re-executing a module would register a
    second module object whose classes no longer match the ones already captured.

    `component_dir` points the same harness at another revision of the package
    (a `git worktree` of the commit under comparison), which is what makes a
    before/after comparison a comparison of revisions rather than of conditions.
    """
    directory = component_dir or COMPONENT
    package = types.ModuleType("hqw")
    package.__path__ = [str(directory)]
    sys.modules["hqw"] = package
    for name in _MEASURED_MODULES:
        if f"hqw.{name}" in sys.modules:
            continue
        spec = importlib.util.spec_from_file_location(
            f"hqw.{name}", directory / f"{name}.py"
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
            "monotonic_seconds": round(time.monotonic(), 3),
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


def _digest(path: Path) -> str:
    """Short content hash of a file, so a result names the code that produced it."""
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    except OSError:
        return "unavailable"


def _git_state(directory: Path, scope: Path | None = None) -> dict[str, Any]:
    """Revision and dirty flag of a checkout, for the provenance of a run.

    With a `scope`, only that path counts as dirty: a result must not look
    uncommitted merely because a document outside the component was edited.
    """

    def git(*arguments: str) -> str:
        try:
            completed = subprocess.run(
                ["git", "-C", str(directory), *arguments],
                capture_output=True,
                text=True,
            )
        except OSError:
            return ""
        return completed.stdout.strip()

    status: tuple[str, ...] = ("status", "--porcelain")
    if scope is not None:
        status = (*status, "--", str(scope))
    return {
        "path": str(scope or directory),
        "revision": git("rev-parse", "HEAD") or None,
        "dirty": bool(git(*status)),
    }


def _state_accounting(
    samples: list[dict[str, Any]], *, since: float, until: float
) -> dict[str, dict[str, float]]:
    """Seconds and CPU seconds per writer state inside a monotonic time window.

    Each interval is charged to the state the writer was in when it ended, so a
    window the writer spent in delivery retry cannot be reported as the cost of a
    storage pause.
    """
    accounting: dict[str, dict[str, float]] = {}
    previous: dict[str, Any] | None = None
    for sample in samples:
        stamp = sample["monotonic_seconds"]
        if stamp < since or stamp > until:
            previous = None
            continue
        if previous is not None:
            entry = accounting.setdefault(
                sample["state"], {"seconds": 0.0, "cpu_seconds": 0.0}
            )
            entry["seconds"] += stamp - previous["monotonic_seconds"]
            entry["cpu_seconds"] += sample["cpu_seconds"] - previous["cpu_seconds"]
        previous = sample
    return {
        state: {
            "seconds": round(values["seconds"], 3),
            "cpu_seconds": round(values["cpu_seconds"], 3),
            "cpu_percent_of_one_core": round(
                values["cpu_seconds"] / values["seconds"] * 100, 1
            )
            if values["seconds"]
            else None,
        }
        for state, values in accounting.items()
    }


def _startup_probe_on_full_filesystem(
    modules: dict[str, Any], spool_dir: Path, args: argparse.Namespace
) -> dict[str, Any]:
    """Open a fresh spool while the filesystem has no space, and report what it says.

    The guard keeps the writer's own reserve, so the filesystem is not actually
    full until a filler file consumes the rest. The filler is removed in a
    `finally`, so the probe cannot change the run it is part of.
    """
    filler = spool_dir / "startup-filler.bin"
    filler_bytes = 0
    fill_error: str | None = None
    try:
        try:
            with filler.open("wb") as handle:
                while shutil.disk_usage(spool_dir).free > 64 * 1024:
                    handle.write(b"\0" * (256 * 1024))
                    filler_bytes += 256 * 1024
        except OSError as exc:
            # The filesystem refused the write: it is as full as it can get, and
            # that is the condition this probe exists for, not a failure of the
            # probe.
            fill_error = f"{type(exc).__name__}: {exc}"
        free_bytes = shutil.disk_usage(spool_dir).free
        probe_path = spool_dir / "startup-probe.db"
        try:
            spool = modules["spool"].SQLiteSpool(
                probe_path,
                max_pending_rows=args.max_pending_rows,
                max_pending_bytes=args.max_pending_bytes,
                max_event_bytes=64 * 1024,
                max_dead_letter_rows=1_000,
                max_dead_letter_bytes=16 * 1024 * 1024,
                busy_timeout_seconds=args.timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001 - the classification is the result
            return {
                "raised": type(exc).__name__,
                "message": str(exc)[:200],
                "filler_bytes": filler_bytes,
                "fill_error": fill_error,
                "free_bytes": free_bytes,
            }
        spool.close()
        return {
            "raised": None,
            "filler_bytes": filler_bytes,
            "fill_error": fill_error,
            "free_bytes": free_bytes,
            "note": "the spool opened on a filesystem this probe could not fill",
        }
    finally:
        with contextlib.suppress(OSError):
            filler.unlink()


def _reset_table(
    modules: dict[str, Any], args: argparse.Namespace, transport_kwargs: dict[str, Any]
) -> str | None:
    """Drop the verification table; returns an error text instead of raising.

    A failed reset invalidates the run: old rows would enter the row counts and
    the identity oracle, and a verdict computed over them is not a verdict about
    this run (reported as `reset_error`, which fails the final verdict).
    """
    transport = modules["transport"].IlpHttpTransport(
        args.questdb_host, args.questdb_port, **transport_kwargs
    )
    try:
        transport.exec_query(f"drop table if exists {args.table}")
    except Exception as exc:  # noqa: BLE001 - reported, not raised
        return f"{type(exc).__name__}: {exc}"
    finally:
        transport.close()
    return None


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
    component_dir = Path(args.component_dir) if args.component_dir else None
    modules = load_component(component_dir)
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
    reset_error = _reset_table(modules, args, transport_kwargs)
    service.start(timeout_seconds=10)
    sampler.start()

    # Phase 1: delivery is impossible, events keep arriving.
    counters = {"attempted": 0, "submitted": 0, "overflowed": 0}
    accepted_ids: set[str] = set()
    payload_sizes: list[int] = []

    def pump(baseline_time: float, baseline_attempted: int, rate: float) -> None:
        """Submit every event the configured rate says is due by now.

        A real installation keeps firing events while its disk is full, so the
        blocked-state measurement below keeps submitting too - optionally at a
        lower rate, which is how the production event rates are measured instead
        of extrapolated.
        """
        due = int((time.monotonic() - baseline_time) * rate) - (
            counters["attempted"] - baseline_attempted
        )
        for _ in range(max(0, due)):
            index = counters["attempted"]
            envelope = make_envelope(modules["event"], index, args.payload_bytes)
            if index % _PAYLOAD_SAMPLE_EVERY == 0:
                payload_sizes.append(len(envelope.to_spool_event().payload))
            if service.submit(envelope):
                counters["submitted"] += 1
                accepted_ids.add(f"{index:032d}")
            else:
                counters["overflowed"] += 1
            counters["attempted"] += 1

    outage_started = time.monotonic()
    block_reason: str | None = None
    while True:
        pump(outage_started, 0, args.event_rate)
        elapsed = time.monotonic() - outage_started
        snapshot = service.snapshot()
        if snapshot.block_reason is not None:
            block_reason = snapshot.block_reason
            break
        if counters["attempted"] >= args.max_events or elapsed >= args.max_seconds:
            break
        time.sleep(0.005)
    outage_seconds = time.monotonic() - outage_started
    # The block is reported from the moment it was detected, not from the end of
    # the window: the state after ten seconds of waiting says nothing about what
    # tripped the guard.
    blocked_at_detection = service.snapshot()
    time.sleep(args.settle_seconds)

    # Phase 1b: the steady blocked state. This is what an installation sitting on
    # a full disk costs while it waits, with the destination still unreachable:
    # how often it re-checks, and how much CPU it burns doing that.
    blocked_started = time.monotonic()
    cpu_at_entry = cpu_seconds()
    blocks_at_entry = service.snapshot().storage_blocks
    blocked_entry_attempted = counters["attempted"]
    while time.monotonic() - blocked_started < args.blocked_seconds:
        pump(blocked_started, blocked_entry_attempted, args.blocked_rate)
        time.sleep(0.005)
    blocked_seconds = time.monotonic() - blocked_started
    blocked = service.snapshot()
    cpu_while_blocked = cpu_seconds() - cpu_at_entry
    blocks_while_blocked = blocked.storage_blocks - blocks_at_entry

    # A spool that has to be created while the filesystem is genuinely out of
    # space: the startup classification this document claims, produced by real
    # storage instead of a replayed SQLite error. The filler is removed again
    # before the recovery phase, and the window above is already measured.
    startup_on_full_filesystem = _startup_probe_on_full_filesystem(
        modules, spool_dir, args
    )

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

    stop_started = time.monotonic()
    stop_cpu_before = cpu_seconds()
    stopped_cleanly = service.stop(timeout_seconds=args.stop_timeout_seconds)
    shutdown_seconds = time.monotonic() - stop_started
    shutdown_cpu = cpu_seconds() - stop_cpu_before
    final = service.snapshot()

    # Verification reads the destination and the spool directly instead of
    # trusting the writer's counters: identity is what has to survive, and a row
    # count cannot tell a re-delivered event from a lost one, because a resend of
    # another event can hold the total up while one event is missing. The verdict
    # below therefore compares distinct event ids, not row counts.
    verification_error: str | None = None
    rows_in_questdb: int | None = None
    distinct_delivered: int | None = None

    # The spool is read before the queries so the id sample can exclude the
    # events the writer is still holding: sampled ids are the ones that have to
    # be at the destination, so a missing sample is a loss and not a queued
    # event.
    spool_ids: set[str] | None = None
    try:
        connection = sqlite3.connect(db_path)
        try:
            spool_ids = {
                str(row[0])
                for row in connection.execute("select event_id from pending")
            }
        finally:
            connection.close()
    except sqlite3.Error as exc:
        verification_error = f"{type(exc).__name__}: {exc}"
    verifiable_ids = sorted(accepted_ids - (spool_ids or set()))
    missing_ids: list[str] | None = None
    transport = modules["transport"].IlpHttpTransport(
        args.questdb_host, args.questdb_port, **transport_kwargs
    )
    try:
        raw = transport.exec_query(f"select count() from {args.table}")
        rows_in_questdb = int(raw["dataset"][0][0])
        # Aggregates instead of enumerating every id: the destination returns a
        # bounded response, and a distinct count cannot be inflated by
        # re-delivery the way a row count can.
        distinct = transport.exec_query(
            f"select count_distinct(event_id) from {args.table}"
        )
        distinct_delivered = int(distinct["dataset"][0][0])
        # Membership of *every* verifiable id, in chunks: a single query with
        # 13,000 ids returns a response the transport refuses, but a chunked
        # membership check is the full set difference, not a sample of it.
        missing_ids = []
        for start in range(0, len(verifiable_ids), _IDS_PER_QUERY):
            chunk = verifiable_ids[start : start + _IDS_PER_QUERY]
            quoted = ", ".join(f"'{event_id}'" for event_id in chunk)
            present = transport.exec_query(
                f"select count_distinct(event_id) from {args.table} "
                f"where event_id in ({quoted})"
            )
            if int(present["dataset"][0][0]) == len(chunk):
                continue
            rows = transport.exec_query(
                f"select distinct event_id from {args.table} "
                f"where event_id in ({quoted})"
            )
            arrived = {str(row[0]) for row in rows["dataset"]}
            missing_ids.extend(event_id for event_id in chunk if event_id not in arrived)
    except Exception as exc:  # noqa: BLE001 - reported as a verification failure
        verification_error = f"{type(exc).__name__}: {exc}"
    finally:
        transport.close()

    sampler.stop()
    sampler.join(timeout=5)
    forwarder.close()
    samples = list(sampler.samples)

    final_db = _size_of(db_path)
    final_wal = _size_of(db_path.with_name(db_path.name + "-wal"))
    usage_after = shutil.disk_usage(spool_dir)
    peak = max(samples, key=lambda s: s["disk_bytes"]) if samples else None
    min_free = min((s["fs_free_bytes"] for s in samples), default=usage_after.free)
    # What the writer actually did during the window each measurement claims to
    # describe: state-seconds and CPU-seconds, not an assumption about the state.
    state_window = _state_accounting(
        samples, since=blocked_started, until=blocked_started + blocked_seconds
    )
    state_run = _state_accounting(samples, since=0.0, until=float("inf"))

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

    # The facts that have to hold for at-least-once: every accepted event is
    # delivered or still durable, no row is stored twice, and a sample of the
    # accepted ids is really present. A failed reset or a failed query makes the
    # whole verification invalid rather than silently optimistic.
    identity_counts_agree: bool | None = None
    unaccounted_events: int | None = None
    duplicate_rows_in_table: int | None = None
    if distinct_delivered is not None and spool_ids is not None:
        identity_counts_agree = (
            distinct_delivered + len(spool_ids) == final.persisted_events
        )
        unaccounted_events = (
            len(accepted_ids) - distinct_delivered - len(spool_ids)
        )
        if rows_in_questdb is not None:
            duplicate_rows_in_table = rows_in_questdb - distinct_delivered
    nothing_lost = (
        reset_error is None
        and verification_error is None
        and final.dead_lettered_events == 0
        and identity_counts_agree is True
        and unaccounted_events == 0
        and duplicate_rows_in_table == 0
        and missing_ids is not None
        and not missing_ids
    )
    return {
        "environment": {
            "python": platform.python_version(),
            "platform": f"{platform.system()}-{platform.machine()}",
            "spool_dir": str(spool_dir),
            "samples_csv": str(samples_path),
            # Provenance, so a baseline/fixed comparison is auditable from the
            # result file alone instead of relying on the reader's trust.
            "harness_sha256": _digest(Path(__file__)),
            "harness_revision": _git_state(
                Path(__file__).resolve().parent.parent, Path(__file__)
            ),
            "component_revision": _git_state(
                (component_dir or COMPONENT).resolve().parent.parent,
                (component_dir or COMPONENT),
            ),
            "arguments": vars(args),
        },
        "parameters": {
            "event_rate_per_second": args.event_rate,
            "blocked_rate_per_second": args.blocked_rate,
            "payload_bytes": args.payload_bytes,
            "component_dir": str(component_dir or COMPONENT),
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
            "state_at_block": blocked_at_detection.state.value,
            "pending_rows_at_block": blocked_at_detection.pending_rows,
            "pending_bytes_at_block": blocked_at_detection.pending_bytes,
            "storage_blocks": blocked_at_detection.storage_blocks,
            "retry_attempts": blocked_at_detection.retry_attempts,
            "last_error": blocked_at_detection.last_error,
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
            # The window is not necessarily a blocked window: at a low offered
            # rate the writer can spend it in delivery retry instead, and the CPU
            # of that state must not be reported as the cost of a pause.
            "state_at_window_end": blocked.state.value,
            "state_seconds": state_window,
            "blocked_seconds_in_window": round(
                state_window.get("blocked", {}).get("seconds", 0.0), 3
            ),
            "cpu_seconds_in_blocked_state": state_window.get("blocked", {}).get(
                "cpu_seconds"
            ),
        },
        "disk_cost": {
            "db_bytes_peak": peak["db_bytes"] if peak else None,
            "wal_bytes_peak": peak["wal_bytes"] if peak else None,
            "disk_bytes_peak": peak["disk_bytes"] if peak else None,
            "bytes_per_pending_byte": ratio_bytes_per_pending_byte,
            "bytes_per_pending_row": ratio_bytes_per_row,
        },
        "identity": {
            "distinct_events_in_questdb": distinct_delivered,
            "events_still_in_spool": None if spool_ids is None else len(spool_ids),
            "accepted_events": len(accepted_ids),
            "persisted_events": final.persisted_events,
            "unaccounted_events": unaccounted_events,
            "counts_agree": identity_counts_agree,
            "duplicate_rows_in_table": duplicate_rows_in_table,
            "raw_rows_in_questdb": rows_in_questdb,
            "verified_ids": len(verifiable_ids),
            "missing_events": None if missing_ids is None else len(missing_ids),
        },
        "writer_states": state_run,
        "startup_on_full_filesystem": startup_on_full_filesystem,
        "payload": {
            "requested_bytes": args.payload_bytes,
            "samples": len(payload_sizes),
            "mean_bytes": (
                round(sum(payload_sizes) / len(payload_sizes), 1)
                if payload_sizes
                else None
            ),
            "min_bytes": min(payload_sizes) if payload_sizes else None,
            "max_bytes": max(payload_sizes) if payload_sizes else None,
        },
        "verification": {
            "reset_error": reset_error,
            "verification_error": verification_error,
        },
        "recovery": {
            "seconds_to_drain": round(recovery_seconds, 3),
            # Two different moments, named so they cannot be confused: the drain
            # observed before the writer was asked to stop, and the state of the
            # spool afterwards, which the shutdown flush may still change.
            "drained_before_shutdown": drained,
            "drained_after_shutdown": spool_ids is not None and not spool_ids,
            "delivered_events": recovered.delivered_events,
            "dead_lettered_events": final.dead_lettered_events,
            "storage_recoveries": recovered.storage_recoveries,
            "rows_in_questdb": rows_in_questdb,
            "rows_still_in_spool": final.pending_rows,
            "rows_still_in_spool_after_shutdown": None
            if spool_ids is None
            else len(spool_ids),
            "final_persisted_events": final.persisted_events,
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
    parser.add_argument(
        "--component-dir",
        default=None,
        help="directory holding the package revision under test (default: this "
        "repository); point it at a worktree to compare revisions",
    )
    parser.add_argument("--event-rate", type=float, default=1_000.0)
    parser.add_argument(
        "--blocked-rate",
        type=float,
        default=None,
        help="events/s offered while the pause is measured (default: --event-rate); "
        "use the production rates to measure instead of extrapolate",
    )
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
    if arguments.blocked_rate is None:
        arguments.blocked_rate = arguments.event_rate

    print(json.dumps(run(arguments), indent=2))


if __name__ == "__main__":
    main()

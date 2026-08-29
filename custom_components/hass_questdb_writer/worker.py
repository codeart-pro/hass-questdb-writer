"""Single-threaded durable delivery state machine.

The worker owns one thread running an asyncio loop with two independent
coroutines: a persist loop (ingress queue -> SQLite) and a delivery loop
(spool -> transport). The blocking ILP/HTTP request runs in the loop's
default executor, so durable persistence never waits on network I/O.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import contextlib
from dataclasses import dataclass
from enum import Enum
import math
from queue import Empty, Full, Queue
import random
from threading import Event, Lock, Thread, current_thread
import time
from typing import Protocol

from .event import EventEnvelope, EventEnvelopeError
from .ilp import IlpEncodingError
from .spool import (
    DeadLetterFullError,
    NewSpoolEvent,
    SpoolFullError,
    SpoolRecord,
    SpoolStats,
)
from .transport import (
    AuthenticationIlpError,
    IlpTransportError,
    PermanentIlpError,
    RetryableIlpError,
)

_MAX_SQLITE_BATCH_BYTES = 2**63 - 1
_MAX_ERROR_TEXT = 4_096


class SpoolHandle(Protocol):
    """Operations used by the worker-owned spool."""

    def enqueue_many(self, events: tuple[NewSpoolEvent, ...]) -> int: ...

    def stats(self) -> SpoolStats: ...

    def peek_batch(
        self, *, max_rows: int, max_bytes: int
    ) -> tuple[SpoolRecord, ...]: ...

    def mark_delivered(self, sequences: tuple[int, ...]) -> int: ...

    def record_attempt(
        self,
        sequences: tuple[int, ...],
        *,
        last_error: str,
        delivery_uncertain: bool,
    ) -> int: ...

    def move_to_dead_letter(
        self,
        sequences: tuple[int, ...],
        *,
        last_error: str,
        failed_ns: int,
        delivery_uncertain: bool,
    ) -> int: ...

    def close(self) -> None: ...


class TransportHandle(Protocol):
    """Operations used by the worker-owned transport."""

    def send_batch(self, payload: bytes) -> None: ...

    def close(self) -> None: ...


class WorkerError(RuntimeError):
    """Base class for writer-service lifecycle failures."""


class WorkerStartError(WorkerError):
    """The worker could not initialize its owned resources."""


class WorkerStartTimeoutError(WorkerError):
    """The worker did not finish initialization before the deadline."""


class WorkerShutdownError(WorkerError):
    """The worker could not persist accepted ingress before shutdown."""


class WorkerConfigurationError(WorkerError):
    """A configured limit cannot accommodate a valid event."""


class WorkerState(str, Enum):
    """Externally observable lifecycle and delivery state."""

    NEW = "new"
    STARTING = "starting"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    BLOCKED = "blocked"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class WorkerSettings:
    """Explicit bounds and timings for one writer service."""

    ingress_queue_capacity: int
    max_serialized_event_bytes: int
    persist_batch_rows: int
    delivery_batch_rows: int
    delivery_batch_bytes: int
    flush_interval_seconds: float
    retry_initial_seconds: float
    retry_max_seconds: float
    retry_multiplier: float
    retry_jitter_ratio: float
    flush_on_shutdown: bool

    def __post_init__(self) -> None:
        for name in (
            "ingress_queue_capacity",
            "max_serialized_event_bytes",
            "persist_batch_rows",
            "delivery_batch_rows",
            "delivery_batch_bytes",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        for name in (
            "flush_interval_seconds",
            "retry_initial_seconds",
            "retry_max_seconds",
            "retry_multiplier",
            "retry_jitter_ratio",
        ):
            value = getattr(self, name)
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
            ):
                raise ValueError(f"{name} must be finite")
        if self.flush_interval_seconds <= 0:
            raise ValueError("flush_interval_seconds must be positive")
        if self.retry_initial_seconds <= 0:
            raise ValueError("retry_initial_seconds must be positive")
        if self.retry_max_seconds < self.retry_initial_seconds:
            raise ValueError(
                "retry_max_seconds must not be below retry_initial_seconds"
            )
        if self.retry_multiplier < 1:
            raise ValueError("retry_multiplier must be at least 1")
        if not 0 <= self.retry_jitter_ratio <= 1:
            raise ValueError("retry_jitter_ratio must be between 0 and 1")
        if not isinstance(self.flush_on_shutdown, bool):
            raise ValueError("flush_on_shutdown must be a boolean")


@dataclass(frozen=True, slots=True)
class WorkerSnapshot:
    """Thread-safe immutable diagnostics snapshot."""

    state: WorkerState
    thread_alive: bool
    accepting: bool
    ingress_queue_depth: int
    held_unpersisted: int
    ingress_high_watermark: int
    submitted_events: int
    persisted_events: int
    delivered_events: int
    retry_attempts: int
    dead_lettered_events: int
    overflowed_events: int
    oversized_events: int
    pending_rows: int
    pending_bytes: int
    dead_letter_rows: int
    dead_letter_bytes: int
    retry_delay_seconds: float | None
    last_error: str | None
    last_success_ns: int | None


@dataclass(frozen=True, slots=True)
class _DeliveryOutcome:
    kind: str
    retry_delay: float | None = None


class _Backoff:
    def __init__(
        self,
        settings: WorkerSettings,
        random_source: Callable[[], float],
    ) -> None:
        self._initial = settings.retry_initial_seconds
        self._maximum = settings.retry_max_seconds
        self._multiplier = settings.retry_multiplier
        self._jitter = settings.retry_jitter_ratio
        self._random = random_source
        self._base = self._initial

    def reset(self) -> None:
        self._base = self._initial

    def next_delay(self) -> float:
        unit = self._random()
        if not 0 <= unit <= 1:
            raise ValueError("random_source must return a value between 0 and 1")
        factor = 1 + self._jitter * (2 * unit - 1)
        delay = self._base * factor
        self._base = min(self._maximum, self._base * self._multiplier)
        return max(0.0, min(self._maximum, delay))


def _positive_seconds(name: str, value: float) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{name} must be positive and finite")
    return float(value)


def _error_text(error: BaseException) -> str:
    message = f"{type(error).__name__}: {error}".replace("\x00", "�")
    if len(message) > _MAX_ERROR_TEXT:
        return f"{message[:_MAX_ERROR_TEXT]}…"
    return message


class WriterService:
    """Own a bounded ingress queue and one durable delivery thread."""

    def __init__(
        self,
        *,
        table: str,
        settings: WorkerSettings,
        spool_factory: Callable[[], SpoolHandle],
        transport_factory: Callable[[], TransportHandle],
        monotonic: Callable[[], float] = time.monotonic,
        wall_time_ns: Callable[[], int] = time.time_ns,
        random_source: Callable[[], float] = random.random,
        thread_name: str = "hass-questdb-writer",
    ) -> None:
        if not isinstance(table, str) or not table or "\x00" in table:
            raise ValueError("table must be a non-empty string without NUL")
        if not isinstance(thread_name, str) or not thread_name:
            raise ValueError("thread_name must be a non-empty string")
        self._table = table
        self._settings = settings
        self._spool_factory = spool_factory
        self._transport_factory = transport_factory
        self._monotonic = monotonic
        self._wall_time_ns = wall_time_ns
        self._random_source = random_source
        self._thread_name = thread_name

        self._ingress: Queue[NewSpoolEvent] = Queue(
            maxsize=settings.ingress_queue_capacity
        )
        self._lock = Lock()
        self._ready = Event()
        self._stop_requested = Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_async = asyncio.Event()
        self._wake_persist = asyncio.Event()
        self._wake_deliver = asyncio.Event()
        self._thread: Thread | None = None
        self._shutdown_deadline: float | None = None
        self._failure: BaseException | None = None

        self._state = WorkerState.NEW
        self._accepting = False
        self._held_unpersisted = 0
        self._ingress_high_watermark = 0
        self._submitted_events = 0
        self._persisted_events = 0
        self._delivered_events = 0
        self._retry_attempts = 0
        self._dead_lettered_events = 0
        self._overflowed_events = 0
        self._oversized_events = 0
        self._spool_stats = SpoolStats(0, 0, 0, 0)
        self._retry_delay_seconds: float | None = None
        self._last_error: str | None = None
        self._last_success_ns: int | None = None

    def start(self, *, timeout_seconds: float) -> None:
        """Start the one-shot worker and wait for owned resources to open."""
        timeout = _positive_seconds("timeout_seconds", timeout_seconds)
        with self._lock:
            if self._state is not WorkerState.NEW:
                raise WorkerStartError("writer service is one-shot and already used")
            self._state = WorkerState.STARTING
            thread = Thread(
                target=self._run,
                name=self._thread_name,
                daemon=True,
            )
            self._thread = thread
            thread.start()

        if not self._ready.wait(timeout):
            with self._lock:
                self._accepting = False
                self._last_error = "worker initialization timed out"
            self._stop_requested.set()
            self._signal_loop(self._stop_async.set)
            raise WorkerStartTimeoutError("worker initialization timed out")

        with self._lock:
            if self._state is WorkerState.FAILED:
                failure = self._failure
                message = self._last_error or "worker initialization failed"
                raise WorkerStartError(message) from failure
            if self._state is not WorkerState.RUNNING:
                raise WorkerStartError(
                    f"worker entered unexpected state {self._state.value}"
                )

    def submit(self, event: EventEnvelope) -> bool:
        """Offer one event without blocking the caller."""
        if not isinstance(event, EventEnvelope):
            raise TypeError("event must be an EventEnvelope")
        spool_event = event.to_spool_event()
        if len(spool_event.payload) > self._settings.max_serialized_event_bytes:
            with self._lock:
                self._oversized_events += 1
                self._last_error = (
                    f"serialized event is {len(spool_event.payload)} bytes; "
                    f"limit is {self._settings.max_serialized_event_bytes}"
                )
            return False

        with self._lock:
            if not self._accepting:
                return False
            try:
                self._ingress.put_nowait(spool_event)
            except Full:
                self._overflowed_events += 1
                self._last_error = "ingress queue is full"
                return False
            self._submitted_events += 1
            self._ingress_high_watermark = max(
                self._ingress_high_watermark, self._ingress.qsize()
            )
        self._signal_loop(self._wake_persist.set)
        return True

    def stop(self, *, timeout_seconds: float) -> bool:
        """Stop accepting, persist accepted ingress, and join by a deadline."""
        timeout = _positive_seconds("timeout_seconds", timeout_seconds)
        thread = self._thread
        if thread is current_thread():
            raise RuntimeError("worker cannot join itself")
        with self._lock:
            self._accepting = False
            if self._state is WorkerState.NEW:
                self._state = WorkerState.STOPPED
                return True
            if thread is None or not thread.is_alive():
                return True
            self._state = WorkerState.STOPPING
            self._shutdown_deadline = self._monotonic() + timeout
        self._stop_requested.set()
        self._signal_loop(self._stop_async.set)
        thread.join(timeout)
        return not thread.is_alive()

    def snapshot(self) -> WorkerSnapshot:
        """Return diagnostics without touching worker-owned resources."""
        with self._lock:
            thread = self._thread
            stats = self._spool_stats
            return WorkerSnapshot(
                state=self._state,
                thread_alive=thread is not None and thread.is_alive(),
                accepting=self._accepting,
                ingress_queue_depth=self._ingress.qsize(),
                held_unpersisted=self._held_unpersisted,
                ingress_high_watermark=self._ingress_high_watermark,
                submitted_events=self._submitted_events,
                persisted_events=self._persisted_events,
                delivered_events=self._delivered_events,
                retry_attempts=self._retry_attempts,
                dead_lettered_events=self._dead_lettered_events,
                overflowed_events=self._overflowed_events,
                oversized_events=self._oversized_events,
                pending_rows=stats.pending_rows,
                pending_bytes=stats.pending_bytes,
                dead_letter_rows=stats.dead_letter_rows,
                dead_letter_bytes=stats.dead_letter_bytes,
                retry_delay_seconds=self._retry_delay_seconds,
                last_error=self._last_error,
                last_success_ns=self._last_success_ns,
            )

    def _set_spool_stats(self, stats: SpoolStats) -> None:
        with self._lock:
            self._spool_stats = stats

    def _set_held(self, count: int) -> None:
        with self._lock:
            self._held_unpersisted = count

    def _set_delivery_state(
        self,
        state: WorkerState,
        *,
        last_error: str | None,
        retry_delay: float | None,
    ) -> None:
        with self._lock:
            if self._state not in (WorkerState.STOPPING, WorkerState.FAILED):
                self._state = state
            self._last_error = last_error
            self._retry_delay_seconds = retry_delay

    def _run(self) -> None:
        spool: SpoolHandle | None = None
        transport: TransportHandle | None = None
        failed = False
        try:
            spool = self._spool_factory()
            transport = self._transport_factory()
            self._set_spool_stats(spool.stats())
            with self._lock:
                if self._stop_requested.is_set():
                    self._state = WorkerState.STOPPING
                    self._accepting = False
                else:
                    self._state = WorkerState.RUNNING
                    self._accepting = True
            self._ready.set()
            asyncio.run(self._async_main(spool, transport))
        except BaseException as exc:
            failed = True
            with self._lock:
                self._failure = exc
                self._accepting = False
                self._state = WorkerState.FAILED
                self._last_error = _error_text(exc)
        finally:
            self._ready.set()
            cleanup_error: BaseException | None = None
            for resource in (transport, spool):
                if resource is None:
                    continue
                try:
                    resource.close()
                except BaseException as exc:
                    if cleanup_error is None:
                        cleanup_error = exc
            with self._lock:
                self._accepting = False
                if cleanup_error is not None and not failed:
                    self._failure = cleanup_error
                    self._state = WorkerState.FAILED
                    self._last_error = _error_text(cleanup_error)
                elif not failed:
                    self._state = WorkerState.STOPPED

    def _drain_ingress(self, held: list[NewSpoolEvent]) -> None:
        while len(held) < self._settings.persist_batch_rows:
            try:
                held.append(self._ingress.get_nowait())
            except Empty:
                break
        self._set_held(len(held))

    def _persist_held(
        self, spool: SpoolHandle, held: list[NewSpoolEvent]
    ) -> bool:
        if not held:
            return True
        try:
            spool.enqueue_many(tuple(held))
        except SpoolFullError as exc:
            self._set_delivery_state(
                WorkerState.BLOCKED,
                last_error=_error_text(exc),
                retry_delay=None,
            )
            return False
        for _event in held:
            self._ingress.task_done()
        with self._lock:
            self._persisted_events += len(held)
        held.clear()
        self._set_held(0)
        self._set_spool_stats(spool.stats())
        return True

    def _signal_loop(self, action: Callable[[], None]) -> None:
        """Run an event setter on the worker loop from another thread."""
        loop = self._loop
        if loop is not None and loop.is_running():
            try:
                loop.call_soon_threadsafe(action)
            except RuntimeError:
                pass

    async def _wait_events(
        self, events: list[asyncio.Event], timeout: float
    ) -> None:
        """Wait for any event or the timeout without busy-spinning."""
        if timeout <= 0:
            await asyncio.sleep(0)
            return
        waiters = [asyncio.ensure_future(event.wait()) for event in events]
        try:
            await asyncio.wait(
                waiters,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=timeout,
            )
        finally:
            for waiter in waiters:
                waiter.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)

    async def _async_main(
        self, spool: SpoolHandle, transport: TransportHandle
    ) -> None:
        """Run the persist and delivery coroutines until stop is requested."""
        self._loop = asyncio.get_running_loop()
        if self._stop_requested.is_set():
            self._stop_async.set()
        persist_task = asyncio.create_task(self._persist_loop(spool))
        deliver_task = asyncio.create_task(
            self._delivery_loop(spool, transport)
        )
        stop_waiter = asyncio.create_task(self._stop_async.wait())
        done, _ = await asyncio.wait(
            {stop_waiter, persist_task, deliver_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_waiter not in done:
            # A worker task ended before stop was requested: surface its failure.
            for task in (persist_task, deliver_task):
                if task in done:
                    task.result()
            raise WorkerError("worker task exited before stop was requested")

        with self._lock:
            self._state = WorkerState.STOPPING
            self._accepting = False
        await persist_task
        if self._settings.flush_on_shutdown:
            stats = spool.stats()
            if stats.pending_rows:
                backoff = _Backoff(self._settings, self._random_source)
                await self._deliver_once(
                    spool, transport, isolate_next=False, backoff=backoff
                )
        deliver_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await deliver_task
        stop_waiter.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stop_waiter

    async def _persist_loop(self, spool: SpoolHandle) -> None:
        """Persist accepted ingress while delivery proceeds independently."""
        held: list[NewSpoolEvent] = []
        while True:
            self._drain_ingress(held)
            if held:
                self._persist_held(spool, held)
            stats = spool.stats()
            self._set_spool_stats(stats)
            self._wake_deliver.set()
            stopping = self._stop_async.is_set()
            if stopping:
                if not held and self._ingress.empty():
                    return
                deadline = self._shutdown_deadline
                if deadline is not None and self._monotonic() >= deadline:
                    raise WorkerShutdownError(
                        f"shutdown left {len(held) + self._ingress.qsize()} "
                        "accepted events unpersisted"
                    )
                await asyncio.sleep(0)
                continue
            self._wake_persist.clear()
            await self._wait_events(
                [self._wake_persist, self._stop_async], 0.05
            )

    async def _delivery_loop(
        self, spool: SpoolHandle, transport: TransportHandle
    ) -> None:
        """Deliver pending spool rows without blocking the persist path."""
        pending_since: float | None = None
        retry_at: float | None = None
        blocked = False
        isolate_next = False
        backoff = _Backoff(self._settings, self._random_source)
        while True:
            if self._stop_async.is_set():
                return
            now = self._monotonic()
            stats = spool.stats()
            self._set_spool_stats(stats)
            if stats.pending_rows and pending_since is None:
                pending_since = now
            if not stats.pending_rows:
                pending_since = None
            retry_ready = retry_at is None or now >= retry_at
            flush_due = (
                stats.pending_rows >= self._settings.delivery_batch_rows
                or stats.pending_bytes >= self._settings.delivery_batch_bytes
                or (
                    pending_since is not None
                    and now - pending_since
                    >= self._settings.flush_interval_seconds
                )
            )
            pressure_due = (
                stats.pending_rows > 0
                and self._ingress.empty()
                and self._held_unpersisted == 0
            )
            delivery_due = (
                stats.pending_rows > 0
                and not blocked
                and retry_ready
                and (flush_due or pressure_due)
            )

            if delivery_due:
                outcome = await self._deliver_once(
                    spool,
                    transport,
                    isolate_next=isolate_next,
                    backoff=backoff,
                )
                stats = spool.stats()
                self._set_spool_stats(stats)
                if outcome.kind == "retry":
                    retry_at = self._monotonic() + (outcome.retry_delay or 0)
                elif outcome.kind == "blocked":
                    blocked = True
                    retry_at = None
                else:
                    retry_at = None
                isolate_next = outcome.kind == "split"
                if outcome.kind in ("success", "dead_letter"):
                    pending_since = (
                        self._monotonic()
                        - self._settings.flush_interval_seconds
                        if stats.pending_rows
                        else None
                    )
                continue

            wait_until: list[float] = [
                now + self._settings.flush_interval_seconds
            ]
            if pending_since is not None:
                flush_at = pending_since + self._settings.flush_interval_seconds
                if flush_at > now:
                    wait_until.append(flush_at)
            if retry_at is not None and retry_at > now:
                wait_until.append(retry_at)
            delay = max(0.0, min(wait_until) - self._monotonic())
            self._wake_deliver.clear()
            await self._wait_events(
                [self._wake_deliver, self._stop_async], delay
            )

    def _build_batch(
        self,
        spool: SpoolHandle,
        *,
        isolate_next: bool,
    ) -> tuple[bytes, tuple[int, ...]] | None:
        max_rows = 1 if isolate_next else self._settings.delivery_batch_rows
        records = spool.peek_batch(
            max_rows=max_rows,
            max_bytes=_MAX_SQLITE_BATCH_BYTES,
        )
        if not records:
            return None

        payload_parts: list[bytes] = []
        sequences: list[int] = []
        payload_bytes = 0
        for record in records:
            try:
                encoded = EventEnvelope.from_bytes(record.payload).to_ilp(
                    self._table
                )
            except (EventEnvelopeError, IlpEncodingError) as exc:
                if payload_parts:
                    break
                error = _error_text(exc)
                try:
                    spool.move_to_dead_letter(
                        (record.sequence,),
                        last_error=error,
                        failed_ns=self._wall_time_ns(),
                        delivery_uncertain=False,
                    )
                except DeadLetterFullError as dead_letter_error:
                    self._set_delivery_state(
                        WorkerState.BLOCKED,
                        last_error=_error_text(dead_letter_error),
                        retry_delay=None,
                    )
                    return None
                with self._lock:
                    self._dead_lettered_events += 1
                self._set_delivery_state(
                    WorkerState.RUNNING,
                    last_error=error,
                    retry_delay=None,
                )
                return b"", ()

            if payload_bytes + len(encoded) > self._settings.delivery_batch_bytes:
                if not payload_parts:
                    raise WorkerConfigurationError(
                        f"encoded event is {len(encoded)} bytes; delivery batch "
                        f"limit is {self._settings.delivery_batch_bytes}"
                    )
                break
            payload_parts.append(encoded)
            sequences.append(record.sequence)
            payload_bytes += len(encoded)
        return b"".join(payload_parts), tuple(sequences)

    async def _deliver_once(
        self,
        spool: SpoolHandle,
        transport: TransportHandle,
        *,
        isolate_next: bool,
        backoff: _Backoff,
    ) -> _DeliveryOutcome:
        built = self._build_batch(spool, isolate_next=isolate_next)
        if built is None:
            return _DeliveryOutcome("blocked")
        payload, sequences = built
        if not sequences:
            return _DeliveryOutcome("dead_letter")

        try:
            await asyncio.to_thread(transport.send_batch, payload)
        except AuthenticationIlpError as exc:
            self._set_delivery_state(
                WorkerState.BLOCKED,
                last_error=_error_text(exc),
                retry_delay=None,
            )
            return _DeliveryOutcome("blocked")
        except RetryableIlpError as exc:
            error = _error_text(exc)
            spool.record_attempt(
                sequences,
                last_error=error,
                delivery_uncertain=exc.delivery_uncertain,
            )
            delay = backoff.next_delay()
            with self._lock:
                self._retry_attempts += 1
            self._set_delivery_state(
                WorkerState.RETRY_WAIT,
                last_error=error,
                retry_delay=delay,
            )
            return _DeliveryOutcome("retry", retry_delay=delay)
        except PermanentIlpError as exc:
            error = _error_text(exc)
            backoff.reset()
            if exc.status_code == 400 and len(sequences) > 1:
                spool.record_attempt(
                    sequences,
                    last_error=error,
                    delivery_uncertain=exc.delivery_uncertain,
                )
                self._set_delivery_state(
                    WorkerState.RUNNING,
                    last_error=error,
                    retry_delay=None,
                )
                return _DeliveryOutcome("split")
            if exc.status_code == 400:
                try:
                    spool.move_to_dead_letter(
                        sequences,
                        last_error=error,
                        failed_ns=self._wall_time_ns(),
                        delivery_uncertain=exc.delivery_uncertain,
                    )
                except DeadLetterFullError as dead_letter_error:
                    self._set_delivery_state(
                        WorkerState.BLOCKED,
                        last_error=_error_text(dead_letter_error),
                        retry_delay=None,
                    )
                    return _DeliveryOutcome("blocked")
                with self._lock:
                    self._dead_lettered_events += len(sequences)
                self._set_delivery_state(
                    WorkerState.RUNNING,
                    last_error=error,
                    retry_delay=None,
                )
                return _DeliveryOutcome("dead_letter")
            self._set_delivery_state(
                WorkerState.BLOCKED,
                last_error=error,
                retry_delay=None,
            )
            return _DeliveryOutcome("blocked")
        except IlpTransportError:
            raise

        spool.mark_delivered(sequences)
        backoff.reset()
        with self._lock:
            self._delivered_events += len(sequences)
            self._last_success_ns = self._wall_time_ns()
        self._set_delivery_state(
            WorkerState.RUNNING,
            last_error=None,
            retry_delay=None,
        )
        return _DeliveryOutcome("success")

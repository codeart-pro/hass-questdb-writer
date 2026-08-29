"""Tests for the durable writer-service state machine."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from threading import Event, Lock
import tempfile
import time
import unittest

from custom_components.hass_questdb_writer.event import EventEnvelope
from custom_components.hass_questdb_writer.spool import (
    NewSpoolEvent,
    SQLiteSpool,
)
from custom_components.hass_questdb_writer.transport import (
    AuthenticationIlpError,
    PermanentIlpError,
    RetryableIlpError,
)
from custom_components.hass_questdb_writer.worker import (
    WorkerSettings,
    WorkerStartError,
    WorkerStartTimeoutError,
    WorkerState,
    WriterService,
)


class ScriptedTransport:
    def __init__(self, outcomes: list[BaseException | None] | None = None) -> None:
        self._outcomes = list(outcomes or [])
        self._lock = Lock()
        self.payloads: list[bytes] = []
        self.send_times: list[float] = []
        self.closed = False

    def send_batch(self, payload: bytes) -> None:
        with self._lock:
            self.payloads.append(payload)
            self.send_times.append(time.monotonic())
            outcome = self._outcomes.pop(0) if self._outcomes else None
        if outcome is not None:
            raise outcome

    def close(self) -> None:
        self.closed = True


class BlockingSpool:
    def __init__(
        self,
        delegate: SQLiteSpool,
        enqueue_entered: Event,
        enqueue_release: Event,
    ) -> None:
        self.delegate = delegate
        self.enqueue_entered = enqueue_entered
        self.enqueue_release = enqueue_release

    def enqueue_many(self, events: tuple[NewSpoolEvent, ...]) -> int:
        self.enqueue_entered.set()
        if not self.enqueue_release.wait(2):
            raise TimeoutError("test did not release enqueue")
        return self.delegate.enqueue_many(events)

    def __getattr__(self, name: str) -> object:
        return getattr(self.delegate, name)


class BlockingTransport(ScriptedTransport):
    def __init__(self) -> None:
        super().__init__()
        self.send_entered = Event()
        self.send_release = Event()

    def send_batch(self, payload: bytes) -> None:
        self.send_entered.set()
        if not self.send_release.wait(2):
            raise TimeoutError("test did not release transport")
        super().send_batch(payload)


class WriterServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.path = Path(self.temporary_directory.name) / "worker.db"
        self.services: list[WriterService] = []
        self.addCleanup(self.stop_services)

    def stop_services(self) -> None:
        for service in self.services:
            if service.snapshot().thread_alive:
                service.stop(timeout_seconds=1)

    def settings(self, **overrides: object) -> WorkerSettings:
        values: dict[str, object] = {
            "ingress_queue_capacity": 20,
            "max_serialized_event_bytes": 4_096,
            "persist_batch_rows": 20,
            "delivery_batch_rows": 3,
            "delivery_batch_bytes": 64_000,
            "flush_interval_seconds": 0.02,
            "retry_initial_seconds": 0.05,
            "retry_max_seconds": 0.1,
            "retry_multiplier": 2,
            "retry_jitter_ratio": 0,
            "flush_on_shutdown": False,
        }
        values.update(overrides)
        return WorkerSettings(**values)

    def open_spool(self) -> SQLiteSpool:
        return SQLiteSpool(
            self.path,
            max_pending_rows=100,
            max_pending_bytes=1_000_000,
            max_event_bytes=10_000,
            max_dead_letter_rows=100,
            max_dead_letter_bytes=1_000_000,
            busy_timeout_seconds=0.25,
        )

    def event(self, index: int) -> EventEnvelope:
        timestamp = 1_700_000_000_000_000_000 + index * 1_000
        return EventEnvelope(
            event_id=f"event-{index}",
            entity_id=f"sensor.test_{index}",
            state=str(index),
            attributes_json="{}",
            timestamp_ns=timestamp,
            ingested_at_ns=timestamp + 100,
            last_changed_ns=timestamp,
            last_updated_ns=timestamp,
            context_id=None,
        )

    def service(
        self,
        transport: ScriptedTransport,
        *,
        settings: WorkerSettings | None = None,
        spool_factory: Callable[[], object] | None = None,
    ) -> WriterService:
        service = WriterService(
            table="ha_events",
            settings=settings or self.settings(),
            spool_factory=spool_factory or self.open_spool,
            transport_factory=lambda: transport,
            random_source=lambda: 0.5,
        )
        self.services.append(service)
        return service

    def wait_for(
        self,
        condition: Callable[[], bool],
        *,
        timeout: float = 2,
    ) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return
            time.sleep(0.005)
        self.fail("condition was not reached before timeout")

    def test_delivers_a_threshold_batch_and_updates_health(self) -> None:
        transport = ScriptedTransport()
        service = self.service(transport)
        service.start(timeout_seconds=1)
        self.assertTrue(service.submit(self.event(1)))
        self.assertTrue(service.submit(self.event(2)))
        self.assertTrue(service.submit(self.event(3)))

        self.wait_for(lambda: service.snapshot().delivered_events == 3)
        snapshot = service.snapshot()
        self.assertEqual(snapshot.state, WorkerState.RUNNING)
        self.assertEqual(snapshot.persisted_events, 3)
        self.assertEqual(snapshot.pending_rows, 0)
        self.assertEqual(len(transport.payloads), 1)
        self.assertEqual(transport.payloads[0].count(b"\n"), 3)

    def test_retryable_failure_recovers_without_terminating_worker(self) -> None:
        retry = RetryableIlpError(
            "temporary",
            retryable=True,
            delivery_uncertain=True,
        )
        transport = ScriptedTransport([retry, None])
        service = self.service(
            transport,
            settings=self.settings(
                delivery_batch_rows=1,
                retry_initial_seconds=0.01,
            ),
        )
        service.start(timeout_seconds=1)
        service.submit(self.event(1))

        self.wait_for(lambda: service.snapshot().delivered_events == 1)
        snapshot = service.snapshot()
        self.assertTrue(snapshot.thread_alive)
        self.assertEqual(snapshot.retry_attempts, 1)
        self.assertEqual(snapshot.pending_rows, 0)
        self.assertIsNone(snapshot.last_error)
        self.assertEqual(len(transport.payloads), 2)

    def test_persists_new_ingress_while_waiting_for_retry(self) -> None:
        retry = RetryableIlpError(
            "QuestDB unavailable",
            retryable=True,
            delivery_uncertain=True,
        )
        transport = ScriptedTransport([retry])
        service = self.service(
            transport,
            settings=self.settings(
                delivery_batch_rows=1,
                retry_initial_seconds=1,
                retry_max_seconds=1,
            ),
        )
        service.start(timeout_seconds=1)
        service.submit(self.event(1))
        self.wait_for(lambda: service.snapshot().state is WorkerState.RETRY_WAIT)
        service.submit(self.event(2))
        self.wait_for(lambda: service.snapshot().pending_rows == 2)

        self.assertTrue(service.stop(timeout_seconds=1))
        with self.open_spool() as spool:
            records = spool.peek_batch(max_rows=10, max_bytes=1_000_000)
            self.assertEqual(
                [record.event_id for record in records], ["event-1", "event-2"]
            )
            self.assertEqual(records[0].attempt_count, 1)
            self.assertTrue(records[0].delivery_uncertain)
            self.assertEqual(records[1].attempt_count, 0)

    def test_http_400_isolates_only_the_bad_row(self) -> None:
        def bad_row() -> PermanentIlpError:
            return PermanentIlpError(
                "schema mismatch",
                retryable=False,
                delivery_uncertain=False,
                status_code=400,
            )

        transport = ScriptedTransport(
            [bad_row(), None, bad_row(), bad_row(), None]
        )
        service = self.service(transport)
        service.start(timeout_seconds=1)
        for index in (1, 2, 3):
            service.submit(self.event(index))

        self.wait_for(
            lambda: service.snapshot().delivered_events == 2
            and service.snapshot().dead_lettered_events == 1
        )
        snapshot = service.snapshot()
        self.assertEqual(snapshot.pending_rows, 0)
        self.assertEqual(snapshot.dead_letter_rows, 1)
        self.assertTrue(service.stop(timeout_seconds=1))
        with self.open_spool() as spool:
            dead = spool.peek_dead_letters(limit=10)
            self.assertEqual([record.event_id for record in dead], ["event-2"])
            self.assertEqual(dead[0].attempt_count, 3)

    def test_authentication_error_blocks_delivery_but_keeps_spooling(self) -> None:
        authentication = AuthenticationIlpError(
            "unauthorized",
            retryable=False,
            delivery_uncertain=False,
            status_code=401,
        )
        transport = ScriptedTransport([authentication])
        service = self.service(
            transport,
            settings=self.settings(delivery_batch_rows=1),
        )
        service.start(timeout_seconds=1)
        service.submit(self.event(1))
        self.wait_for(lambda: service.snapshot().state is WorkerState.BLOCKED)
        service.submit(self.event(2))
        self.wait_for(lambda: service.snapshot().pending_rows == 2)
        time.sleep(0.05)
        self.assertEqual(len(transport.payloads), 1)

        self.assertTrue(service.stop(timeout_seconds=1))
        with self.open_spool() as spool:
            self.assertEqual(spool.stats().pending_rows, 2)

    def test_invalid_stored_payload_moves_to_dead_letter(self) -> None:
        with self.open_spool() as spool:
            spool.enqueue_many((NewSpoolEvent("bad-event", b"not-json", 1),))
        transport = ScriptedTransport()
        service = self.service(transport)
        service.start(timeout_seconds=1)

        self.wait_for(lambda: service.snapshot().dead_lettered_events == 1)
        snapshot = service.snapshot()
        self.assertEqual(snapshot.pending_rows, 0)
        self.assertEqual(snapshot.dead_letter_rows, 1)
        self.assertEqual(transport.payloads, [])

    def test_rejects_oversized_and_queue_overflow_without_blocking(self) -> None:
        transport = ScriptedTransport()
        enqueue_entered = Event()
        enqueue_release = Event()

        def blocking_spool_factory() -> BlockingSpool:
            return BlockingSpool(
                self.open_spool(), enqueue_entered, enqueue_release
            )

        service = self.service(
            transport,
            settings=self.settings(
                ingress_queue_capacity=1,
                persist_batch_rows=1,
                max_serialized_event_bytes=500,
                flush_interval_seconds=1,
            ),
            spool_factory=blocking_spool_factory,
        )
        service.start(timeout_seconds=1)
        self.assertTrue(service.submit(self.event(1)))
        self.assertTrue(enqueue_entered.wait(1))
        self.assertTrue(service.submit(self.event(2)))
        self.assertFalse(service.submit(self.event(3)))
        oversized = replace(
            self.event(4),
            attributes_json='{"large":"' + "x" * 1_000 + '"}',
        )
        self.assertFalse(service.submit(oversized))
        snapshot = service.snapshot()
        self.assertEqual(snapshot.overflowed_events, 1)
        self.assertEqual(snapshot.oversized_events, 1)
        enqueue_release.set()
        self.wait_for(lambda: service.snapshot().persisted_events == 2)

    def test_shutdown_persists_queue_without_remote_flush(self) -> None:
        transport = ScriptedTransport()
        service = self.service(
            transport,
            settings=self.settings(
                delivery_batch_rows=20,
                flush_interval_seconds=10,
            ),
        )
        service.start(timeout_seconds=1)
        for index in range(1, 6):
            self.assertTrue(service.submit(self.event(index)))
        self.assertTrue(service.stop(timeout_seconds=1))
        self.assertEqual(service.snapshot().state, WorkerState.STOPPED)
        self.assertEqual(transport.payloads, [])
        with self.open_spool() as spool:
            self.assertEqual(spool.stats().pending_rows, 5)

    def test_shutdown_can_attempt_one_remote_flush(self) -> None:
        transport = ScriptedTransport()
        service = self.service(
            transport,
            settings=self.settings(
                flush_interval_seconds=10,
                flush_on_shutdown=True,
            ),
        )
        service.start(timeout_seconds=1)
        service.submit(self.event(1))
        self.assertTrue(service.stop(timeout_seconds=1))
        self.assertEqual(service.snapshot().delivered_events, 1)
        self.assertEqual(len(transport.payloads), 1)

    def test_startup_failure_is_visible(self) -> None:
        transport = ScriptedTransport()

        def fail_spool() -> object:
            raise OSError("storage unavailable")

        service = self.service(transport, spool_factory=fail_spool)
        with self.assertRaises(WorkerStartError):
            service.start(timeout_seconds=1)
        snapshot = service.snapshot()
        self.assertEqual(snapshot.state, WorkerState.FAILED)
        self.assertFalse(snapshot.thread_alive)
        self.assertIn("storage unavailable", snapshot.last_error or "")

    def test_start_timeout_never_enables_event_acceptance(self) -> None:
        transport = ScriptedTransport()
        factory_entered = Event()
        factory_release = Event()

        def slow_spool() -> SQLiteSpool:
            factory_entered.set()
            if not factory_release.wait(2):
                raise TimeoutError("test did not release spool factory")
            return self.open_spool()

        service = self.service(transport, spool_factory=slow_spool)
        with self.assertRaises(WorkerStartTimeoutError):
            service.start(timeout_seconds=0.01)
        self.assertTrue(factory_entered.is_set())
        self.assertFalse(service.submit(self.event(1)))
        factory_release.set()
        self.wait_for(lambda: not service.snapshot().thread_alive)
        self.assertEqual(service.snapshot().state, WorkerState.STOPPED)

    def test_stop_timeout_reports_a_still_blocked_transport(self) -> None:
        transport = BlockingTransport()
        service = self.service(
            transport,
            settings=self.settings(delivery_batch_rows=1),
        )
        service.start(timeout_seconds=1)
        self.assertTrue(service.submit(self.event(1)))
        self.assertTrue(transport.send_entered.wait(1))

        self.assertFalse(service.stop(timeout_seconds=0.01))
        snapshot = service.snapshot()
        self.assertEqual(snapshot.state, WorkerState.STOPPING)
        self.assertTrue(snapshot.thread_alive)
        transport.send_release.set()
        self.wait_for(lambda: not service.snapshot().thread_alive)
        self.assertEqual(service.snapshot().state, WorkerState.STOPPED)

    def test_unexpected_transport_failure_leaves_row_pending(self) -> None:
        transport = ScriptedTransport([RuntimeError("unexpected")])
        service = self.service(
            transport,
            settings=self.settings(delivery_batch_rows=1),
        )
        service.start(timeout_seconds=1)
        service.submit(self.event(1))
        self.wait_for(lambda: service.snapshot().state is WorkerState.FAILED)
        snapshot = service.snapshot()
        self.assertFalse(snapshot.thread_alive)
        self.assertIn("unexpected", snapshot.last_error or "")
        with self.open_spool() as spool:
            self.assertEqual(spool.stats().pending_rows, 1)


if __name__ == "__main__":
    unittest.main()

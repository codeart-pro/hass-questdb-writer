"""Tests for the writer health sensor platform."""

from __future__ import annotations

from dataclasses import replace
import unittest
from unittest.mock import Mock

from homeassistant.components.sensor import SensorDeviceClass

from custom_components.hass_questdb_writer.sensor import (
    QuestDbHealthSensor,
    _sensor_specs,
)
from custom_components.hass_questdb_writer.runtime import RuntimeSnapshot
from custom_components.hass_questdb_writer.worker import (
    WorkerSnapshot,
    WorkerState,
)


def worker_snapshot(**overrides: object) -> WorkerSnapshot:
    values: dict[str, object] = {
        "state": WorkerState.RUNNING,
        "thread_alive": True,
        "accepting": True,
        "ingress_queue_depth": 0,
        "held_unpersisted": 0,
        "ingress_high_watermark": 1,
        "submitted_events": 10,
        "persisted_events": 10,
        "delivered_events": 9,
        "retry_attempts": 1,
        "dead_lettered_events": 0,
        "dead_letter_evicted_events": 0,
        "uncertain_delivered_events": 0,
        "overflowed_events": 0,
        "oversized_events": 0,
        "pending_rows": 1,
        "pending_bytes": 100,
        "dead_letter_rows": 0,
        "dead_letter_bytes": 0,
        "retry_delay_seconds": 2.0,
        "last_error": None,
        "last_success_ns": None,
        "block_reason": None,
        "last_errors": (),
    }
    values.update(overrides)
    return WorkerSnapshot(**values)  # type: ignore[arg-type]


def runtime_snapshot(**worker_overrides: object) -> RuntimeSnapshot:
    return RuntimeSnapshot(
        listener_active=True,
        state_events_seen=20,
        state_events_accepted=10,
        state_events_without_new_state=0,
        state_events_skipped_unknown=2,
        state_events_excluded=1,
        attribute_entries_removed=3,
        conversion_errors=0,
        submission_rejections=0,
        worker=worker_snapshot(**worker_overrides),
    )


class QuestDbHealthSensorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.runtime = Mock(snapshot=runtime_snapshot)
        self.entry = Mock(entry_id="entry-1")

    def sensor(self, key: str) -> QuestDbHealthSensor:
        spec = next(spec for spec in _sensor_specs() if spec.key == key)
        return QuestDbHealthSensor(self.runtime, self.entry, spec)

    async def test_state_sensor_reports_worker_state(self) -> None:
        sensor = self.sensor("state")
        self.assertEqual(sensor.device_class, SensorDeviceClass.ENUM)
        self.assertIn("running", sensor.options or [])
        await sensor.async_update()
        self.assertEqual(sensor.native_value, "running")

    async def test_blocked_state_is_reported(self) -> None:
        self.runtime.snapshot = Mock(
            return_value=runtime_snapshot(
                state=WorkerState.BLOCKED, block_reason="auth"
            )
        )
        sensor = self.sensor("state")
        await sensor.async_update()
        self.assertEqual(sensor.native_value, "blocked")

    async def test_last_success_age_computes_seconds(self) -> None:
        import time

        self.runtime.snapshot = Mock(
            return_value=runtime_snapshot(
                last_success_ns=time.time_ns() - 5_000_000_000
            )
        )
        sensor = self.sensor("last_success_age")
        self.assertEqual(sensor.device_class, SensorDeviceClass.DURATION)
        await sensor.async_update()
        self.assertIsInstance(sensor.native_value, float)
        self.assertGreaterEqual(sensor.native_value, 4.5)
        self.assertLess(sensor.native_value, 6.0)

    async def test_last_success_age_unknown_without_success(self) -> None:
        sensor = self.sensor("last_success_age")
        await sensor.async_update()
        self.assertIsNone(sensor.native_value)

    async def test_pending_rows_and_delivered_events(self) -> None:
        pending = self.sensor("pending_rows")
        delivered = self.sensor("delivered_events")
        await pending.async_update()
        await delivered.async_update()
        self.assertEqual(pending.native_value, 1)
        self.assertEqual(delivered.native_value, 9)
        self.assertIsNotNone(delivered.state_class)

    async def test_last_error_reports_error_text(self) -> None:
        self.runtime.snapshot = Mock(
            return_value=runtime_snapshot(
                last_error="QuestDB request failed: gaierror"
            )
        )
        sensor = self.sensor("last_error")
        await sensor.async_update()
        self.assertEqual(
            sensor.native_value, "QuestDB request failed: gaierror"
        )

    async def test_unique_ids_and_device_are_stable(self) -> None:
        sensor = self.sensor("state")
        self.assertEqual(sensor.unique_id, "entry-1-state")
        self.assertEqual(sensor.device_info["identifiers"], {("hass_questdb_writer", "entry-1")})


if __name__ == "__main__":
    unittest.main()

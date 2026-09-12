"""Tests for the writer health sensor platform."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import AsyncMock, Mock, patch

from homeassistant.components.sensor import SensorDeviceClass

from custom_components.hass_questdb_writer import sensor as sensor_module
from custom_components.hass_questdb_writer.entity import QuestDbWriterEntity
from custom_components.hass_questdb_writer.sensor import (
    HealthSensorSpec,
    QuestDbHealthSensor,
    QuestDbTableSizeSensor,
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

    async def test_last_error_is_none_when_clean(self) -> None:
        sensor = self.sensor("last_error")
        await sensor.async_update()
        self.assertEqual(sensor.native_value, "none")

    async def test_unique_ids_and_device_are_stable(self) -> None:
        sensor = self.sensor("state")
        self.assertEqual(sensor.unique_id, "entry-1-state")
        self.assertEqual(sensor.device_info["identifiers"], {("hass_questdb_writer", "entry-1")})

    def table_size_sensor(self) -> QuestDbTableSizeSensor:
        entry = Mock(
            entry_id="entry-1",
            data={
                "host": "questdb",
                "port": 9000,
                "table": "hass",
                "use_tls": False,
                "username": None,
                "password": None,
            },
        )
        return QuestDbTableSizeSensor(entry, "hass")

    async def test_table_size_reports_bytes(self) -> None:
        sensor = self.table_size_sensor()
        sensor.hass = Mock(
            async_add_executor_job=AsyncMock(
                side_effect=lambda fn, *a: fn(*a)
            )
        )
        with patch(
            "custom_components.hass_questdb_writer.sensor.IlpHttpTransport"
        ) as transport_cls:
            transport_cls.return_value.exec_query = Mock(
                return_value={"dataset": [[1073741824]]}
            )
            await sensor.async_update()
        self.assertEqual(sensor.native_value, 1073.7)
        self.assertEqual(sensor.native_unit_of_measurement, "MB")
        self.assertIsNone(sensor.device_class)
        self.assertTrue(sensor.available)

    async def test_table_size_goes_unavailable_on_transport_error(self) -> None:
        sensor = self.table_size_sensor()
        sensor.hass = Mock(
            async_add_executor_job=AsyncMock(
                side_effect=lambda fn, *a: fn(*a)
            )
        )
        from custom_components.hass_questdb_writer.transport import (
            RetryableIlpError,
        )

        with patch(
            "custom_components.hass_questdb_writer.sensor.IlpHttpTransport"
        ) as transport_cls:
            transport_cls.return_value.exec_query = Mock(
                side_effect=RetryableIlpError(
                    "down", retryable=True, delivery_uncertain=False
                )
            )
            await sensor.async_update()
        self.assertFalse(sensor.available)


class TableSizeAvailabilityLoggingTests(unittest.IsolatedAsyncioTestCase):
    """Silver rule log-when-unavailable: one line per availability transition."""

    logger = "custom_components.hass_questdb_writer.sensor"

    def setUp(self) -> None:
        self.entry = Mock(
            entry_id="entry-1",
            data={
                "host": "questdb",
                "port": 9000,
                "table": "hass",
                "use_tls": False,
                "username": None,
                "password": None,
            },
        )

    def sensor(self) -> QuestDbTableSizeSensor:
        sensor = QuestDbTableSizeSensor(self.entry, "hass")
        sensor.hass = Mock(
            async_add_executor_job=AsyncMock(side_effect=lambda fn, *a: fn(*a))
        )
        return sensor

    async def test_first_failure_and_recovery_are_logged_once(self) -> None:
        from custom_components.hass_questdb_writer.transport import (
            RetryableIlpError,
        )

        sensor = self.sensor()
        with patch(
            "custom_components.hass_questdb_writer.sensor.IlpHttpTransport"
        ) as transport_cls:
            transport_cls.return_value.exec_query = Mock(
                side_effect=RetryableIlpError(
                    "connection refused", retryable=True, delivery_uncertain=False
                )
            )
            with self.assertLogs(self.logger, level="WARNING") as logged:
                await sensor.async_update()
                await sensor.async_update()

            self.assertFalse(sensor.available)
            self.assertEqual(len(logged.records), 1)
            self.assertIn(
                "connection refused", logged.records[0].getMessage()
            )

            transport_cls.return_value.exec_query = Mock(
                return_value={"dataset": [[1_000_000]]}
            )
            with self.assertLogs(self.logger, level="INFO") as recovered:
                await sensor.async_update()
                await sensor.async_update()

        self.assertTrue(sensor.available)
        self.assertEqual(len(recovered.records), 1)
        self.assertEqual(sensor.native_value, 1.0)

    async def test_healthy_refreshes_stay_silent(self) -> None:
        sensor = self.sensor()
        with patch(
            "custom_components.hass_questdb_writer.sensor.IlpHttpTransport"
        ) as transport_cls:
            transport_cls.return_value.exec_query = Mock(
                return_value={"dataset": [[1_000_000]]}
            )
            with self.assertNoLogs(self.logger, level="INFO"):
                await sensor.async_update()


class SharedEntityBaseTests(unittest.TestCase):
    """Rule common-modules: the shared entity plumbing lives in entity.py."""

    def setUp(self) -> None:
        self.entry = Mock(entry_id="entry-1", data={})
        self.runtime = Mock(snapshot=runtime_snapshot)

    def test_sensors_derive_from_the_base_entity(self) -> None:
        self.assertTrue(issubclass(QuestDbHealthSensor, QuestDbWriterEntity))
        self.assertTrue(issubclass(QuestDbTableSizeSensor, QuestDbWriterEntity))
        self.assertEqual(
            QuestDbWriterEntity.__module__,
            "custom_components.hass_questdb_writer.entity",
        )

    def test_both_entities_report_the_same_device(self) -> None:
        health = QuestDbHealthSensor(
            self.runtime, self.entry, _sensor_specs()[0]
        )
        table = QuestDbTableSizeSensor(self.entry, "hass")
        self.assertEqual(health.device_info, table.device_info)
        self.assertEqual(health.device_info["name"], "HASS QuestDB Writer")
        self.assertEqual(
            health.device_info["identifiers"], {("hass_questdb_writer", "entry-1")}
        )
        self.assertEqual(health.unique_id, "entry-1-state")
        self.assertEqual(table.unique_id, "entry-1-table_size")


class SensorNamingTests(unittest.IsolatedAsyncioTestCase):
    """Bronze rule has-entity-name: labels describe the entity, not the device."""

    def setUp(self) -> None:
        self.runtime = Mock(snapshot=runtime_snapshot)
        self.entry = Mock(entry_id="entry-1", data={})

    def health_sensor(self, spec: HealthSensorSpec) -> QuestDbHealthSensor:
        return QuestDbHealthSensor(self.runtime, self.entry, spec)

    def test_health_sensor_labels_are_device_relative(self) -> None:
        expected = {
            "state": "State",
            "last_success_age": "Seconds since last delivery",
            "pending_rows": "Pending rows",
            "delivered_events": "Events delivered",
            "last_error": "Last delivery error",
        }
        names: dict[str, str | None] = {}
        for spec in _sensor_specs():
            sensor = self.health_sensor(spec)
            self.assertTrue(sensor.has_entity_name, spec.key)
            label = (sensor.name or "").lower()
            self.assertNotIn("writer", label)
            self.assertNotIn("questdb", label)
            names[spec.key] = sensor.name
        self.assertEqual(names, expected)

    def test_table_size_label_is_device_relative(self) -> None:
        sensor = QuestDbTableSizeSensor(self.entry, "hass")
        self.assertTrue(sensor.has_entity_name)
        self.assertEqual(sensor.name, "Table size")


class SensorPollingIntervalTests(unittest.IsolatedAsyncioTestCase):
    """ADR-0011: explicit platform interval, own timer for the table size."""

    def setUp(self) -> None:
        self.entry = Mock(
            entry_id="entry-1",
            data={
                "host": "questdb",
                "port": 9000,
                "table": "hass",
                "use_tls": False,
                "username": None,
                "password": None,
            },
        )

    def table_size_sensor(self) -> QuestDbTableSizeSensor:
        sensor = QuestDbTableSizeSensor(self.entry, "hass")
        sensor.hass = Mock(
            async_add_executor_job=AsyncMock(side_effect=lambda fn, *a: fn(*a))
        )
        return sensor

    def test_platform_declares_an_explicit_scan_interval(self) -> None:
        self.assertEqual(
            sensor_module.SCAN_INTERVAL, timedelta(seconds=30)
        )

    def test_platform_limits_parallel_updates(self) -> None:
        self.assertEqual(sensor_module.PARALLEL_UPDATES, 1)

    def test_health_sensors_keep_using_the_platform_interval(self) -> None:
        runtime = Mock(snapshot=runtime_snapshot)
        sensor = QuestDbHealthSensor(runtime, self.entry, _sensor_specs()[0])
        self.assertTrue(sensor.should_poll)

    def test_table_size_sensor_leaves_platform_polling(self) -> None:
        self.assertFalse(self.table_size_sensor().should_poll)
        self.assertEqual(
            sensor_module.TABLE_SIZE_SCAN_INTERVAL, timedelta(minutes=5)
        )

    async def test_table_size_sensor_measures_once_and_starts_its_timer(
        self,
    ) -> None:
        sensor = self.table_size_sensor()
        unsub = Mock()
        with (
            patch.object(
                QuestDbTableSizeSensor, "async_update", AsyncMock()
            ) as update,
            patch(
                "custom_components.hass_questdb_writer.sensor.async_track_time_interval",
                return_value=unsub,
            ) as track,
        ):
            await sensor.async_added_to_hass()

        update.assert_awaited_once()
        track.assert_called_once_with(
            sensor.hass,
            sensor._async_refresh,
            timedelta(minutes=5),
            name="hass_questdb_writer table size",
        )
        self.assertIs(sensor._unsub_timer, unsub)

    async def test_timer_callback_refreshes_and_writes_the_state(self) -> None:
        sensor = self.table_size_sensor()
        with (
            patch.object(
                QuestDbTableSizeSensor, "async_update", AsyncMock()
            ) as update,
            patch.object(
                QuestDbTableSizeSensor, "async_write_ha_state"
            ) as write_state,
        ):
            await sensor._async_refresh(
                datetime(2026, 9, 12, 10, 0, tzinfo=timezone.utc)
            )

        update.assert_awaited_once()
        write_state.assert_called_once_with()

    async def test_removal_cancels_the_timer(self) -> None:
        sensor = self.table_size_sensor()
        unsub = Mock()
        sensor._unsub_timer = unsub

        await sensor.async_will_remove_from_hass()

        unsub.assert_called_once_with()
        self.assertIsNone(sensor._unsub_timer)


if __name__ == "__main__":
    unittest.main()

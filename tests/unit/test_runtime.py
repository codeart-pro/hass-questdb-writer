"""Tests for the Home Assistant lifecycle adapter."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import datetime as dt
import json
from pathlib import Path
import tempfile
from typing import Any
import unittest
from unittest.mock import AsyncMock, Mock, patch

from homeassistant.const import EVENT_STATE_CHANGED, STATE_UNKNOWN
from homeassistant.core import Context, Event, State
from homeassistant.helpers.entityfilter import convert_include_exclude_filter

from custom_components.hass_questdb_writer.attribute_filter import AttributeFilter
from custom_components.hass_questdb_writer.event import EventEnvelope
from custom_components.hass_questdb_writer.runtime import (
    ConnectionConfiguration,
    HassQuestDbRuntime,
    RuntimeConfiguration,
    SpoolConfiguration,
    datetime_to_epoch_ns,
)
from custom_components.hass_questdb_writer.worker import (
    WorkerSettings,
    WorkerSnapshot,
    WorkerState,
)


def worker_snapshot(*, last_error: str | None = None) -> WorkerSnapshot:
    return WorkerSnapshot(
        state=WorkerState.RUNNING,
        thread_alive=True,
        accepting=True,
        ingress_queue_depth=0,
        held_unpersisted=0,
        ingress_high_watermark=0,
        submitted_events=0,
        persisted_events=0,
        delivered_events=0,
        retry_attempts=0,
        dead_lettered_events=0,
        dead_letter_evicted_events=0,
        uncertain_delivered_events=0,
        overflowed_events=0,
        oversized_events=0,
        pending_rows=0,
        pending_bytes=0,
        dead_letter_rows=0,
        dead_letter_bytes=0,
        retry_delay_seconds=None,
        last_error=last_error,
        last_success_ns=None,
    )


class FakeService:
    def __init__(self, timeline: list[str]) -> None:
        self.timeline = timeline
        self.events: list[EventEnvelope] = []
        self.submit_result = True
        self.start_error: Exception | None = None
        self.start_timeout: float | None = None
        self.stop_timeout: float | None = None

    def start(self, *, timeout_seconds: float) -> None:
        self.start_timeout = timeout_seconds
        self.timeline.append("worker_start")
        if self.start_error is not None:
            raise self.start_error

    def stop(self, *, timeout_seconds: float) -> bool:
        self.stop_timeout = timeout_seconds
        self.timeline.append("worker_stop")
        return True

    def submit(self, event: EventEnvelope) -> bool:
        self.events.append(event)
        return self.submit_result

    def snapshot(self) -> WorkerSnapshot:
        base = worker_snapshot(last_error="rejected")
        if getattr(self, "blocked_auth", False):
            return replace(
                base,
                state=WorkerState.BLOCKED,
                block_reason="auth",
            )
        return base


class FakeBus:
    def __init__(self, timeline: list[str]) -> None:
        self.timeline = timeline
        self.event_type: str | None = None
        self.listener: Any = None
        self.listen_error: Exception | None = None
        self.unsubscribe_error: Exception | None = None

    def async_listen(self, event_type: str, listener: Any) -> Any:
        self.timeline.append("listener_start")
        if self.listen_error is not None:
            raise self.listen_error
        self.event_type = event_type
        self.listener = listener

        def unsubscribe() -> None:
            self.timeline.append("listener_stop")
            if self.unsubscribe_error is not None:
                raise self.unsubscribe_error
            self.listener = None

        return unsubscribe


class FakeHass:
    def __init__(self, timeline: list[str]) -> None:
        self.bus = FakeBus(timeline)

    def async_add_executor_job(self, target: Any, *args: Any) -> Any:
        return asyncio.get_running_loop().run_in_executor(None, target, *args)

    def async_create_task(self, coro: Any) -> Any:
        return asyncio.get_running_loop().create_task(coro)

    def async_create_background_task(self, coro: Any, name: str) -> Any:
        return asyncio.get_running_loop().create_task(coro)


class HassQuestDbRuntimeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.timeline: list[str] = []
        self.hass = FakeHass(self.timeline)
        self.service = FakeService(self.timeline)

    def configuration(self) -> RuntimeConfiguration:
        return RuntimeConfiguration(
            table="ha_events",
            spool=SpoolConfiguration(
                path=Path(self.temporary_directory.name) / "runtime.db",
                max_pending_rows=100,
                max_pending_bytes=1_000_000,
                max_event_bytes=10_000,
                max_dead_letter_rows=10,
                max_dead_letter_bytes=100_000,
                busy_timeout_seconds=0.25,
            ),
            connection=ConnectionConfiguration(
                host="questdb",
                port=9000,
                use_tls=False,
                timeout_seconds=1,
            ),
            worker=WorkerSettings(
                ingress_queue_capacity=10,
                max_serialized_event_bytes=10_000,
                persist_batch_rows=10,
                delivery_batch_rows=10,
                delivery_batch_bytes=100_000,
                flush_interval_seconds=0.1,
                retry_initial_seconds=0.1,
                retry_max_seconds=1,
                retry_multiplier=2,
                retry_jitter_ratio=0,
                flush_on_shutdown=False,
            ),
            start_timeout_seconds=2,
            stop_timeout_seconds=3,
            tracked_entity_ids=None,
        )

    def runtime(self) -> HassQuestDbRuntime:
        return HassQuestDbRuntime(
            self.hass,  # type: ignore[arg-type]
            self.configuration(),
            service_factory=lambda: self.service,  # type: ignore[arg-type,return-value]
            event_id_factory=lambda: "event-fixed",
            wall_time_ns=lambda: 1_700_000_000_500_000_000,
        )

    async def test_starts_worker_before_listener_and_stops_in_reverse(self) -> None:
        runtime = self.runtime()
        await runtime.async_start()
        self.assertEqual(
            self.timeline, ["worker_start", "listener_start"]
        )
        self.assertEqual(self.hass.bus.event_type, EVENT_STATE_CHANGED)
        self.assertEqual(self.service.start_timeout, 2)
        self.assertTrue(runtime.snapshot().listener_active)

        self.assertTrue(await runtime.async_stop())
        self.assertEqual(
            self.timeline,
            [
                "worker_start",
                "listener_start",
                "listener_stop",
                "worker_stop",
            ],
        )
        self.assertEqual(self.service.stop_timeout, 3)
        self.assertFalse(runtime.snapshot().listener_active)

    async def test_failed_worker_start_runs_bounded_cleanup(self) -> None:
        self.service.start_error = RuntimeError("startup failed")
        runtime = self.runtime()
        with self.assertRaisesRegex(RuntimeError, "startup failed"):
            await runtime.async_start()
        self.assertEqual(self.timeline, ["worker_start", "worker_stop"])
        self.assertEqual(self.service.stop_timeout, 3)

    async def test_failed_listener_registration_stops_worker(self) -> None:
        self.hass.bus.listen_error = RuntimeError("listener failed")
        runtime = self.runtime()
        with self.assertRaisesRegex(RuntimeError, "listener failed"):
            await runtime.async_start()
        self.assertEqual(
            self.timeline,
            ["worker_start", "listener_start", "worker_stop"],
        )

    async def test_unsubscribe_failure_still_stops_worker(self) -> None:
        runtime = self.runtime()
        await runtime.async_start()
        self.hass.bus.unsubscribe_error = RuntimeError("unsubscribe failed")
        self.assertFalse(await runtime.async_stop())
        self.assertEqual(self.timeline[-2:], ["listener_stop", "worker_stop"])
        self.assertTrue(runtime.snapshot().listener_active)

    async def test_converts_state_event_using_ha_json_serializer(self) -> None:
        runtime = self.runtime()
        await runtime.async_start()
        timestamp = dt.datetime(
            2023, 11, 14, 22, 13, 20, 123456, tzinfo=dt.UTC
        )
        state = State(
            "sensor.kitchen",
            "on",
            attributes={"values": {1, 2}, "when": timestamp},
            last_changed=timestamp - dt.timedelta(seconds=2),
            last_updated=timestamp - dt.timedelta(seconds=1),
            context=Context(id="context-1"),
        )
        event = Event(
            EVENT_STATE_CHANGED,
            {
                "entity_id": state.entity_id,
                "old_state": None,
                "new_state": state,
            },
            time_fired_timestamp=timestamp.timestamp(),
        )

        self.hass.bus.listener(event)
        self.assertEqual(len(self.service.events), 1)
        envelope = self.service.events[0]
        self.assertEqual(envelope.event_id, "event-fixed")
        self.assertEqual(envelope.entity_id, "sensor.kitchen")
        self.assertEqual(envelope.last_updated_ns, 1_699_999_999_123_456_000)
        self.assertEqual(envelope.last_changed_ns, 1_699_999_998_123_456_000)
        self.assertEqual(envelope.context_id, "context-1")
        self.assertIn('"values":[1,2]', envelope.attributes_json)
        self.assertIn("2023-11-14T22:13:20.123456+00:00", envelope.attributes_json)
        snapshot = runtime.snapshot()
        self.assertEqual(snapshot.state_events_seen, 1)
        self.assertEqual(snapshot.state_events_accepted, 1)
        await runtime.async_stop()

    async def test_ignores_state_removal_without_new_state(self) -> None:
        runtime = self.runtime()
        await runtime.async_start()
        event = Event(
            EVENT_STATE_CHANGED,
            {
                "entity_id": "sensor.removed",
                "old_state": State("sensor.removed", "on"),
                "new_state": None,
            },
        )
        self.hass.bus.listener(event)
        snapshot = runtime.snapshot()
        self.assertEqual(snapshot.state_events_seen, 1)
        self.assertEqual(snapshot.state_events_without_new_state, 1)
        self.assertEqual(self.service.events, [])
        await runtime.async_stop()

    async def test_skips_unknown_state_events(self) -> None:
        runtime = self.runtime()
        await runtime.async_start()
        state = State("sensor.unknown_test", STATE_UNKNOWN)
        event = Event(
            EVENT_STATE_CHANGED,
            {
                "entity_id": state.entity_id,
                "old_state": None,
                "new_state": state,
            },
        )
        self.hass.bus.listener(event)
        snapshot = runtime.snapshot()
        self.assertEqual(snapshot.state_events_seen, 1)
        self.assertEqual(snapshot.state_events_skipped_unknown, 1)
        self.assertEqual(snapshot.state_events_accepted, 0)
        self.assertEqual(self.service.events, [])
        await runtime.async_stop()

    async def test_entity_filter_excludes_entities(self) -> None:
        configuration = replace(
            self.configuration(),
            entity_filter=convert_include_exclude_filter(
                {
                    "include": {
                        "domains": [],
                        "entity_globs": [],
                        "entities": ["sensor.kitchen"],
                    },
                    "exclude": {
                        "domains": ["binary_sensor"],
                        "entity_globs": [],
                        "entities": [],
                    },
                }
            ),
        )
        runtime = HassQuestDbRuntime(
            self.hass,  # type: ignore[arg-type]
            configuration,
            service_factory=lambda: self.service,  # type: ignore[arg-type,return-value]
            event_id_factory=lambda: "event-fixed",
            wall_time_ns=lambda: 1_700_000_000_500_000_000,
        )
        await runtime.async_start()
        for entity_id, state in (
            ("sensor.kitchen", "on"),
            ("binary_sensor.door", "on"),
            ("sensor.garden", "42"),
        ):
            event = Event(
                EVENT_STATE_CHANGED,
                {
                    "entity_id": entity_id,
                    "old_state": None,
                    "new_state": State(entity_id, state),
                },
            )
            self.hass.bus.listener(event)
        snapshot = runtime.snapshot()
        self.assertEqual(snapshot.state_events_seen, 3)
        self.assertEqual(snapshot.state_events_excluded, 1)
        self.assertEqual(snapshot.state_events_accepted, 2)
        self.assertEqual(
            [event.entity_id for event in self.service.events],
            ["sensor.kitchen", "sensor.garden"],
        )
        await runtime.async_stop()

    async def test_entity_include_list_is_an_allowlist(self) -> None:
        configuration = replace(
            self.configuration(),
            entity_filter=convert_include_exclude_filter(
                {
                    "include": {
                        "domains": [],
                        "entity_globs": [],
                        "entities": ["sensor.kitchen"],
                    },
                    "exclude": {
                        "domains": [],
                        "entity_globs": [],
                        "entities": [],
                    },
                }
            ),
        )
        runtime = HassQuestDbRuntime(
            self.hass,  # type: ignore[arg-type]
            configuration,
            service_factory=lambda: self.service,  # type: ignore[arg-type,return-value]
            event_id_factory=lambda: "event-fixed",
            wall_time_ns=lambda: 1_700_000_000_500_000_000,
        )
        await runtime.async_start()
        for entity_id in ("sensor.kitchen", "sensor.garden"):
            event = Event(
                EVENT_STATE_CHANGED,
                {
                    "entity_id": entity_id,
                    "old_state": None,
                    "new_state": State(entity_id, "on"),
                },
            )
            self.hass.bus.listener(event)
        snapshot = runtime.snapshot()
        self.assertEqual(snapshot.state_events_excluded, 1)
        self.assertEqual(snapshot.state_events_accepted, 1)
        self.assertEqual(self.service.events[0].entity_id, "sensor.kitchen")
        await runtime.async_stop()

    async def test_conversion_failure_does_not_escape_event_callback(self) -> None:
        runtime = self.runtime()
        await runtime.async_start()
        state = State("sensor.invalid", "on", attributes={"bad": object()})
        event = Event(
            EVENT_STATE_CHANGED,
            {
                "entity_id": state.entity_id,
                "old_state": None,
                "new_state": state,
            },
        )
        self.hass.bus.listener(event)
        self.assertEqual(runtime.snapshot().conversion_errors, 1)
        self.assertEqual(self.service.events, [])
        await runtime.async_stop()

    async def test_submission_rejection_is_visible(self) -> None:
        runtime = self.runtime()
        self.service.submit_result = False
        await runtime.async_start()
        state = State("sensor.rejected", "on")
        event = Event(
            EVENT_STATE_CHANGED,
            {
                "entity_id": state.entity_id,
                "old_state": None,
                "new_state": state,
            },
        )
        self.hass.bus.listener(event)
        snapshot = runtime.snapshot()
        self.assertEqual(snapshot.submission_rejections, 1)
        self.assertEqual(snapshot.state_events_accepted, 0)
        await runtime.async_stop()

    def test_datetime_conversion_is_integer_and_rejects_naive_values(self) -> None:
        value = dt.datetime(1970, 1, 2, 0, 0, 0, 1, tzinfo=dt.UTC)
        self.assertEqual(datetime_to_epoch_ns(value), 86_400_000_001_000)
        with self.assertRaises(ValueError):
            datetime_to_epoch_ns(value.replace(tzinfo=None))

    async def test_attribute_filter_applies_and_counts_removed_entries(self) -> None:
        configuration = replace(
            self.configuration(),
            attribute_filter=AttributeFilter(
                allow=("friendly_name", "unit_*"),
                deny=("rssi",),
            ),
        )
        runtime = HassQuestDbRuntime(
            self.hass,  # type: ignore[arg-type]
            configuration,
            service_factory=lambda: self.service,  # type: ignore[arg-type,return-value]
            event_id_factory=lambda: "event-fixed",
            wall_time_ns=lambda: 1_700_000_000_500_000_000,
        )
        await runtime.async_start()
        timestamp = dt.datetime(2023, 11, 14, 22, 13, 20, 123456, tzinfo=dt.UTC)
        state = State(
            "sensor.kitchen",
            "23.5",
            attributes={
                "friendly_name": "Kitchen",
                "unit_of_measurement": "°C",
                "rssi": -70,
                "linkquality": 100,
                "device_class": "temperature",
            },
            last_changed=timestamp - dt.timedelta(seconds=2),
            last_updated=timestamp - dt.timedelta(seconds=1),
            context=Context(id="context-1"),
        )
        event = Event(
            EVENT_STATE_CHANGED,
            {
                "entity_id": state.entity_id,
                "old_state": None,
                "new_state": state,
            },
        )
        self.hass.bus.listener(event)
        self.assertEqual(len(self.service.events), 1)
        self.assertEqual(
            json.loads(self.service.events[0].attributes_json),
            {"friendly_name": "Kitchen", "unit_of_measurement": "°C"},
        )
        self.assertEqual(runtime.snapshot().attribute_entries_removed, 3)
        await runtime.async_stop()

    async def test_attribute_filter_deny_wins_over_allow(self) -> None:
        configuration = replace(
            self.configuration(),
            attribute_filter=AttributeFilter(
                allow=("friendly_name", "rssi"),
                deny=("rssi",),
            ),
        )
        runtime = HassQuestDbRuntime(
            self.hass,  # type: ignore[arg-type]
            configuration,
            service_factory=lambda: self.service,  # type: ignore[arg-type,return-value]
            event_id_factory=lambda: "event-fixed",
            wall_time_ns=lambda: 1_700_000_000_500_000_000,
        )
        await runtime.async_start()
        state = State(
            "sensor.denied",
            "on",
            attributes={"friendly_name": "Denied", "rssi": -55},
        )
        event = Event(
            EVENT_STATE_CHANGED,
            {
                "entity_id": state.entity_id,
                "old_state": None,
                "new_state": state,
            },
        )
        self.hass.bus.listener(event)
        self.assertEqual(
            json.loads(self.service.events[0].attributes_json),
            {"friendly_name": "Denied"},
        )
        await runtime.async_stop()

    async def test_auth_block_creates_repair_issue(self) -> None:
        create = Mock()
        delete = Mock()
        with (
            patch(
                "homeassistant.helpers.issue_registry.async_create_issue", create
            ),
            patch(
                "homeassistant.helpers.issue_registry.async_delete_issue", delete
            ),
        ):
            self.service.blocked_auth = True
            runtime = HassQuestDbRuntime(
                self.hass,
                self.configuration(),
                service_factory=lambda: self.service,
                entry_id="entry-1",
                monitor_interval_seconds=0.01,
            )
            await runtime.async_start()
            create.assert_called_once()
            self.assertEqual(create.call_args.args[2], "auth_failed_entry-1")
            self.assertEqual(create.call_args.kwargs["is_persistent"], True)
            self.assertEqual(create.call_args.kwargs["is_fixable"], False)
            self.assertEqual(
                create.call_args.kwargs["translation_key"], "auth_failed"
            )
            # once the worker leaves the auth-blocked state, the issue is gone
            self.service.blocked_auth = False
            await asyncio.sleep(0.05)
            delete.assert_called()
            await runtime.async_stop()

    async def test_auth_issue_is_removed_on_stop(self) -> None:
        delete = Mock()
        with patch(
            "homeassistant.helpers.issue_registry.async_delete_issue", delete
        ):
            runtime = HassQuestDbRuntime(
                self.hass,
                self.configuration(),
                service_factory=lambda: self.service,
                entry_id="entry-1",
            )
            await runtime.async_start()
            await runtime.async_stop()
            delete.assert_called()
            self.assertEqual(delete.call_args.args[2], "auth_failed_entry-1")


if __name__ == "__main__":
    unittest.main()

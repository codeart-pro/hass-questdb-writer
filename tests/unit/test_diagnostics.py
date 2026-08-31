"""Tests for the diagnostics payload (redaction, structure, probes)."""

from __future__ import annotations

from dataclasses import asdict, replace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from custom_components.hass_questdb_writer.diagnostics import (
    async_get_config_entry_diagnostics,
)
from custom_components.hass_questdb_writer.runtime import RuntimeSnapshot
from custom_components.hass_questdb_writer.transport import (
    RetryableIlpError,
)
from custom_components.hass_questdb_writer.worker import (
    WorkerErrorRecord,
    WorkerSnapshot,
    WorkerState,
)


def worker_snapshot() -> WorkerSnapshot:
    return WorkerSnapshot(
        state=WorkerState.RUNNING,
        thread_alive=True,
        accepting=True,
        ingress_queue_depth=0,
        held_unpersisted=0,
        ingress_high_watermark=0,
        submitted_events=10,
        persisted_events=10,
        delivered_events=9,
        retry_attempts=1,
        dead_lettered_events=0,
        dead_letter_evicted_events=0,
        uncertain_delivered_events=0,
        overflowed_events=0,
        oversized_events=0,
        pending_rows=1,
        pending_bytes=100,
        dead_letter_rows=0,
        dead_letter_bytes=0,
        retry_delay_seconds=2.0,
        last_error="QuestDB request failed: gaierror",
        last_success_ns=1_700_000_000_000_000_000,
        block_reason=None,
        last_errors=(
            WorkerErrorRecord(ts_ns=1_700_000_000_000_000_000, error="boom"),
        ),
    )


def runtime_snapshot() -> RuntimeSnapshot:
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
        worker=worker_snapshot(),
    )


class DiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.entry = Mock(
            data={
                "host": "questdb",
                "port": 9000,
                "table": "ha_events",
                "use_tls": False,
                "username": "admin",
                "password": "top-secret",
            },
            options={
                "retention_days": 30,
                "include": {"entities": ["sensor.a"]},
            },
            runtime_data=Mock(snapshot=runtime_snapshot),
        )
        self.hass = Mock(
            version="2026.7.2",
            async_add_executor_job=AsyncMock(
                side_effect=lambda fn, *args: fn(*args)
            ),
        )

    async def test_password_is_redacted(self) -> None:
        with patch(
            "custom_components.hass_questdb_writer.diagnostics.IlpHttpTransport"
        ):
            result = await async_get_config_entry_diagnostics(
                self.hass, self.entry
            )
        self.assertEqual(
            result["entry_data"]["password"], "**REDACTED**"
        )
        self.assertEqual(result["entry_data"]["host"], "questdb")

    async def test_structure_contains_runtime_and_schema(self) -> None:
        transport = Mock()
        transport.exec_query = Mock(
            side_effect=[
                {"dataset": [[30, "DAY"]]},
                {"dataset": [["Build Information: QuestDB 10.0.1, JDK 17.0.9"]]},
            ]
        )
        with patch(
            "custom_components.hass_questdb_writer.diagnostics.IlpHttpTransport",
            return_value=transport,
        ):
            result = await async_get_config_entry_diagnostics(
                self.hass, self.entry
            )
        self.assertEqual(result["domain"], "hass_questdb_writer")
        self.assertEqual(
            result["versions"]["integration"], "0.1.0-dev0"
        )
        from homeassistant.const import __version__ as HA_VERSION

        self.assertEqual(result["versions"]["home_assistant"], HA_VERSION)
        worker = result["runtime"]["worker"]
        self.assertEqual(worker["state"], "running")
        self.assertEqual(worker["retry_attempts"], 1)
        self.assertEqual(worker["last_errors"][0]["error"], "boom")
        self.assertEqual(result["runtime"]["state_events_skipped_unknown"], 2)
        self.assertEqual(result["schema"]["ttl_days"], 30)
        self.assertIn(
            "QuestDB", result["schema"]["questdb_version"]
        )
        self.assertEqual(result["schema"]["table"], "ha_events")
        self.assertEqual(result["options"]["retention_days"], 30)

    async def test_unreachable_server_marks_schema_unavailable(self) -> None:
        transport = Mock()
        transport.exec_query = Mock(
            side_effect=RetryableIlpError(
                "down", retryable=True, delivery_uncertain=False
            )
        )
        with patch(
            "custom_components.hass_questdb_writer.diagnostics.IlpHttpTransport",
            return_value=transport,
        ):
            result = await async_get_config_entry_diagnostics(
                self.hass, self.entry
            )
        self.assertTrue(result["schema"]["unavailable"])
        self.assertEqual(result["schema"]["table"], "ha_events")

    async def test_runtime_snapshot_can_serialize(self) -> None:
        # Regression guard: asdict() over the snapshot must never raise.
        dumped = asdict(replace(runtime_snapshot()))
        self.assertEqual(dumped["worker"]["state"], "running")


if __name__ == "__main__":
    unittest.main()

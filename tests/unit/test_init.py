"""Tests for config-entry setup and provisional runtime mapping."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
import unittest

from homeassistant.exceptions import ConfigEntryNotReady

from custom_components.hass_questdb_writer import (
    _runtime_configuration,
    async_setup_entry,
    async_unload_entry,
)
from custom_components.hass_questdb_writer.const import (
    CONF_HOST,
    CONF_PORT,
    CONF_TABLE,
    CONF_USE_TLS,
    PROVISIONAL_DELIVERY_BATCH_BYTES,
    PROVISIONAL_MAX_PENDING_BYTES,
)
from custom_components.hass_questdb_writer.worker import (
    WorkerStartError,
    WorkerStartTimeoutError,
)


class FakeConfig:
    def __init__(self, root: Path) -> None:
        self.root = root

    def path(self, *parts: str) -> str:
        return str(self.root.joinpath(*parts))


class ConfigEntrySetupTests(unittest.IsolatedAsyncioTestCase):
    def entry(self) -> SimpleNamespace:
        return SimpleNamespace(
            entry_id="entry-1",
            data={
                CONF_HOST: "questdb",
                CONF_PORT: 9000,
                CONF_TABLE: "ha_events",
                CONF_USE_TLS: False,
            },
            options={},
            runtime_data=None,
            async_on_unload=lambda _callback: None,
            add_update_listener=lambda _listener: lambda: None,
        )

    def test_maps_entry_to_explicit_provisional_profile(self) -> None:
        hass = SimpleNamespace(config=FakeConfig(Path("/config")))
        configuration = _runtime_configuration(  # type: ignore[arg-type]
            hass, self.entry()
        )
        self.assertEqual(configuration.connection.host, "questdb")
        self.assertEqual(configuration.table, "ha_events")
        self.assertEqual(
            configuration.spool.path,
            Path("/config/.storage/hass_questdb_writer/entry-1.db"),
        )
        self.assertEqual(
            configuration.spool.max_pending_bytes,
            PROVISIONAL_MAX_PENDING_BYTES,
        )
        self.assertEqual(
            configuration.worker.delivery_batch_bytes,
            PROVISIONAL_DELIVERY_BATCH_BYTES,
        )

    async def test_setup_starts_before_publishing_runtime_data(self) -> None:
        hass = SimpleNamespace(
            config=FakeConfig(Path("/config")),
            config_entries=Mock(
                async_forward_entry_setups=AsyncMock(),
                async_unload_platforms=AsyncMock(),
            ),
        )
        entry = self.entry()
        runtime = Mock()
        runtime.async_start = AsyncMock()
        with patch(
            "custom_components.hass_questdb_writer.HassQuestDbRuntime",
            return_value=runtime,
        ):
            self.assertTrue(
                await async_setup_entry(hass, entry)  # type: ignore[arg-type]
            )
        runtime.async_start.assert_awaited_once_with()
        self.assertIs(entry.runtime_data, runtime)

    async def test_failed_setup_does_not_publish_runtime_data(self) -> None:
        hass = SimpleNamespace(config=FakeConfig(Path("/config")))
        entry = self.entry()
        runtime = Mock()
        runtime.async_start = AsyncMock(side_effect=RuntimeError("failed"))
        with patch(
            "custom_components.hass_questdb_writer.HassQuestDbRuntime",
            return_value=runtime,
        ):
            with self.assertRaises(RuntimeError):
                await async_setup_entry(hass, entry)  # type: ignore[arg-type]
        self.assertIsNone(entry.runtime_data)

    async def test_unload_returns_worker_stop_result(self) -> None:
        hass = SimpleNamespace()
        runtime = Mock()
        runtime.async_stop = AsyncMock(return_value=False)
        entry = self.entry()
        entry.runtime_data = runtime
        self.assertFalse(
            await async_unload_entry(hass, entry)  # type: ignore[arg-type]
        )
        runtime.async_stop.assert_awaited_once_with()

    async def setup_failing_with(self, error: BaseException) -> BaseException:
        """Run setup with a runtime whose start raises ``error``."""
        hass = SimpleNamespace(config=FakeConfig(Path("/config")))
        entry = self.entry()
        runtime = Mock()
        runtime.async_start = AsyncMock(side_effect=error)
        with patch(
            "custom_components.hass_questdb_writer.HassQuestDbRuntime",
            return_value=runtime,
        ):
            with self.assertRaises(ConfigEntryNotReady) as caught:
                await async_setup_entry(hass, entry)  # type: ignore[arg-type]
        self.assertIsNone(entry.runtime_data)
        raised = caught.exception
        assert raised is not None
        return raised

    async def test_worker_start_error_becomes_not_ready(self) -> None:
        """A worker that cannot start is transient, so HA must retry later."""
        failure = WorkerStartError("spool could not be opened")
        caught = await self.setup_failing_with(failure)
        self.assertIs(caught.__cause__, failure)

    async def test_worker_start_timeout_becomes_not_ready(self) -> None:
        failure = WorkerStartTimeoutError("worker initialization timed out")
        caught = await self.setup_failing_with(failure)
        self.assertIs(caught.__cause__, failure)


if __name__ == "__main__":
    unittest.main()

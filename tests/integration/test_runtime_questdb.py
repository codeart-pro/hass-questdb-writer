"""End-to-end Home Assistant event-bus delivery to real QuestDB."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request

from homeassistant import bootstrap, loader
from homeassistant.config_entries import ConfigEntry, ConfigEntryState, SOURCE_USER
from homeassistant.core import HomeAssistant

from custom_components.hass_questdb_writer.const import (
    CONF_HOST,
    CONF_PORT,
    CONF_TABLE,
    CONF_USE_TLS,
    DOMAIN,
)
from custom_components.hass_questdb_writer.runtime import (
    ConnectionConfiguration,
    HassQuestDbRuntime,
    RuntimeConfiguration,
    SpoolConfiguration,
)
from custom_components.hass_questdb_writer.schema import validate_table_columns
from custom_components.hass_questdb_writer.worker import WorkerSettings, WorkerState


class RuntimeQuestDbIntegrationTests(unittest.IsolatedAsyncioTestCase):
    host = os.environ.get("QUESTDB_HTTP_HOST", "questdb")
    port = int(os.environ.get("QUESTDB_HTTP_PORT", "9000"))
    table = "hass_qdb_writer_runtime_integration"

    def sql(self, statement: str) -> dict:
        query = urllib.parse.urlencode({"query": statement})
        with urllib.request.urlopen(
            f"http://{self.host}:{self.port}/exec?{query}", timeout=10
        ) as response:
            return json.load(response)

    def sql_maybe(self, statement: str) -> dict | None:
        """Run a statement, treating a missing-table rejection as None."""
        try:
            return self.sql(statement)
        except urllib.error.HTTPError:
            return None

    async def asyncSetUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addAsyncCleanup(self._cleanup_temporary_directory)
        component_source = Path(__file__).parents[2] / "custom_components"
        Path(self.temporary_directory.name, "custom_components").symlink_to(
            component_source,
            target_is_directory=True,
        )
        # The table is owned by the integration: the runtime's schema manager
        # creates and validates it when the worker starts.
        self.sql(f"drop table if exists {self.table}")
        self.addAsyncCleanup(self._drop_table)
        self.hass = HomeAssistant(self.temporary_directory.name)
        loader.async_setup(self.hass)
        initialized = await bootstrap.async_from_config_dict({}, self.hass)
        self.assertIs(initialized, self.hass)
        await self.hass.async_start()
        self.addAsyncCleanup(self.hass.async_stop, force=True)

    async def _cleanup_temporary_directory(self) -> None:
        self.temporary_directory.cleanup()

    async def _drop_table(self) -> None:
        self.sql(f"drop table if exists {self.table}")

    async def _unload_entry(self, entry: ConfigEntry) -> None:
        if entry.state is ConfigEntryState.LOADED:
            await self.hass.config_entries.async_unload(entry.entry_id)

    def configuration(self) -> RuntimeConfiguration:
        return RuntimeConfiguration(
            table=self.table,
            spool=SpoolConfiguration(
                path=Path(self.temporary_directory.name) / "runtime.db",
                max_pending_rows=100,
                max_pending_bytes=1_000_000,
                max_event_bytes=100_000,
                max_dead_letter_rows=10,
                max_dead_letter_bytes=1_000_000,
                busy_timeout_seconds=1,
            ),
            connection=ConnectionConfiguration(
                host=self.host,
                port=self.port,
                use_tls=False,
                timeout_seconds=2,
            ),
            worker=WorkerSettings(
                ingress_queue_capacity=10,
                max_serialized_event_bytes=100_000,
                persist_batch_rows=10,
                delivery_batch_rows=1,
                delivery_batch_bytes=100_000,
                flush_interval_seconds=0.05,
                retry_initial_seconds=0.05,
                retry_max_seconds=0.2,
                retry_multiplier=2,
                retry_jitter_ratio=0,
                flush_on_shutdown=False,
            ),
            start_timeout_seconds=2,
            stop_timeout_seconds=3,
            tracked_entity_ids=None,
        )

    async def test_real_state_change_reaches_questdb(self) -> None:
        runtime = HassQuestDbRuntime(self.hass, self.configuration())
        await runtime.async_start()
        self.addAsyncCleanup(runtime.async_stop)

        self.hass.states.async_set(
            "input_boolean.questdb_test",
            "on",
            {"source": "real-home-assistant-bus"},
        )
        await self.hass.async_block_till_done()

        deadline = time.monotonic() + 5
        while True:
            result = self.sql_maybe(
                f"select entity_id, domain, state, attributes "
                f"from {self.table}"
            )
            if (result or {}).get("count") == 1 or time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.01)

        assert result is not None
        self.assertEqual(result["count"], 1)
        row = result["dataset"][0]
        self.assertEqual(row[0], "input_boolean.questdb_test")
        self.assertEqual(row[1], "input_boolean")
        self.assertEqual(row[2], "on")
        self.assertEqual(
            json.loads(row[3]),
            {"source": "real-home-assistant-bus"},
        )
        snapshot = runtime.snapshot()
        self.assertEqual(snapshot.state_events_seen, 1)
        self.assertEqual(snapshot.state_events_accepted, 1)
        self.assertEqual(snapshot.worker.delivered_events, 1)
        self.assertEqual(snapshot.worker.state, WorkerState.RUNNING)

        # The worker owns the table: it created it with the declared
        # designated timestamp and dedup upsert keys.
        columns = self.sql(f"SHOW COLUMNS FROM {self.table}")
        validate_table_columns(columns["dataset"], self.table)

    async def test_config_flow_setup_reload_event_and_unload(self) -> None:
        form = await self.hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_USER},
        )
        self.assertEqual(form["type"], "form")
        self.assertEqual(form["step_id"], "user")
        result = await self.hass.config_entries.flow.async_configure(
            form["flow_id"],
            {
                CONF_HOST: self.host,
                CONF_PORT: self.port,
                CONF_TABLE: self.table,
                CONF_USE_TLS: False,
            },
        )
        self.assertEqual(result["type"], "create_entry")
        entry = result["result"]
        self.assertIsInstance(entry, ConfigEntry)
        self.addAsyncCleanup(self._unload_entry, entry)
        await self.hass.async_block_till_done()
        self.assertEqual(entry.state, ConfigEntryState.LOADED)
        first_runtime = entry.runtime_data

        self.hass.states.async_set("input_boolean.questdb_test", "on")
        await self.hass.async_block_till_done()
        deadline = time.monotonic() + 5
        while True:
            result = self.sql_maybe(
                f"select entity_id, state from {self.table} "
                "where entity_id = 'input_boolean.questdb_test'"
            )
            if (result or {}).get("count") == 1 or time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.01)

        assert result is not None
        self.assertEqual(
            result["dataset"],
            [["input_boolean.questdb_test", "on"]],
        )

        self.assertTrue(
            await self.hass.config_entries.async_reload(entry.entry_id)
        )
        self.assertEqual(entry.state, ConfigEntryState.LOADED)
        self.assertIsNot(entry.runtime_data, first_runtime)
        self.assertFalse(first_runtime.snapshot().listener_active)
        self.assertEqual(
            first_runtime.snapshot().worker.state,
            WorkerState.STOPPED,
        )

        self.hass.states.async_set("input_boolean.questdb_test", "off")
        await self.hass.async_block_till_done()
        deadline = time.monotonic() + 5
        while True:
            result = self.sql_maybe(
                f"select entity_id, state from {self.table} "
                "where entity_id = 'input_boolean.questdb_test' "
                "order by last_updated"
            )
            if ((result or {}).get("count") or 0) >= 2 or time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.01)
        assert result is not None
        self.assertEqual(
            result["dataset"],
            [
                ["input_boolean.questdb_test", "on"],
                ["input_boolean.questdb_test", "off"],
            ],
        )

        self.assertTrue(
            await self.hass.config_entries.async_unload(entry.entry_id)
        )
        self.assertEqual(entry.state, ConfigEntryState.NOT_LOADED)

    async def test_options_flow_entity_filter_is_applied_after_reload(self) -> None:
        form = await self.hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": SOURCE_USER},
        )
        result = await self.hass.config_entries.flow.async_configure(
            form["flow_id"],
            {
                CONF_HOST: self.host,
                CONF_PORT: self.port,
                CONF_TABLE: self.table,
                CONF_USE_TLS: False,
            },
        )
        entry = result["result"]
        self.addAsyncCleanup(self._unload_entry, entry)
        await self.hass.async_block_till_done()
        first_runtime = entry.runtime_data

        options_form = await self.hass.config_entries.options.async_init(
            entry.entry_id
        )
        result = await self.hass.config_entries.options.async_configure(
            options_form["flow_id"],
            {
                "include_entities": ["input_boolean.questdb_test"],
                "exclude_entities": [],
                "include_domains": "",
                "exclude_domains": "",
                "include_entity_globs": "",
                "exclude_entity_globs": "",
                "show_advanced": False,
            },
        )
        self.assertEqual(result["type"], "create_entry")

        reload_deadline = time.monotonic() + 5
        while getattr(entry, "runtime_data", None) is first_runtime:
            if time.monotonic() >= reload_deadline:
                self.fail("options change did not reload the entry")
            await asyncio.sleep(0.01)

        self.hass.states.async_set("input_boolean.questdb_test", "on")
        self.hass.states.async_set("sensor.filtered_out", "42")
        await self.hass.async_block_till_done()

        deadline = time.monotonic() + 5
        while True:
            result = self.sql_maybe(
                f"select entity_id, state from {self.table} "
                f"where entity_id in "
                f"('input_boolean.questdb_test', 'sensor.filtered_out')"
            )
            if (result or {}).get("count") == 1 or time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.01)
        assert result is not None
        self.assertEqual(
            result["dataset"],
            [["input_boolean.questdb_test", "on"]],
        )
        snapshot = entry.runtime_data.snapshot()
        self.assertEqual(snapshot.state_events_excluded, 1)


if __name__ == "__main__":
    unittest.main()

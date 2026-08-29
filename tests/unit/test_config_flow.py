"""Tests for the HASS QuestDB Writer config and options flows."""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock, patch
import unittest

from custom_components.hass_questdb_writer.config_flow import (
    HassQuestDbWriterConfigFlow,
    HassQuestDbWriterOptionsFlow,
)
from custom_components.hass_questdb_writer.const import (
    CONF_DELIVERY_BATCH_ROWS,
    CONF_EXCLUDE,
    CONF_FLUSH_ON_SHUTDOWN,
    CONF_HOST,
    CONF_INCLUDE,
    CONF_MAX_DEAD_LETTER_BYTES,
    CONF_MAX_SERIALIZED_EVENT_BYTES,
    CONF_PASSWORD,
    CONF_PORT,
    CONF_RETRY_INITIAL_SECONDS,
    CONF_RETRY_MAX_SECONDS,
    CONF_SHOW_ADVANCED,
    CONF_TABLE,
    CONF_USE_TLS,
    CONF_USERNAME,
    DEFAULT_PORT,
    DEFAULT_TABLE,
)


class ConfigFlowTests(unittest.IsolatedAsyncioTestCase):
    def user_input(self, overrides: dict | None = None) -> dict[str, object]:
        values: dict[str, object] = {
            CONF_HOST: "questdb",
            CONF_PORT: 9000,
            CONF_TABLE: "events",
            CONF_USE_TLS: False,
            CONF_USERNAME: "",
            CONF_PASSWORD: "",
        }
        values.update(overrides or {})
        return values

    async def test_initial_form_has_explicit_local_defaults(self) -> None:
        flow = HassQuestDbWriterConfigFlow()
        result = await flow.async_step_user()
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "user")
        self.assertEqual(
            result["data_schema"](self.user_input()),
            self.user_input(),
        )

    async def test_invalid_input_is_rejected_and_preserved(self) -> None:
        flow = HassQuestDbWriterConfigFlow()
        user_input = self.user_input({CONF_HOST: "   "})
        result = await flow.async_step_user(user_input)
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["errors"], {"base": "invalid_connection"})
        self.assertEqual(
            result["data_schema"]({}),
            user_input,
        )

    async def test_table_limit_counts_utf8_bytes(self) -> None:
        flow = HassQuestDbWriterConfigFlow()
        result = await flow.async_step_user(
            self.user_input({CONF_TABLE: "я" * 64})
        )
        self.assertEqual(result["errors"], {"base": "invalid_connection"})

    async def test_auth_requires_both_fields(self) -> None:
        flow = HassQuestDbWriterConfigFlow()
        result = await flow.async_step_user(
            self.user_input({CONF_USERNAME: "user"})
        )
        self.assertEqual(result["errors"], {"base": "invalid_auth_pair"})
        with patch.object(flow, "async_set_unique_id", AsyncMock()):
            result = await flow.async_step_user(
                self.user_input({CONF_USERNAME: "user", CONF_PASSWORD: "pass"})
            )
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"][CONF_USERNAME], "user")
        self.assertEqual(result["data"][CONF_PASSWORD], "pass")

    async def test_empty_auth_is_stored_as_none(self) -> None:
        flow = HassQuestDbWriterConfigFlow()
        with (
            patch.object(flow, "async_set_unique_id", AsyncMock()),
            patch.object(flow, "_abort_if_unique_id_configured", Mock()),
        ):
            result = await flow.async_step_user(self.user_input())
        self.assertEqual(result["data"][CONF_USERNAME], None)
        self.assertEqual(result["data"][CONF_PASSWORD], None)

    async def test_valid_input_is_trimmed_and_uniquely_identified(self) -> None:
        flow = HassQuestDbWriterConfigFlow()
        set_unique_id = AsyncMock()
        abort_if_configured = Mock()
        with (
            patch.object(flow, "async_set_unique_id", set_unique_id),
            patch.object(
                flow,
                "_abort_if_unique_id_configured",
                abort_if_configured,
            ),
        ):
            result = await flow.async_step_user(
                self.user_input(
                    {CONF_HOST: " QuestDB ", CONF_TABLE: " events "}
                )
            )
        set_unique_id.assert_awaited_once_with("http://questdb:9000/events")
        abort_if_configured.assert_called_once_with()
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["title"], "QuestDB at QuestDB")
        self.assertEqual(result["data"][CONF_HOST], "QuestDB")
        self.assertEqual(result["data"][CONF_TABLE], "events")


class OptionsFlowTests(unittest.IsolatedAsyncioTestCase):
    def flow(self, options: dict | None = None) -> HassQuestDbWriterOptionsFlow:
        entry = Mock(options=options or {})
        return HassQuestDbWriterOptionsFlow(entry)

    def filter_input(self, overrides: dict | None = None) -> dict[str, object]:
        values: dict[str, object] = {
            "include_entities": ["sensor.kitchen"],
            "exclude_entities": [],
            "include_domains": "",
            "exclude_domains": "sensor, binary_sensor",
            "include_entity_globs": "sensor.garden_*",
            "exclude_entity_globs": "",
            CONF_SHOW_ADVANCED: False,
        }
        values.update(overrides or {})
        return values

    async def test_init_step_builds_include_exclude_filter(self) -> None:
        flow = self.flow()
        result = await flow.async_step_init()
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "init")

        result = await flow.async_step_init(self.filter_input())
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(
            result["data"][CONF_INCLUDE],
            {
                "domains": [],
                "entity_globs": ["sensor.garden_*"],
                "entities": ["sensor.kitchen"],
            },
        )
        self.assertEqual(
            result["data"][CONF_EXCLUDE],
            {
                "domains": ["sensor", "binary_sensor"],
                "entity_globs": [],
                "entities": [],
            },
        )

    async def test_init_step_carries_filter_into_advanced_step(self) -> None:
        flow = self.flow()
        result = await flow.async_step_init(
            self.filter_input({CONF_SHOW_ADVANCED: True})
        )
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "advanced")
        self.assertIn(CONF_DELIVERY_BATCH_ROWS, result["data_schema"].schema)

        result = await flow.async_step_advanced(
            {CONF_DELIVERY_BATCH_ROWS: 7, CONF_FLUSH_ON_SHUTDOWN: True}
        )
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["data"][CONF_DELIVERY_BATCH_ROWS], 7)
        self.assertTrue(result["data"][CONF_FLUSH_ON_SHUTDOWN])
        self.assertEqual(
            result["data"][CONF_INCLUDE]["entities"], ["sensor.kitchen"]
        )

    async def test_advanced_rejects_retry_bounds_inversion(self) -> None:
        flow = self.flow()
        await flow.async_step_init(
            self.filter_input({CONF_SHOW_ADVANCED: True})
        )
        result = await flow.async_step_advanced(
            {
                CONF_RETRY_INITIAL_SECONDS: 30,
                CONF_RETRY_MAX_SECONDS: 1,
            }
        )
        self.assertEqual(result["errors"], {"base": "invalid_retry_bounds"})

    async def test_advanced_rejects_event_larger_than_dead_letter(self) -> None:
        flow = self.flow()
        await flow.async_step_init(
            self.filter_input({CONF_SHOW_ADVANCED: True})
        )
        result = await flow.async_step_advanced(
            {
                CONF_MAX_SERIALIZED_EVENT_BYTES: 1_048_576,
                CONF_MAX_DEAD_LETTER_BYTES: 65_536,
            }
        )
        self.assertEqual(result["errors"], {"base": "invalid_size_bounds"})

    async def test_advanced_schema_preserves_entered_values_on_error(self) -> None:
        flow = self.flow()
        await flow.async_step_init(
            self.filter_input({CONF_SHOW_ADVANCED: True})
        )
        user_input = {
            CONF_RETRY_INITIAL_SECONDS: 30,
            CONF_RETRY_MAX_SECONDS: 1,
        }
        result = await flow.async_step_advanced(user_input)
        schema_values = result["data_schema"]({})
        self.assertEqual(schema_values[CONF_RETRY_INITIAL_SECONDS], 30)
        self.assertEqual(schema_values[CONF_RETRY_MAX_SECONDS], 1)


if __name__ == "__main__":
    unittest.main()

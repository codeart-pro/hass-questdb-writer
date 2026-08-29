"""Tests for the HASS QuestDB Writer config flow."""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock, patch
import unittest

from custom_components.hass_questdb_writer.config_flow import (
    HassQuestDbWriterConfigFlow,
)
from custom_components.hass_questdb_writer.const import (
    CONF_HOST,
    CONF_PORT,
    CONF_TABLE,
    CONF_USE_TLS,
    DEFAULT_PORT,
    DEFAULT_TABLE,
)


class ConfigFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_initial_form_has_explicit_local_defaults(self) -> None:
        flow = HassQuestDbWriterConfigFlow()
        result = await flow.async_step_user()
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "user")
        self.assertEqual(
            result["data_schema"](
                {
                    CONF_HOST: "questdb",
                    CONF_PORT: DEFAULT_PORT,
                    CONF_TABLE: DEFAULT_TABLE,
                    CONF_USE_TLS: False,
                }
            ),
            {
                CONF_HOST: "questdb",
                CONF_PORT: 9000,
                CONF_TABLE: DEFAULT_TABLE,
                CONF_USE_TLS: False,
            },
        )

    async def test_invalid_input_is_rejected_and_preserved(self) -> None:
        flow = HassQuestDbWriterConfigFlow()
        user_input = {
            CONF_HOST: "   ",
            CONF_PORT: 9000,
            CONF_TABLE: "events",
            CONF_USE_TLS: False,
        }
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
            {
                CONF_HOST: "questdb",
                CONF_PORT: 9000,
                CONF_TABLE: "я" * 64,
                CONF_USE_TLS: False,
            }
        )
        self.assertEqual(result["errors"], {"base": "invalid_connection"})

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
                {
                    CONF_HOST: " QuestDB ",
                    CONF_PORT: 9000,
                    CONF_TABLE: " events ",
                    CONF_USE_TLS: False,
                }
            )
        set_unique_id.assert_awaited_once_with("http://questdb:9000/events")
        abort_if_configured.assert_called_once_with()
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(result["title"], "QuestDB at QuestDB")
        self.assertEqual(result["data"][CONF_HOST], "QuestDB")
        self.assertEqual(result["data"][CONF_TABLE], "events")


if __name__ == "__main__":
    unittest.main()

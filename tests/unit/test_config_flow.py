"""Tests for the HASS QuestDB Writer config and options flows."""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock, patch
import unittest

from homeassistant.config_entries import SOURCE_RECONFIGURE

from custom_components.hass_questdb_writer.config_flow import (
    HassQuestDbWriterConfigFlow,
    HassQuestDbWriterOptionsFlow,
)
from custom_components.hass_questdb_writer.transport import (
    AuthenticationIlpError,
    IlpTransportError,
)
from custom_components.hass_questdb_writer.const import (
    CONF_ATTRIBUTE_ALLOWLIST,
    CONF_ATTRIBUTE_DENYLIST,
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
        with (
            patch.object(flow, "async_set_unique_id", AsyncMock()),
            patch.object(flow, "_test_connection", AsyncMock()),
        ):
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
            patch.object(flow, "_test_connection", AsyncMock()),
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
            patch.object(flow, "_test_connection", AsyncMock()),
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

    def _reconfigure_flow(self, entry_data: dict) -> HassQuestDbWriterConfigFlow:
        entry = Mock(data=entry_data)
        flow = HassQuestDbWriterConfigFlow()
        flow.context = {"source": SOURCE_RECONFIGURE, "entry_id": "entry-1"}
        flow.hass = Mock(
            config_entries=Mock(
                async_get_known_entry=Mock(return_value=entry)
            )
        )
        return flow

    async def test_reconfigure_prefills_data_and_hides_secret(self) -> None:
        flow = self._reconfigure_flow(
            {
                CONF_HOST: "questdb",
                CONF_PORT: 9000,
                CONF_TABLE: "events",
                CONF_USE_TLS: False,
                CONF_USERNAME: "admin",
                CONF_PASSWORD: "secret",
            }
        )
        result = await flow.async_step_user()
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "user")
        defaults = result["data_schema"]({})
        self.assertEqual(defaults[CONF_HOST], "questdb")
        self.assertEqual(defaults[CONF_PORT], 9000)
        self.assertEqual(defaults[CONF_USERNAME], "admin")
        self.assertEqual(defaults[CONF_PASSWORD], "")

    async def test_reconfigure_updates_entry_and_keeps_stored_secret(self) -> None:
        flow = self._reconfigure_flow(
            {
                CONF_HOST: "questdb",
                CONF_PORT: 9000,
                CONF_TABLE: "events",
                CONF_USE_TLS: False,
                CONF_USERNAME: "admin",
                CONF_PASSWORD: "secret",
            }
        )
        update = Mock(return_value={"type": "abort", "reason": "reconfigure_successful"})
        with (
            patch.object(flow, "async_update_reload_and_abort", update),
            patch.object(flow, "_test_connection", AsyncMock()),
        ):
            result = await flow.async_step_user(
                {
                    CONF_HOST: "questdb2",
                    CONF_PORT: 9001,
                    CONF_TABLE: "events",
                    CONF_USE_TLS: True,
                    CONF_USERNAME: "",
                    CONF_PASSWORD: "",
                }
            )
        self.assertEqual(result["type"], "abort")
        update.assert_called_once()
        call = update.call_args
        self.assertEqual(call[0][0].data[CONF_PASSWORD], "secret")
        new_data = call.kwargs["data"]
        self.assertEqual(new_data[CONF_HOST], "questdb2")
        self.assertEqual(new_data[CONF_PORT], 9001)
        self.assertEqual(new_data[CONF_USE_TLS], True)
        self.assertEqual(new_data[CONF_USERNAME], "admin")
        self.assertEqual(new_data[CONF_PASSWORD], "secret")

    async def test_reconfigure_can_replace_credentials(self) -> None:
        flow = self._reconfigure_flow(
            {
                CONF_HOST: "questdb",
                CONF_PORT: 9000,
                CONF_TABLE: "events",
                CONF_USE_TLS: False,
                CONF_USERNAME: "admin",
                CONF_PASSWORD: "secret",
            }
        )
        update = Mock(return_value={"type": "abort"})
        with (
            patch.object(flow, "async_update_reload_and_abort", update),
            patch.object(flow, "_test_connection", AsyncMock()),
        ):
            result = await flow.async_step_user(
                {
                    CONF_HOST: "questdb",
                    CONF_PORT: 9000,
                    CONF_TABLE: "events",
                    CONF_USE_TLS: False,
                    CONF_USERNAME: "newuser",
                    CONF_PASSWORD: "newpass",
                }
            )
        self.assertEqual(result["type"], "abort")
        self.assertEqual(update.call_args.kwargs["data"][CONF_USERNAME], "newuser")
        self.assertEqual(update.call_args.kwargs["data"][CONF_PASSWORD], "newpass")

    async def test_reconfigure_rejects_auth_pair_without_stored_secret(self) -> None:
        flow = self._reconfigure_flow(
            {
                CONF_HOST: "questdb",
                CONF_PORT: 9000,
                CONF_TABLE: "events",
                CONF_USE_TLS: False,
            }
        )
        result = await flow.async_step_user(
            self.user_input({CONF_USERNAME: "user"})
        )
        self.assertEqual(result["errors"], {"base": "invalid_auth_pair"})

    async def test_unreachable_server_keeps_form_with_input(self) -> None:
        flow = HassQuestDbWriterConfigFlow()
        with patch.object(
            flow,
            "_test_connection",
            AsyncMock(side_effect=IlpTransportError("down", retryable=True, delivery_uncertain=False)),
        ):
            result = await flow.async_step_user(
                self.user_input({CONF_HOST: "nope"})
            )
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["errors"], {"base": "cannot_connect"})
        self.assertEqual(result["data_schema"]({})[CONF_HOST], "nope")

    async def test_rejected_credentials_keep_form(self) -> None:
        flow = HassQuestDbWriterConfigFlow()
        with patch.object(
            flow,
            "_test_connection",
            AsyncMock(
                side_effect=AuthenticationIlpError(
                    "401", retryable=False, delivery_uncertain=False, status_code=401
                )
            ),
        ):
            result = await flow.async_step_user(
                self.user_input({CONF_USERNAME: "user", CONF_PASSWORD: "bad"})
            )
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["errors"], {"base": "invalid_auth"})


class OptionsFlowTests(unittest.IsolatedAsyncioTestCase):
    def flow(self, options: dict | None = None) -> HassQuestDbWriterOptionsFlow:
        entry = Mock(options=options or {})
        return HassQuestDbWriterOptionsFlow(entry)

    async def _init(
        self, flow: HassQuestDbWriterOptionsFlow, user_input: dict | None = None
    ):
        with patch(
            "custom_components.hass_questdb_writer.config_flow._domain_selector_options",
            AsyncMock(return_value=[]),
        ):
            return await flow.async_step_init(user_input)

    def filter_input(self, overrides: dict | None = None) -> dict[str, object]:
        values: dict[str, object] = {
            "include_entities": ["sensor.kitchen"],
            "exclude_entities": [],
            "include_domains": "",
            "exclude_domains": "sensor, binary_sensor",
            "include_entity_globs": "sensor.garden_*",
            "exclude_entity_globs": "",
            CONF_ATTRIBUTE_ALLOWLIST: "",
            CONF_ATTRIBUTE_DENYLIST: "",
            CONF_SHOW_ADVANCED: False,
        }
        values.update(overrides or {})
        return values

    async def test_init_step_builds_include_exclude_filter(self) -> None:
        flow = self.flow()
        result = await self._init(flow)
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["step_id"], "init")
        self.assertIn(CONF_ATTRIBUTE_ALLOWLIST, result["data_schema"].schema)
        self.assertIn(CONF_ATTRIBUTE_DENYLIST, result["data_schema"].schema)

        result = await self._init(flow, self.filter_input())
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
        self.assertEqual(result["data"][CONF_ATTRIBUTE_ALLOWLIST], [])
        self.assertEqual(result["data"][CONF_ATTRIBUTE_DENYLIST], [])

    async def test_init_step_stores_attribute_pattern_lists(self) -> None:
        flow = self.flow()
        result = await self._init(
            flow,
            self.filter_input(
                {
                    CONF_ATTRIBUTE_ALLOWLIST: "friendly_name, unit_*",
                    CONF_ATTRIBUTE_DENYLIST: "rssi, linkquality",
                }
            ),
        )
        self.assertEqual(result["type"], "create_entry")
        self.assertEqual(
            result["data"][CONF_ATTRIBUTE_ALLOWLIST],
            ["friendly_name", "unit_*"],
        )
        self.assertEqual(
            result["data"][CONF_ATTRIBUTE_DENYLIST],
            ["rssi", "linkquality"],
        )

    async def test_init_step_carries_filter_into_advanced_step(self) -> None:
        flow = self.flow()
        result = await self._init(
            flow, self.filter_input({CONF_SHOW_ADVANCED: True})
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

    async def test_overlapping_entities_are_rejected(self) -> None:
        flow = self.flow()
        result = await self._init(
            flow,
            self.filter_input(
                {"exclude_entities": ["sensor.kitchen"]}
            ),
        )
        self.assertEqual(result["type"], "form")
        self.assertEqual(result["errors"], {"base": "overlapping_filters"})
        self.assertIn("entities: sensor.kitchen", result["description_placeholders"]["conflicts"])

    async def test_overlapping_attributes_are_rejected(self) -> None:
        flow = self.flow()
        result = await self._init(
            flow,
            self.filter_input(
                {
                    CONF_ATTRIBUTE_ALLOWLIST: "friendly_name, rssi",
                    CONF_ATTRIBUTE_DENYLIST: "rssi, linkquality",
                }
            ),
        )
        self.assertEqual(result["errors"], {"base": "overlapping_filters"})
        self.assertIn("attributes: rssi", result["description_placeholders"]["conflicts"])

    async def test_overlapping_domains_and_globs_are_rejected(self) -> None:
        flow = self.flow()
        result = await self._init(
            flow,
            self.filter_input(
                {
                    "include_domains": "sensor",
                    "exclude_domains": "sensor, binary_sensor",
                    "exclude_entity_globs": "sensor.garden_*",
                }
            ),
        )
        self.assertEqual(result["errors"], {"base": "overlapping_filters"})
        conflicts = result["description_placeholders"]["conflicts"]
        self.assertIn("domains: sensor", conflicts)
        self.assertIn("globs: sensor.garden_*", conflicts)

    async def test_identical_items_in_include_and_exclude_entities(self) -> None:
        flow = self.flow()
        result = await self._init(
            flow,
            self.filter_input(
                {
                    "include_entities": ["sensor.kitchen", "sensor.garden"],
                    "exclude_entities": ["sensor.kitchen"],
                }
            ),
        )
        self.assertEqual(result["errors"], {"base": "overlapping_filters"})
        conflicts = result["description_placeholders"]["conflicts"]
        self.assertIn("sensor.kitchen", conflicts)
        self.assertNotIn("sensor.garden", conflicts)

    async def test_advanced_rejects_retry_bounds_inversion(self) -> None:
        flow = self.flow()
        await self._init(flow, self.filter_input({CONF_SHOW_ADVANCED: True}))
        result = await flow.async_step_advanced(
            {
                CONF_RETRY_INITIAL_SECONDS: 30,
                CONF_RETRY_MAX_SECONDS: 1,
            }
        )
        self.assertEqual(result["errors"], {"base": "invalid_retry_bounds"})

    async def test_advanced_rejects_event_larger_than_dead_letter(self) -> None:
        flow = self.flow()
        await self._init(flow, self.filter_input({CONF_SHOW_ADVANCED: True}))
        result = await flow.async_step_advanced(
            {
                CONF_MAX_SERIALIZED_EVENT_BYTES: 1_048_576,
                CONF_MAX_DEAD_LETTER_BYTES: 65_536,
            }
        )
        self.assertEqual(result["errors"], {"base": "invalid_size_bounds"})

    async def test_advanced_schema_preserves_entered_values_on_error(self) -> None:
        flow = self.flow()
        await self._init(flow, self.filter_input({CONF_SHOW_ADVANCED: True}))
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

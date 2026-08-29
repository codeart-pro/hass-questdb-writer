"""Config flow for HASS QuestDB Writer."""

from __future__ import annotations

from typing import Any

import voluptuous as vol

from homeassistant import config_entries

from .const import (
    CONF_HOST,
    CONF_PORT,
    CONF_TABLE,
    CONF_USE_TLS,
    DEFAULT_PORT,
    DEFAULT_TABLE,
    DOMAIN,
)


class HassQuestDbWriterConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Configure HASS QuestDB Writer."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Handle the initial configuration step."""
        if user_input is not None:
            host = user_input[CONF_HOST].strip()
            table = user_input[CONF_TABLE].strip()
            if not host or not table or len(table.encode("utf-8")) > 127:
                return self.async_show_form(
                    step_id="user",
                    data_schema=self._schema(user_input),
                    errors={"base": "invalid_connection"},
                )
            scheme = "https" if user_input[CONF_USE_TLS] else "http"
            await self.async_set_unique_id(
                f"{scheme}://{host.lower()}:{user_input[CONF_PORT]}/{table}"
            )
            self._abort_if_unique_id_configured()
            return self.async_create_entry(
                title=f"QuestDB at {host}",
                data={
                    **user_input,
                    CONF_HOST: host,
                    CONF_TABLE: table,
                },
            )

        return self.async_show_form(step_id="user", data_schema=self._schema())

    def _schema(self, values: dict[str, Any] | None = None) -> vol.Schema:
        values = values or {}
        return vol.Schema(
            {
                vol.Required(CONF_HOST, default=values.get(CONF_HOST, "")): str,
                vol.Required(
                    CONF_PORT, default=values.get(CONF_PORT, DEFAULT_PORT)
                ): vol.All(
                    vol.Coerce(int), vol.Range(min=1, max=65535)
                ),
                vol.Required(
                    CONF_TABLE, default=values.get(CONF_TABLE, DEFAULT_TABLE)
                ): str,
                vol.Required(
                    CONF_USE_TLS, default=values.get(CONF_USE_TLS, False)
                ): bool,
            }
        )

"""Config flow for HASS QuestDB Writer."""

from __future__ import annotations

from typing import Any

import voluptuous as vol

from homeassistant import config_entries

from .const import DEFAULT_PORT, DEFAULT_TABLE, DOMAIN

CONF_HOST = "host"
CONF_PORT = "port"
CONF_TABLE = "table"
CONF_USE_TLS = "use_tls"


class HassQuestDbWriterConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Configure HASS QuestDB Writer."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Handle the initial configuration step."""
        if user_input is not None:
            host = user_input[CONF_HOST].strip()
            await self.async_set_unique_id(f"{host}:{user_input[CONF_PORT]}")
            self._abort_if_unique_id_configured()
            return self.async_create_entry(
                title=f"QuestDB at {host}",
                data={**user_input, CONF_HOST: host},
            )

        schema = vol.Schema(
            {
                vol.Required(CONF_HOST): str,
                vol.Required(CONF_PORT, default=DEFAULT_PORT): vol.All(
                    vol.Coerce(int), vol.Range(min=1, max=65535)
                ),
                vol.Required(CONF_TABLE, default=DEFAULT_TABLE): str,
                vol.Required(CONF_USE_TLS, default=False): bool,
            }
        )
        return self.async_show_form(step_id="user", data_schema=schema)

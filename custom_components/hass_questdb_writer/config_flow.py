"""Config and options flows for HASS QuestDB Writer."""

from __future__ import annotations

from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import (
    CONF_DOMAINS,
    CONF_ENTITIES,
    CONF_EXCLUDE,
    CONF_INCLUDE,
)
from homeassistant.helpers import selector
from homeassistant.helpers.entityfilter import (
    CONF_EXCLUDE_DOMAINS,
    CONF_EXCLUDE_ENTITIES,
    CONF_EXCLUDE_ENTITY_GLOBS,
    CONF_ENTITY_GLOBS,
    CONF_INCLUDE_DOMAINS,
    CONF_INCLUDE_ENTITIES,
    CONF_INCLUDE_ENTITY_GLOBS,
    INCLUDE_EXCLUDE_BASE_FILTER_SCHEMA,
)

from .const import (
    CONF_ATTRIBUTE_ALLOWLIST,
    CONF_ATTRIBUTE_DENYLIST,
    CONF_DELIVERY_BATCH_BYTES,
    CONF_DELIVERY_BATCH_ROWS,
    CONF_FLUSH_INTERVAL_SECONDS,
    CONF_FLUSH_ON_SHUTDOWN,
    CONF_HOST,
    CONF_HTTP_TIMEOUT_SECONDS,
    CONF_INGRESS_QUEUE_CAPACITY,
    CONF_MAX_DEAD_LETTER_BYTES,
    CONF_MAX_DEAD_LETTER_ROWS,
    CONF_MAX_PENDING_BYTES,
    CONF_MAX_PENDING_ROWS,
    CONF_MAX_SERIALIZED_EVENT_BYTES,
    CONF_PASSWORD,
    CONF_PERSIST_BATCH_ROWS,
    CONF_PORT,
    CONF_RETRY_INITIAL_SECONDS,
    CONF_RETRY_JITTER_RATIO,
    CONF_RETRY_MAX_SECONDS,
    CONF_RETRY_MULTIPLIER,
    CONF_SHOW_ADVANCED,
    CONF_SQLITE_BUSY_TIMEOUT_SECONDS,
    CONF_START_TIMEOUT_SECONDS,
    CONF_STOP_TIMEOUT_SECONDS,
    CONF_TABLE,
    CONF_USE_TLS,
    CONF_USERNAME,
    DEFAULT_PORT,
    DEFAULT_TABLE,
    DOMAIN,
    PROVISIONAL_DELIVERY_BATCH_BYTES,
    PROVISIONAL_DELIVERY_BATCH_ROWS,
    PROVISIONAL_FLUSH_INTERVAL_SECONDS,
    PROVISIONAL_FLUSH_ON_SHUTDOWN,
    PROVISIONAL_HTTP_TIMEOUT_SECONDS,
    PROVISIONAL_INGRESS_QUEUE_CAPACITY,
    PROVISIONAL_MAX_DEAD_LETTER_BYTES,
    PROVISIONAL_MAX_DEAD_LETTER_ROWS,
    PROVISIONAL_MAX_PENDING_BYTES,
    PROVISIONAL_MAX_PENDING_ROWS,
    PROVISIONAL_MAX_SERIALIZED_EVENT_BYTES,
    PROVISIONAL_PERSIST_BATCH_ROWS,
    PROVISIONAL_RETRY_INITIAL_SECONDS,
    PROVISIONAL_RETRY_JITTER_RATIO,
    PROVISIONAL_RETRY_MAX_SECONDS,
    PROVISIONAL_RETRY_MULTIPLIER,
    PROVISIONAL_SQLITE_BUSY_TIMEOUT_SECONDS,
    PROVISIONAL_START_TIMEOUT_SECONDS,
    PROVISIONAL_STOP_TIMEOUT_SECONDS,
)


def _split_csv(value: str) -> list[str]:
    """Split a comma-separated input into stripped non-empty parts."""
    return [part.strip() for part in value.split(",") if part.strip()]


def _number(
    minimum: float,
    maximum: float,
    step: float,
) -> selector.NumberSelector:
    return selector.NumberSelector(
        selector.NumberSelectorConfig(
            mode=selector.NumberSelectorMode.BOX,
            min=minimum,
            max=maximum,
            step=step,
        )
    )


def _user_schema(values: dict[str, Any]) -> vol.Schema:
    return vol.Schema(
        {
            vol.Required(CONF_HOST, default=values.get(CONF_HOST, "")): str,
            vol.Required(
                CONF_PORT, default=values.get(CONF_PORT, DEFAULT_PORT)
            ): vol.All(vol.Coerce(int), vol.Range(min=1, max=65535)),
            vol.Required(
                CONF_TABLE, default=values.get(CONF_TABLE, DEFAULT_TABLE)
            ): str,
            vol.Required(
                CONF_USE_TLS, default=values.get(CONF_USE_TLS, False)
            ): bool,
            vol.Optional(
                CONF_USERNAME, default=values.get(CONF_USERNAME, "")
            ): str,
            vol.Optional(
                CONF_PASSWORD, default=values.get(CONF_PASSWORD, "")
            ): str,
        }
    )


def _filter_side(user_input: dict[str, Any], include: bool) -> dict[str, Any]:
    """Assemble one include/exclude side in the entity-filter schema shape."""
    if include:
        entities = user_input.get(CONF_INCLUDE_ENTITIES, [])
        domains = user_input.get(CONF_INCLUDE_DOMAINS, "")
        globs = user_input.get(CONF_INCLUDE_ENTITY_GLOBS, "")
    else:
        entities = user_input.get(CONF_EXCLUDE_ENTITIES, [])
        domains = user_input.get(CONF_EXCLUDE_DOMAINS, "")
        globs = user_input.get(CONF_EXCLUDE_ENTITY_GLOBS, "")
    return {
        CONF_DOMAINS: _split_csv(domains),
        CONF_ENTITY_GLOBS: _split_csv(globs),
        CONF_ENTITIES: list(entities),
    }


def _init_schema(options: dict[str, Any]) -> vol.Schema:
    include = options.get(CONF_INCLUDE, {}) or {}
    exclude = options.get(CONF_EXCLUDE, {}) or {}
    return vol.Schema(
        {
            vol.Optional(
                CONF_INCLUDE_ENTITIES,
                default=include.get(CONF_ENTITIES, []),
            ): selector.EntitySelector(
                selector.EntitySelectorConfig(multiple=True)
            ),
            vol.Optional(
                CONF_EXCLUDE_ENTITIES,
                default=exclude.get(CONF_ENTITIES, []),
            ): selector.EntitySelector(
                selector.EntitySelectorConfig(multiple=True)
            ),
            vol.Optional(
                CONF_INCLUDE_DOMAINS,
                default=", ".join(include.get(CONF_DOMAINS, [])),
            ): selector.TextSelector(),
            vol.Optional(
                CONF_EXCLUDE_DOMAINS,
                default=", ".join(exclude.get(CONF_DOMAINS, [])),
            ): selector.TextSelector(),
            vol.Optional(
                CONF_INCLUDE_ENTITY_GLOBS,
                default=", ".join(include.get(CONF_ENTITY_GLOBS, [])),
            ): selector.TextSelector(),
            vol.Optional(
                CONF_EXCLUDE_ENTITY_GLOBS,
                default=", ".join(exclude.get(CONF_ENTITY_GLOBS, [])),
            ): selector.TextSelector(),
            vol.Optional(
                CONF_ATTRIBUTE_ALLOWLIST,
                default=", ".join(options.get(CONF_ATTRIBUTE_ALLOWLIST, [])),
            ): selector.TextSelector(),
            vol.Optional(
                CONF_ATTRIBUTE_DENYLIST,
                default=", ".join(options.get(CONF_ATTRIBUTE_DENYLIST, [])),
            ): selector.TextSelector(),
            vol.Optional(CONF_SHOW_ADVANCED, default=False): bool,
        }
    )


def _advanced_schema(options: dict[str, Any]) -> vol.Schema:
    get = options.get
    return vol.Schema(
        {
            vol.Optional(
                CONF_INGRESS_QUEUE_CAPACITY,
                default=get(CONF_INGRESS_QUEUE_CAPACITY, PROVISIONAL_INGRESS_QUEUE_CAPACITY),
            ): _number(10, 100_000, 10),
            vol.Optional(
                CONF_MAX_SERIALIZED_EVENT_BYTES,
                default=get(CONF_MAX_SERIALIZED_EVENT_BYTES, PROVISIONAL_MAX_SERIALIZED_EVENT_BYTES),
            ): _number(1_024, 1_048_576, 1_024),
            vol.Optional(
                CONF_PERSIST_BATCH_ROWS,
                default=get(CONF_PERSIST_BATCH_ROWS, PROVISIONAL_PERSIST_BATCH_ROWS),
            ): _number(1, 10_000, 1),
            vol.Optional(
                CONF_DELIVERY_BATCH_ROWS,
                default=get(CONF_DELIVERY_BATCH_ROWS, PROVISIONAL_DELIVERY_BATCH_ROWS),
            ): _number(1, 100_000, 1),
            vol.Optional(
                CONF_DELIVERY_BATCH_BYTES,
                default=get(CONF_DELIVERY_BATCH_BYTES, PROVISIONAL_DELIVERY_BATCH_BYTES),
            ): _number(4_096, 16_777_216, 1_024),
            vol.Optional(
                CONF_FLUSH_INTERVAL_SECONDS,
                default=get(CONF_FLUSH_INTERVAL_SECONDS, PROVISIONAL_FLUSH_INTERVAL_SECONDS),
            ): _number(0.05, 300, 0.05),
            vol.Optional(
                CONF_RETRY_INITIAL_SECONDS,
                default=get(CONF_RETRY_INITIAL_SECONDS, PROVISIONAL_RETRY_INITIAL_SECONDS),
            ): _number(0.1, 300, 0.1),
            vol.Optional(
                CONF_RETRY_MAX_SECONDS,
                default=get(CONF_RETRY_MAX_SECONDS, PROVISIONAL_RETRY_MAX_SECONDS),
            ): _number(1, 3_600, 1),
            vol.Optional(
                CONF_RETRY_MULTIPLIER,
                default=get(CONF_RETRY_MULTIPLIER, PROVISIONAL_RETRY_MULTIPLIER),
            ): _number(1, 10, 0.1),
            vol.Optional(
                CONF_RETRY_JITTER_RATIO,
                default=get(CONF_RETRY_JITTER_RATIO, PROVISIONAL_RETRY_JITTER_RATIO),
            ): _number(0, 1, 0.05),
            vol.Optional(
                CONF_FLUSH_ON_SHUTDOWN,
                default=get(CONF_FLUSH_ON_SHUTDOWN, PROVISIONAL_FLUSH_ON_SHUTDOWN),
            ): bool,
            vol.Optional(
                CONF_MAX_PENDING_ROWS,
                default=get(CONF_MAX_PENDING_ROWS, PROVISIONAL_MAX_PENDING_ROWS),
            ): _number(100, 10_000_000, 100),
            vol.Optional(
                CONF_MAX_PENDING_BYTES,
                default=get(CONF_MAX_PENDING_BYTES, PROVISIONAL_MAX_PENDING_BYTES),
            ): _number(1_048_576, 1_073_741_824, 1_048_576),
            vol.Optional(
                CONF_MAX_DEAD_LETTER_ROWS,
                default=get(CONF_MAX_DEAD_LETTER_ROWS, PROVISIONAL_MAX_DEAD_LETTER_ROWS),
            ): _number(10, 1_000_000, 10),
            vol.Optional(
                CONF_MAX_DEAD_LETTER_BYTES,
                default=get(CONF_MAX_DEAD_LETTER_BYTES, PROVISIONAL_MAX_DEAD_LETTER_BYTES),
            ): _number(65_536, 268_435_456, 65_536),
            vol.Optional(
                CONF_SQLITE_BUSY_TIMEOUT_SECONDS,
                default=get(CONF_SQLITE_BUSY_TIMEOUT_SECONDS, PROVISIONAL_SQLITE_BUSY_TIMEOUT_SECONDS),
            ): _number(0.05, 30, 0.05),
            vol.Optional(
                CONF_HTTP_TIMEOUT_SECONDS,
                default=get(CONF_HTTP_TIMEOUT_SECONDS, PROVISIONAL_HTTP_TIMEOUT_SECONDS),
            ): _number(1, 120, 1),
            vol.Optional(
                CONF_START_TIMEOUT_SECONDS,
                default=get(CONF_START_TIMEOUT_SECONDS, PROVISIONAL_START_TIMEOUT_SECONDS),
            ): _number(1, 120, 1),
            vol.Optional(
                CONF_STOP_TIMEOUT_SECONDS,
                default=get(CONF_STOP_TIMEOUT_SECONDS, PROVISIONAL_STOP_TIMEOUT_SECONDS),
            ): _number(1, 300, 1),
        }
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
            username = (user_input.get(CONF_USERNAME) or "").strip() or None
            password = user_input.get(CONF_PASSWORD) or None
            if not host or not table or len(table.encode("utf-8")) > 127:
                return self.async_show_form(
                    step_id="user",
                    data_schema=_user_schema(user_input),
                    errors={"base": "invalid_connection"},
                )
            if (username is None) != (password is None):
                return self.async_show_form(
                    step_id="user",
                    data_schema=_user_schema(user_input),
                    errors={"base": "invalid_auth_pair"},
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
                    CONF_USERNAME: username,
                    CONF_PASSWORD: password,
                },
            )

        return self.async_show_form(step_id="user", data_schema=_user_schema({}))

    @staticmethod
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Return the options flow for this config entry."""
        return HassQuestDbWriterOptionsFlow(config_entry)


class HassQuestDbWriterOptionsFlow(config_entries.OptionsFlow):
    """Edit filters and tuning knobs for one config entry."""

    def __init__(self, entry: config_entries.ConfigEntry) -> None:
        self._entry = entry
        self._filter_options: dict[str, Any] = {}

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Handle the entity filter step."""
        errors: dict[str, str] = {}
        if user_input is not None:
            include = _filter_side(user_input, include=True)
            exclude = _filter_side(user_input, include=False)
            self._filter_options = {
                CONF_INCLUDE: include,
                CONF_EXCLUDE: exclude,
                CONF_ATTRIBUTE_ALLOWLIST: _split_csv(
                    user_input.get(CONF_ATTRIBUTE_ALLOWLIST, "")
                ),
                CONF_ATTRIBUTE_DENYLIST: _split_csv(
                    user_input.get(CONF_ATTRIBUTE_DENYLIST, "")
                ),
            }
            try:
                INCLUDE_EXCLUDE_BASE_FILTER_SCHEMA(
                    {
                        CONF_INCLUDE: include,
                        CONF_EXCLUDE: exclude,
                    }
                )
            except vol.Invalid:
                errors["base"] = "invalid_filter"
            else:
                if user_input.get(CONF_SHOW_ADVANCED):
                    return self.async_show_form(
                        step_id="advanced",
                        data_schema=_advanced_schema(self._entry.options),
                    )
                return self.async_create_entry(
                    title="", data=self._filter_options
                )
        return self.async_show_form(
            step_id="init",
            data_schema=_init_schema(self._entry.options),
            errors=errors,
        )

    async def async_step_advanced(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Handle the advanced tuning step."""
        errors: dict[str, str] = {}
        if user_input is not None:
            # Complete partial input with the schema defaults so the
            # cross-field checks and the stored options are always full.
            values = {**_advanced_schema(self._entry.options)({}), **user_input}
            if values[CONF_RETRY_MAX_SECONDS] < values[CONF_RETRY_INITIAL_SECONDS]:
                errors["base"] = "invalid_retry_bounds"
            if (
                values[CONF_MAX_SERIALIZED_EVENT_BYTES]
                > values[CONF_MAX_DEAD_LETTER_BYTES]
            ):
                errors["base"] = "invalid_size_bounds"
            if errors:
                return self.async_show_form(
                    step_id="advanced",
                    data_schema=_advanced_schema(user_input),
                    errors=errors,
                )
            return self.async_create_entry(
                title="",
                data={**self._filter_options, **values},
            )
        return self.async_show_form(
            step_id="advanced",
            data_schema=_advanced_schema(self._entry.options),
        )

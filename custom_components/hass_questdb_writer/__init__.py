"""HASS QuestDB Writer integration."""

from __future__ import annotations

from pathlib import Path
from typing import TypeAlias

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    CONF_DOMAINS,
    CONF_ENTITIES,
    CONF_EXCLUDE,
    CONF_INCLUDE,
)
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.entityfilter import (
    CONF_ENTITY_GLOBS,
    EntityFilter,
    convert_include_exclude_filter,
)

from .attribute_filter import AttributeFilter
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
    CONF_PERSIST_IDLE_POLL_SECONDS,
    CONF_PORT,
    CONF_RETENTION_DAYS,
    CONF_RETRY_INITIAL_SECONDS,
    CONF_RETRY_JITTER_RATIO,
    CONF_RETRY_MAX_SECONDS,
    CONF_RETRY_MULTIPLIER,
    CONF_SQLITE_BUSY_TIMEOUT_SECONDS,
    CONF_START_TIMEOUT_SECONDS,
    CONF_STOP_TIMEOUT_SECONDS,
    CONF_TABLE,
    CONF_TLS_SELF_SIGNED,
    CONF_USE_TLS,
    CONF_USERNAME,
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
    PROVISIONAL_PERSIST_IDLE_POLL_SECONDS,
    PROVISIONAL_RETRY_INITIAL_SECONDS,
    PROVISIONAL_RETRY_JITTER_RATIO,
    PROVISIONAL_RETRY_MAX_SECONDS,
    PROVISIONAL_RETRY_MULTIPLIER,
    PROVISIONAL_SQLITE_BUSY_TIMEOUT_SECONDS,
    PROVISIONAL_START_TIMEOUT_SECONDS,
    PROVISIONAL_STOP_TIMEOUT_SECONDS,
)
from .runtime import (
    ConnectionConfiguration,
    HassQuestDbRuntime,
    RuntimeConfiguration,
    SpoolConfiguration,
)
from .worker import (
    WorkerSettings,
    WorkerStartError,
    WorkerStartTimeoutError,
)

HassQuestDbConfigEntry: TypeAlias = ConfigEntry[HassQuestDbRuntime]


def _entity_filter(options: dict) -> EntityFilter | None:
    """Build the include/exclude filter from stored options, if configured."""
    if CONF_INCLUDE not in options and CONF_EXCLUDE not in options:
        return None
    sides: dict[str, dict[str, list[str]]] = {}
    for key in (CONF_INCLUDE, CONF_EXCLUDE):
        side = options.get(key) or {}
        sides[key] = {
            CONF_DOMAINS: side.get(CONF_DOMAINS, []),
            CONF_ENTITY_GLOBS: side.get(CONF_ENTITY_GLOBS, []),
            CONF_ENTITIES: side.get(CONF_ENTITIES, []),
        }
    return convert_include_exclude_filter(sides)


def _runtime_configuration(
    hass: HomeAssistant, entry: HassQuestDbConfigEntry
) -> RuntimeConfiguration:
    """Build the runtime profile from entry data, options, and defaults."""
    data = entry.data
    options = entry.options
    spool_path = Path(
        hass.config.path(".storage", DOMAIN, f"{entry.entry_id}.db")
    )
    return RuntimeConfiguration(
        table=data[CONF_TABLE],
        spool=SpoolConfiguration(
            path=spool_path,
            max_pending_rows=options.get(
                CONF_MAX_PENDING_ROWS, PROVISIONAL_MAX_PENDING_ROWS
            ),
            max_pending_bytes=options.get(
                CONF_MAX_PENDING_BYTES, PROVISIONAL_MAX_PENDING_BYTES
            ),
            max_event_bytes=options.get(
                CONF_MAX_SERIALIZED_EVENT_BYTES,
                PROVISIONAL_MAX_SERIALIZED_EVENT_BYTES,
            ),
            max_dead_letter_rows=options.get(
                CONF_MAX_DEAD_LETTER_ROWS, PROVISIONAL_MAX_DEAD_LETTER_ROWS
            ),
            max_dead_letter_bytes=options.get(
                CONF_MAX_DEAD_LETTER_BYTES, PROVISIONAL_MAX_DEAD_LETTER_BYTES
            ),
            busy_timeout_seconds=options.get(
                CONF_SQLITE_BUSY_TIMEOUT_SECONDS,
                PROVISIONAL_SQLITE_BUSY_TIMEOUT_SECONDS,
            ),
        ),
        connection=ConnectionConfiguration(
            host=data[CONF_HOST],
            port=data[CONF_PORT],
            use_tls=data[CONF_USE_TLS],
            timeout_seconds=options.get(
                CONF_HTTP_TIMEOUT_SECONDS, PROVISIONAL_HTTP_TIMEOUT_SECONDS
            ),
            username=data.get(CONF_USERNAME),
            password=data.get(CONF_PASSWORD),
            tls_self_signed=bool(data.get(CONF_TLS_SELF_SIGNED, False)),
        ),
        worker=WorkerSettings(
            ingress_queue_capacity=options.get(
                CONF_INGRESS_QUEUE_CAPACITY,
                PROVISIONAL_INGRESS_QUEUE_CAPACITY,
            ),
            max_serialized_event_bytes=options.get(
                CONF_MAX_SERIALIZED_EVENT_BYTES,
                PROVISIONAL_MAX_SERIALIZED_EVENT_BYTES,
            ),
            persist_batch_rows=options.get(
                CONF_PERSIST_BATCH_ROWS, PROVISIONAL_PERSIST_BATCH_ROWS
            ),
            delivery_batch_rows=options.get(
                CONF_DELIVERY_BATCH_ROWS, PROVISIONAL_DELIVERY_BATCH_ROWS
            ),
            delivery_batch_bytes=options.get(
                CONF_DELIVERY_BATCH_BYTES, PROVISIONAL_DELIVERY_BATCH_BYTES
            ),
            flush_interval_seconds=options.get(
                CONF_FLUSH_INTERVAL_SECONDS,
                PROVISIONAL_FLUSH_INTERVAL_SECONDS,
            ),
            persist_idle_poll_seconds=options.get(
                CONF_PERSIST_IDLE_POLL_SECONDS,
                PROVISIONAL_PERSIST_IDLE_POLL_SECONDS,
            ),
            retry_initial_seconds=options.get(
                CONF_RETRY_INITIAL_SECONDS,
                PROVISIONAL_RETRY_INITIAL_SECONDS,
            ),
            retry_max_seconds=options.get(
                CONF_RETRY_MAX_SECONDS, PROVISIONAL_RETRY_MAX_SECONDS
            ),
            retry_multiplier=options.get(
                CONF_RETRY_MULTIPLIER, PROVISIONAL_RETRY_MULTIPLIER
            ),
            retry_jitter_ratio=options.get(
                CONF_RETRY_JITTER_RATIO, PROVISIONAL_RETRY_JITTER_RATIO
            ),
            flush_on_shutdown=options.get(
                CONF_FLUSH_ON_SHUTDOWN, PROVISIONAL_FLUSH_ON_SHUTDOWN
            ),
        ),
        start_timeout_seconds=options.get(
            CONF_START_TIMEOUT_SECONDS, PROVISIONAL_START_TIMEOUT_SECONDS
        ),
        stop_timeout_seconds=options.get(
            CONF_STOP_TIMEOUT_SECONDS, PROVISIONAL_STOP_TIMEOUT_SECONDS
        ),
        tracked_entity_ids=None,
        entity_filter=_entity_filter(options),
        attribute_filter=_attribute_filter(options),
        retention_days=int(options.get(CONF_RETENTION_DAYS, 0) or 0),
    )


def _attribute_filter(options: dict) -> AttributeFilter | None:
    """Build the attribute allow/deny filter from stored options, if any."""
    allow = tuple(options.get(CONF_ATTRIBUTE_ALLOWLIST, []))
    deny = tuple(options.get(CONF_ATTRIBUTE_DENYLIST, []))
    if not allow and not deny:
        return None
    return AttributeFilter(allow=allow, deny=deny)


async def async_setup_entry(
    hass: HomeAssistant, entry: HassQuestDbConfigEntry
) -> bool:
    """Set up HASS QuestDB Writer from a config entry."""
    runtime = HassQuestDbRuntime(
        hass,
        _runtime_configuration(hass, entry),
        entry_id=entry.entry_id,
        # QuestDB rejecting the stored credentials is offered for repair through
        # the reauthentication flow (ADR-0012); the runtime only asks once per
        # outage and Home Assistant skips the request when a reauth or
        # reconfigure flow is already running.
        request_reauth=lambda: entry.async_start_reauth(hass),
    )
    try:
        await runtime.async_start()
    except (WorkerStartError, WorkerStartTimeoutError) as err:
        # The worker owns the spool and the delivery thread: if it cannot come
        # up (spool file not writable yet, disc pressure, slow start) this is
        # transient, so let Home Assistant retry the setup with its own backoff
        # instead of leaving the entry in a setup error until a restart.
        # Anything else (programming or configuration errors) stays fatal.
        raise ConfigEntryNotReady(
            f"QuestDB writer did not start: {err}"
        ) from err
    entry.runtime_data = runtime
    await hass.config_entries.async_forward_entry_setups(entry, ["sensor"])
    entry.async_on_unload(entry.add_update_listener(async_update_options))
    return True


async def async_unload_entry(
    hass: HomeAssistant, entry: HassQuestDbConfigEntry
) -> bool:
    """Unload a HASS QuestDB Writer config entry."""
    unload_ok = await entry.runtime_data.async_stop()
    if unload_ok:
        await hass.config_entries.async_unload_platforms(entry, ["sensor"])
    return unload_ok


async def async_update_options(
    hass: HomeAssistant, entry: HassQuestDbConfigEntry
) -> None:
    """Reload the entry after its options were edited in the UI."""
    await hass.config_entries.async_reload(entry.entry_id)

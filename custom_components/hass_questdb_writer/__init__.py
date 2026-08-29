"""HASS QuestDB Writer integration."""

from __future__ import annotations

from pathlib import Path

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import (
    CONF_HOST,
    CONF_PORT,
    CONF_TABLE,
    CONF_USE_TLS,
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
from .runtime import (
    ConnectionConfiguration,
    HassQuestDbRuntime,
    RuntimeConfiguration,
    SpoolConfiguration,
)
from .worker import WorkerSettings

type HassQuestDbConfigEntry = ConfigEntry[HassQuestDbRuntime]


def _runtime_configuration(
    hass: HomeAssistant, entry: HassQuestDbConfigEntry
) -> RuntimeConfiguration:
    """Build the explicit provisional development profile."""
    spool_path = Path(
        hass.config.path(".storage", DOMAIN, f"{entry.entry_id}.db")
    )
    return RuntimeConfiguration(
        table=entry.data[CONF_TABLE],
        spool=SpoolConfiguration(
            path=spool_path,
            max_pending_rows=PROVISIONAL_MAX_PENDING_ROWS,
            max_pending_bytes=PROVISIONAL_MAX_PENDING_BYTES,
            max_event_bytes=PROVISIONAL_MAX_SERIALIZED_EVENT_BYTES,
            max_dead_letter_rows=PROVISIONAL_MAX_DEAD_LETTER_ROWS,
            max_dead_letter_bytes=PROVISIONAL_MAX_DEAD_LETTER_BYTES,
            busy_timeout_seconds=PROVISIONAL_SQLITE_BUSY_TIMEOUT_SECONDS,
        ),
        connection=ConnectionConfiguration(
            host=entry.data[CONF_HOST],
            port=entry.data[CONF_PORT],
            use_tls=entry.data[CONF_USE_TLS],
            timeout_seconds=PROVISIONAL_HTTP_TIMEOUT_SECONDS,
        ),
        worker=WorkerSettings(
            ingress_queue_capacity=PROVISIONAL_INGRESS_QUEUE_CAPACITY,
            max_serialized_event_bytes=PROVISIONAL_MAX_SERIALIZED_EVENT_BYTES,
            persist_batch_rows=PROVISIONAL_PERSIST_BATCH_ROWS,
            delivery_batch_rows=PROVISIONAL_DELIVERY_BATCH_ROWS,
            delivery_batch_bytes=PROVISIONAL_DELIVERY_BATCH_BYTES,
            flush_interval_seconds=PROVISIONAL_FLUSH_INTERVAL_SECONDS,
            retry_initial_seconds=PROVISIONAL_RETRY_INITIAL_SECONDS,
            retry_max_seconds=PROVISIONAL_RETRY_MAX_SECONDS,
            retry_multiplier=PROVISIONAL_RETRY_MULTIPLIER,
            retry_jitter_ratio=PROVISIONAL_RETRY_JITTER_RATIO,
            flush_on_shutdown=PROVISIONAL_FLUSH_ON_SHUTDOWN,
        ),
        start_timeout_seconds=PROVISIONAL_START_TIMEOUT_SECONDS,
        stop_timeout_seconds=PROVISIONAL_STOP_TIMEOUT_SECONDS,
        tracked_entity_ids=None,
    )


async def async_setup_entry(
    hass: HomeAssistant, entry: HassQuestDbConfigEntry
) -> bool:
    """Set up HASS QuestDB Writer from a config entry."""
    runtime = HassQuestDbRuntime(hass, _runtime_configuration(hass, entry))
    await runtime.async_start()
    entry.runtime_data = runtime
    return True


async def async_unload_entry(
    hass: HomeAssistant, entry: HassQuestDbConfigEntry
) -> bool:
    """Unload a HASS QuestDB Writer config entry."""
    return await entry.runtime_data.async_stop()

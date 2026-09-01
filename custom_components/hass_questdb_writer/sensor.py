"""Health sensors exposing the writer's live state to Home Assistant.

These sensors read from the in-memory runtime snapshot, never from
QuestDB: they keep reporting (and raising alarms) even while the server
is unreachable, which makes them the honest source for a write watchdog.

"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import json
from pathlib import Path
import time
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfTime
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import (
    CONF_HOST,
    CONF_PASSWORD,
    CONF_PORT,
    CONF_TABLE,
    CONF_USE_TLS,
    CONF_USERNAME,
    DOMAIN,
    PROVISIONAL_HTTP_TIMEOUT_SECONDS,
)
from .runtime import HassQuestDbRuntime, RuntimeSnapshot
from .transport import IlpHttpTransport, IlpTransportError
from .worker import WorkerState

_MANIFEST = json.loads(
    (Path(__file__).parent / "manifest.json").read_text(encoding="utf-8")
)

_WORKER_STATE_OPTIONS = [state.value for state in WorkerState]


@dataclass(frozen=True, slots=True)
class HealthSensorSpec:
    """One health sensor: identity plus a snapshot extractor."""

    key: str
    name: str
    icon: str
    device_class: SensorDeviceClass | None
    state_class: SensorStateClass | None
    native_unit: str | None
    options: list[str] | None
    extractor: Callable[[RuntimeSnapshot], Any]


def _last_success_age(snapshot: RuntimeSnapshot) -> float | None:
    last_success_ns = snapshot.worker.last_success_ns
    if last_success_ns is None:
        return None
    return max(0.0, (time.time_ns() - last_success_ns) / 1_000_000_000)


def _sensor_specs() -> tuple[HealthSensorSpec, ...]:
    return (
        HealthSensorSpec(
            key="state",
            name="Writer state",
            icon="mdi:database-check-outline",
            device_class=SensorDeviceClass.ENUM,
            state_class=None,
            native_unit=None,
            options=_WORKER_STATE_OPTIONS,
            extractor=lambda snapshot: snapshot.worker.state.value,
        ),
        HealthSensorSpec(
            key="last_success_age",
            name="Seconds since last delivery",
            icon="mdi:clock-alert-outline",
            device_class=SensorDeviceClass.DURATION,
            state_class=None,
            native_unit=UnitOfTime.SECONDS,
            options=None,
            extractor=_last_success_age,
        ),
        HealthSensorSpec(
            key="pending_rows",
            name="Pending rows in spool",
            icon="mdi:tray-arrow-down",
            device_class=None,
            state_class=SensorStateClass.MEASUREMENT,
            native_unit=None,
            options=None,
            extractor=lambda snapshot: snapshot.worker.pending_rows,
        ),
        HealthSensorSpec(
            key="delivered_events",
            name="Events delivered",
            icon="mdi:database-arrow-up-outline",
            device_class=None,
            state_class=SensorStateClass.TOTAL_INCREASING,
            native_unit=None,
            options=None,
            extractor=lambda snapshot: snapshot.worker.delivered_events,
        ),
        HealthSensorSpec(
            key="last_error",
            name="Last delivery error",
            icon="mdi:alert-outline",
            device_class=None,
            state_class=None,
            native_unit=None,
            options=None,
            extractor=lambda snapshot: snapshot.worker.last_error or "none",
        ),
    )


class QuestDbHealthSensor(SensorEntity):
    """A polled sensor reading one value from the runtime snapshot."""

    _attr_should_poll = True

    def __init__(
        self,
        runtime: HassQuestDbRuntime,
        entry: ConfigEntry,
        spec: HealthSensorSpec,
    ) -> None:
        self._runtime = runtime
        self._spec = spec
        self._attr_unique_id = f"{entry.entry_id}-{spec.key}"
        self._attr_name = spec.name
        self._attr_icon = spec.icon
        self._attr_device_class = spec.device_class
        self._attr_state_class = spec.state_class
        self._attr_native_unit_of_measurement = spec.native_unit
        self._attr_options = spec.options
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry.entry_id)},
            name="HASS QuestDB Writer",
            manufacturer="HASS QuestDB Writer",
            sw_version=_MANIFEST["version"],
        )

    async def async_update(self) -> None:
        """Refresh this sensor from the in-memory snapshot (no I/O)."""
        self._attr_native_value = self._spec.extractor(self._runtime.snapshot())


class QuestDbTableSizeSensor(SensorEntity):
    """On-disk size of the entry's table, queried from QuestDB.

    Unlike the memory-backed health sensors this one talks to QuestDB:
    while the server is unreachable the sensor goes unavailable (it is
    a growth monitor, not a watchdog source).
    """

    _attr_should_poll = True

    def __init__(self, entry: ConfigEntry, table_name: str) -> None:
        self._entry_data = entry.data
        self._table = table_name
        self._transport: IlpHttpTransport | None = None
        self._attr_unique_id = f"{entry.entry_id}-table_size"
        self._attr_name = "Table size on disk"
        self._attr_icon = "mdi:database-outline"
        self._attr_device_class = SensorDeviceClass.DATA_SIZE
        self._attr_native_unit_of_measurement = "B"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry.entry_id)},
            name="HASS QuestDB Writer",
            manufacturer="HASS QuestDB Writer",
            sw_version=_MANIFEST["version"],
        )

    async def async_update(self) -> None:
        """Query the table partitions size from QuestDB."""
        if self._transport is None:
            self._transport = IlpHttpTransport(
                self._entry_data[CONF_HOST],
                self._entry_data[CONF_PORT],
                use_tls=self._entry_data.get(CONF_USE_TLS, False),
                timeout_seconds=PROVISIONAL_HTTP_TIMEOUT_SECONDS,
                username=self._entry_data.get(CONF_USERNAME) or None,
                password=self._entry_data.get(CONF_PASSWORD) or None,
            )
        try:
            result = await self.hass.async_add_executor_job(
                self._transport.exec_query,
                f"SELECT sum(diskSize) FROM table_partitions('{self._table}')",
            )
            rows = result.get("dataset") or []
            size = int(rows[0][0]) if rows and rows[0][0] is not None else None
            self._attr_native_value = size
            self._attr_available = True
        except IlpTransportError:
            self._attr_available = False


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the writer health sensors for this config entry."""
    runtime: HassQuestDbRuntime = entry.runtime_data
    sensors = [
        QuestDbHealthSensor(runtime, entry, spec)
        for spec in _sensor_specs()
    ]
    sensors.append(
        QuestDbTableSizeSensor(entry, entry.data[CONF_TABLE])
    )
    async_add_entities(sensors)

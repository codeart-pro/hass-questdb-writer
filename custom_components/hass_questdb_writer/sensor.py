"""Health sensors exposing the writer's live state to Home Assistant.

These sensors read from the in-memory runtime snapshot, never from
QuestDB: they keep reporting (and raising alarms) even while the server
is unreachable, which makes them the honest source for a write watchdog.

"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
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
from homeassistant.core import CALLBACK_TYPE, HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.event import async_track_time_interval

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

# Polling model (ADR-0011).
#
# HA reads this module constant through
# `EntityComponent.async_setup_entry` (`getattr(platform, "SCAN_INTERVAL", None)`)
# and uses it as the platform interval for the five in-memory health sensors.
# 30 s is deliberate: reading the writer snapshot costs ~5 us, a user overriding
# the interval can force an update with `homeassistant.update_entity`, and the
# watchdog automation in the README should see a stuck writer within a minute.
SCAN_INTERVAL = timedelta(seconds=30)

# The table-size sensor queries QuestDB over HTTP, so it leaves platform polling
# (`should_poll = False`) and owns a slower timer of its own: the value is a sum
# over partitions that only grows, and 5 minutes bounds it to 288 queries/day.
TABLE_SIZE_SCAN_INTERVAL = timedelta(minutes=5)
TABLE_SIZE_TIMER_NAME = "hass_questdb_writer table size"


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
    # Names read as fields of the writer device: with `has_entity_name` the
    # device name ("HASS QuestDB Writer") is prefixed by Home Assistant, so the
    # labels here must not repeat it.
    return (
        HealthSensorSpec(
            key="state",
            name="State",
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
            name="Pending rows",
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

    # The name describes the entity only; HA prefixes the device name (Bronze
    # rule has-entity-name).
    _attr_has_entity_name = True

    # Platform interval is the module-level SCAN_INTERVAL (ADR-0011).
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

    It also leaves the platform poll and refreshes itself on
    `TABLE_SIZE_SCAN_INTERVAL` (ADR-0011): the platform interval exists for the
    in-memory health sensors, and a SQL query every platform tick would cost
    2,880 requests per day for a value that only grows.
    """

    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(self, entry: ConfigEntry, table_name: str) -> None:
        self._entry_data = entry.data
        self._table = table_name
        self._transport: IlpHttpTransport | None = None
        self._unsub_timer: CALLBACK_TYPE | None = None
        self._attr_unique_id = f"{entry.entry_id}-table_size"
        self._attr_name = "Table size"
        self._attr_icon = "mdi:database-outline"
        # No device_class on purpose: DATA_SIZE has a unit converter in
        # HA which would rewrite the state into the registry unit (B),
        # fighting our fixed MB. Without a device class the unit is shown
        # exactly as reported.
        self._attr_device_class = None
        self._attr_native_unit_of_measurement = "MB"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry.entry_id)},
            name="HASS QuestDB Writer",
            manufacturer="HASS QuestDB Writer",
            sw_version=_MANIFEST["version"],
        )

    async def async_added_to_hass(self) -> None:
        """Take the first measurement and start the refresh timer.

        HA writes the state right after this hook returns
        (`Entity.add_to_platform_finish`), so only the measurement is needed
        here; later refreshes write their own state.
        """
        await super().async_added_to_hass()
        await self.async_update()
        self._unsub_timer = async_track_time_interval(
            self.hass,
            self._async_refresh,
            TABLE_SIZE_SCAN_INTERVAL,
            name=TABLE_SIZE_TIMER_NAME,
        )

    async def async_will_remove_from_hass(self) -> None:
        """Cancel the timer so a removed entity leaves no scheduled work."""
        if self._unsub_timer is not None:
            self._unsub_timer()
            self._unsub_timer = None
        await super().async_will_remove_from_hass()

    async def _async_refresh(self, _now: datetime | None = None) -> None:
        """Refresh the value and publish it without platform polling."""
        await self.async_update()
        self.async_write_ha_state()

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
            size_bytes = int(rows[0][0]) if rows and rows[0][0] is not None else None
            self._attr_native_value = (
                round(size_bytes / 1_000_000, 1) if size_bytes is not None else None
            )
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

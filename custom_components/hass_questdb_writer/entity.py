"""Shared base entity for the writer's entities.

The integration quality scale rule `common-modules` expects the patterns an
integration reuses across its entities to live in common modules, and names
`entity.py` for the base entity. Every entity of this integration belongs to the
single device a config entry creates, so the device registry entry, the unique id
and the `has_entity_name` contract are defined here once instead of in every
sensor class.
"""

from __future__ import annotations

import json
from pathlib import Path

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.device_registry import DeviceInfo

from .const import DOMAIN

DEVICE_NAME = "HASS QuestDB Writer"

_MANIFEST = json.loads(
    (Path(__file__).parent / "manifest.json").read_text(encoding="utf-8")
)


class QuestDbWriterEntity(SensorEntity):
    """Base entity for one writer config entry.

    Subclasses pass their own ``key`` (stable, part of the unique id) and
    ``name`` (the device-relative label shown by Home Assistant).
    """

    # The name describes the entity only; Home Assistant prefixes the device
    # name (rule has-entity-name).
    _attr_has_entity_name = True

    def __init__(self, entry: ConfigEntry, key: str, name: str) -> None:
        self._attr_unique_id = f"{entry.entry_id}-{key}"
        self._attr_name = name
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry.entry_id)},
            name=DEVICE_NAME,
            manufacturer=DEVICE_NAME,
            sw_version=_MANIFEST["version"],
        )

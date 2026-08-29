"""HASS QuestDB Writer integration."""

from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import DOMAIN


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up HASS QuestDB Writer from a config entry."""
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = None
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a HASS QuestDB Writer config entry."""
    entries = hass.data.get(DOMAIN)
    if entries is not None:
        entries.pop(entry.entry_id, None)
        if not entries:
            hass.data.pop(DOMAIN, None)
    return True

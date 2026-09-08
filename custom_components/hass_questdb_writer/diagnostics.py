"""Diagnostics support: download a safe snapshot from the UI."""

from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import __version__ as HA_VERSION
from homeassistant.core import HomeAssistant
from homeassistant.helpers.redact import async_redact_data

from .const import CONF_PASSWORD, DOMAIN
from .transport import IlpHttpTransport, IlpTransportError

_MANIFEST = json.loads(
    (Path(__file__).parent / "manifest.json").read_text(encoding="utf-8")
)
_INTEGRATION_VERSION = _MANIFEST["version"]


async def _probe_schema(
    hass: HomeAssistant, entry: ConfigEntry
) -> dict[str, Any]:
    """Read the owned table's TTL and the QuestDB server version.

    Never raises: an unreachable server just yields ``unavailable``.
    """
    data = entry.data
    transport = IlpHttpTransport(
        data["host"],
        data["port"],
        use_tls=data.get("use_tls", False),
        timeout_seconds=5.0,
        username=data.get("username") or None,
        password=data.get("password") or None,
    )
    table = data["table"].replace("'", "''")
    result: dict[str, Any] = {"table": data["table"]}
    try:
        ttl_document = await hass.async_add_executor_job(
            transport.exec_query,
            "SELECT ttlValue, ttlUnit FROM tables() "
            f"WHERE table_name = '{table}'",
        )
        version_document = await hass.async_add_executor_job(
            transport.exec_query, "SELECT build()"
        )
        ttl_rows = ttl_document.get("dataset") or []
        version_rows = version_document.get("dataset") or []
        if ttl_rows and ttl_rows[0]:
            result["ttl_days"] = int(ttl_rows[0][0])
        if version_rows and version_rows[0]:
            result["questdb_version"] = str(version_rows[0][0])
    except IlpTransportError:
        result["unavailable"] = True
    return result


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ConfigEntry
) -> dict[str, Any]:
    """Return diagnostics for a config entry.

    Sensitive data (the HTTP Basic password) is redacted; everything else
    is configuration and counters — safe to share in a bug report.
    """
    return {
        "entry_data": async_redact_data(dict(entry.data), [CONF_PASSWORD]),
        "options": entry.options,
        "runtime": asdict(entry.runtime_data.snapshot()),
        "schema": await _probe_schema(hass, entry),
        "versions": {
            "integration": _INTEGRATION_VERSION,
            "home_assistant": HA_VERSION,
        },
        "domain": DOMAIN,
    }

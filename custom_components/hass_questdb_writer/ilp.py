"""Pure-Python QuestDB ILP encoding."""

from __future__ import annotations

from collections.abc import Mapping
import math
from typing import TypeAlias

FieldValue: TypeAlias = str | int | float | bool | None


class IlpEncodingError(ValueError):
    """Raised when a value cannot be represented safely as ILP."""


def _require_name(value: str, kind: str) -> str:
    if not isinstance(value, str) or not value:
        raise IlpEncodingError(f"{kind} must be a non-empty string")
    if "\x00" in value:
        raise IlpEncodingError(f"{kind} must not contain NUL")
    return value


def _escape_identifier(value: str, kind: str) -> str:
    """Escape a measurement, symbol key/value, or field key."""
    value = _require_name(value, kind)
    return (
        value.replace("\\", "\\\\")
        .replace(" ", "\\ ")
        .replace(",", "\\,")
        .replace("=", "\\=")
        .replace("\n", "\\\n")
        .replace("\r", "\\\r")
    )


def _escape_string(value: str) -> str:
    """Escape a quoted ILP string field value."""
    if "\x00" in value:
        raise IlpEncodingError("string field must not contain NUL")
    return (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\\n")
        .replace("\r", "\\\r")
    )


def _encode_field(value: FieldValue) -> str | None:
    """Encode one ILP field value, or omit a null."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        if not -(2**63) < value < 2**63:
            raise IlpEncodingError("integer field is outside signed 64-bit range")
        return f"{value}i"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise IlpEncodingError("float field must be finite")
        return repr(value)
    if isinstance(value, str):
        return f'"{_escape_string(value)}"'
    raise IlpEncodingError(f"unsupported field type: {type(value).__name__}")


def encode_row(
    table: str,
    *,
    symbols: Mapping[str, str | None],
    fields: Mapping[str, FieldValue],
    timestamp_ns: int,
) -> bytes:
    """Encode one QuestDB ILP row terminated by a newline."""
    if not isinstance(timestamp_ns, int) or isinstance(timestamp_ns, bool):
        raise IlpEncodingError("timestamp_ns must be an integer")

    measurement = _escape_identifier(table, "table")
    symbol_parts = []
    for key, value in symbols.items():
        if value is None:
            continue
        symbol_parts.append(
            f"{_escape_identifier(key, 'symbol key')}="
            f"{_escape_identifier(value, 'symbol value')}"
        )

    field_parts = []
    for key, value in fields.items():
        encoded = _encode_field(value)
        if encoded is None:
            continue
        field_parts.append(f"{_escape_identifier(key, 'field key')}={encoded}")
    if not field_parts:
        raise IlpEncodingError("at least one non-null field is required")

    symbols_text = f",{','.join(symbol_parts)}" if symbol_parts else ""
    line = (
        f"{measurement}{symbols_text} {','.join(field_parts)} {timestamp_ns}\n"
    )
    return line.encode("utf-8")

"""Versioned immutable events stored in the durable spool."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from typing import Final, Self

from .ilp import IlpTimestampMicros, encode_row
from .spool import NewSpoolEvent

EVENT_PAYLOAD_VERSION: Final = 1
_SIGNED_64_MAX: Final = 2**63 - 1


class EventEnvelopeError(ValueError):
    """An event cannot be serialized or restored safely."""


def _string(name: str, value: str, *, allow_empty: bool) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        qualifier = "a string" if allow_empty else "a non-empty string"
        raise EventEnvelopeError(f"{name} must be {qualifier}")
    if "\x00" in value:
        raise EventEnvelopeError(f"{name} must not contain NUL")
    return value


def _timestamp_ns(name: str, value: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= _SIGNED_64_MAX
    ):
        raise EventEnvelopeError(
            f"{name} must be a non-negative signed 64-bit integer"
        )
    return value


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    """Plain immutable state-change data with no live HA objects."""

    event_id: str
    entity_id: str
    state: str
    attributes_json: str
    timestamp_ns: int
    ingested_at_ns: int
    last_changed_ns: int
    last_updated_ns: int
    context_id: str | None

    def __post_init__(self) -> None:
        _string("event_id", self.event_id, allow_empty=False)
        _string("entity_id", self.entity_id, allow_empty=False)
        domain, separator, object_id = self.entity_id.partition(".")
        if not separator or not domain or not object_id:
            raise EventEnvelopeError(
                "entity_id must contain a non-empty domain and object ID"
            )
        _string("state", self.state, allow_empty=True)
        _string("attributes_json", self.attributes_json, allow_empty=False)
        try:
            attributes = json.loads(self.attributes_json)
        except (TypeError, ValueError) as exc:
            raise EventEnvelopeError("attributes_json is not valid JSON") from exc
        if not isinstance(attributes, dict):
            raise EventEnvelopeError("attributes_json must contain a JSON object")
        _timestamp_ns("timestamp_ns", self.timestamp_ns)
        _timestamp_ns("ingested_at_ns", self.ingested_at_ns)
        _timestamp_ns("last_changed_ns", self.last_changed_ns)
        _timestamp_ns("last_updated_ns", self.last_updated_ns)
        if self.context_id is not None:
            _string("context_id", self.context_id, allow_empty=False)

    @property
    def domain(self) -> str:
        """Return the Home Assistant entity domain."""
        return self.entity_id.partition(".")[0]

    def to_bytes(self) -> bytes:
        """Serialize deterministically for the versioned spool payload."""
        document = {"version": EVENT_PAYLOAD_VERSION, **asdict(self)}
        return json.dumps(
            document,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @classmethod
    def from_bytes(cls, payload: bytes) -> Self:
        """Restore and fully validate one spool payload."""
        if not isinstance(payload, bytes) or not payload:
            raise EventEnvelopeError("event payload must be non-empty bytes")
        try:
            document = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise EventEnvelopeError("event payload is not valid UTF-8 JSON") from exc
        if not isinstance(document, dict):
            raise EventEnvelopeError("event payload must contain a JSON object")
        if document.pop("version", None) != EVENT_PAYLOAD_VERSION:
            raise EventEnvelopeError("unsupported event payload version")
        expected = {field_name for field_name in cls.__dataclass_fields__}
        if set(document) != expected:
            raise EventEnvelopeError("event payload has unexpected fields")
        try:
            return cls(**document)
        except TypeError as exc:
            raise EventEnvelopeError("event payload fields are invalid") from exc

    def to_spool_event(self) -> NewSpoolEvent:
        """Create the immutable unit accepted by the SQLite spool."""
        return NewSpoolEvent(
            event_id=self.event_id,
            payload=self.to_bytes(),
            created_ns=self.ingested_at_ns,
        )

    def to_ilp(self, table: str) -> bytes:
        """Encode this event as one QuestDB ILP row."""
        return encode_row(
            table,
            symbols={"entity_id": self.entity_id, "domain": self.domain},
            fields={
                "event_id": self.event_id,
                "state": self.state,
                "attributes": self.attributes_json,
                "ingested_at": IlpTimestampMicros(self.ingested_at_ns // 1_000),
                "last_changed": IlpTimestampMicros(
                    self.last_changed_ns // 1_000
                ),
                "last_updated": IlpTimestampMicros(
                    self.last_updated_ns // 1_000
                ),
                "context_id": self.context_id,
            },
            timestamp_ns=self.timestamp_ns,
        )

"""Tests for versioned event serialization and ILP conversion."""

from __future__ import annotations

from dataclasses import replace
import json
import unittest

from custom_components.hass_questdb_writer.event import (
    EventEnvelope,
    EventEnvelopeError,
)


class EventEnvelopeTests(unittest.TestCase):
    def event(self) -> EventEnvelope:
        return EventEnvelope(
            event_id="event-1",
            entity_id="sensor.kitchen",
            state='on "quoted"',
            attributes_json='{"friendly_name":"Кухня","values":[1,true]}',
            ingested_at_ns=1_700_000_000_223_456_789,
            last_changed_ns=1_700_000_000_120_000_999,
            last_updated_ns=1_700_000_000_121_000_999,
            context_id="context-1",
        )

    def test_round_trip_is_deterministic(self) -> None:
        event = self.event()
        payload = event.to_bytes()
        self.assertEqual(EventEnvelope.from_bytes(payload), event)
        self.assertEqual(EventEnvelope.from_bytes(payload).to_bytes(), payload)
        self.assertIn("Кухня".encode(), payload)

    def test_creates_spool_event_with_ingestion_timestamp(self) -> None:
        event = self.event()
        spool_event = event.to_spool_event()
        self.assertEqual(spool_event.event_id, event.event_id)
        self.assertEqual(spool_event.created_ns, event.ingested_at_ns)
        self.assertEqual(EventEnvelope.from_bytes(spool_event.payload), event)

    def test_encodes_timestamp_fields_in_microseconds(self) -> None:
        encoded = self.event().to_ilp("ha_events")
        self.assertIn(b"ingested_at=1700000000223456t", encoded)
        self.assertIn(b"last_changed=1700000000120000t", encoded)
        # The designated timestamp is the HA last_updated value in nanoseconds
        # (ADR-0005) and must not be repeated as a named field.
        self.assertTrue(encoded.endswith(b" 1700000000121000999\n"))
        self.assertNotIn(b"last_updated=", encoded)

    def test_rejects_non_string_state(self) -> None:
        with self.assertRaises(EventEnvelopeError):
            replace(self.event(), state=123)  # type: ignore[arg-type]

    def test_rejects_nul_in_state(self) -> None:
        with self.assertRaises(EventEnvelopeError):
            replace(self.event(), state="a\x00b")

    def test_rejects_invalid_attributes_json(self) -> None:
        with self.assertRaises(EventEnvelopeError):
            replace(self.event(), attributes_json="not-json")

    def test_from_bytes_rejects_non_bytes_or_empty(self) -> None:
        for payload in (b"", "text"):  # type: ignore[list-item]
            with self.subTest(payload=payload):
                with self.assertRaises(EventEnvelopeError):
                    EventEnvelope.from_bytes(payload)  # type: ignore[arg-type]

    def test_from_bytes_rejects_non_object_json(self) -> None:
        with self.assertRaises(EventEnvelopeError):
            EventEnvelope.from_bytes(b"[1,2]")

    def test_from_bytes_rejects_mismatched_field_types(self) -> None:
        document = json.loads(self.event().to_bytes().decode("utf-8"))
        document["event_id"] = 123
        with self.assertRaises(EventEnvelopeError):
            EventEnvelope.from_bytes(
                json.dumps(document).encode("utf-8")
            )

    def test_uses_entity_domain_as_a_symbol(self) -> None:
        encoded = self.event().to_ilp("ha_events")
        self.assertTrue(
            encoded.startswith(
                b"ha_events,entity_id=sensor.kitchen,domain=sensor "
            )
        )

    def test_rejects_invalid_entity_id(self) -> None:
        for entity_id in ("sensor", ".kitchen", "sensor."):
            with self.subTest(entity_id=entity_id):
                with self.assertRaises(EventEnvelopeError):
                    replace(self.event(), entity_id=entity_id)

    def test_rejects_non_object_attributes(self) -> None:
        with self.assertRaisesRegex(EventEnvelopeError, "JSON object"):
            replace(self.event(), attributes_json="[]")

    def test_rejects_unknown_payload_version(self) -> None:
        document = json.loads(self.event().to_bytes())
        document["version"] = 3
        with self.assertRaisesRegex(EventEnvelopeError, "version"):
            EventEnvelope.from_bytes(json.dumps(document).encode())

    def test_rejects_legacy_payload_with_time_fired(self) -> None:
        document = json.loads(self.event().to_bytes())
        document["version"] = 1
        document["timestamp_ns"] = 1_700_000_000_123_456_789
        with self.assertRaisesRegex(EventEnvelopeError, "version"):
            EventEnvelope.from_bytes(json.dumps(document).encode())

    def test_rejects_extra_payload_fields(self) -> None:
        document = json.loads(self.event().to_bytes())
        document["unexpected"] = True
        with self.assertRaisesRegex(EventEnvelopeError, "unexpected"):
            EventEnvelope.from_bytes(json.dumps(document).encode())

    def test_rejects_negative_designated_timestamp(self) -> None:
        with self.assertRaisesRegex(EventEnvelopeError, "non-negative"):
            replace(self.event(), last_updated_ns=-1)


if __name__ == "__main__":
    unittest.main()

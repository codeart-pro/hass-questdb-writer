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
            timestamp_ns=1_700_000_000_123_456_789,
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
        self.assertIn(b"last_updated=1700000000121000t", encoded)
        self.assertTrue(encoded.endswith(b" 1700000000123456789\n"))

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
        document["version"] = 2
        with self.assertRaisesRegex(EventEnvelopeError, "version"):
            EventEnvelope.from_bytes(json.dumps(document).encode())

    def test_rejects_extra_payload_fields(self) -> None:
        document = json.loads(self.event().to_bytes())
        document["unexpected"] = True
        with self.assertRaisesRegex(EventEnvelopeError, "unexpected"):
            EventEnvelope.from_bytes(json.dumps(document).encode())

    def test_rejects_negative_designated_timestamp(self) -> None:
        with self.assertRaisesRegex(EventEnvelopeError, "non-negative"):
            replace(self.event(), timestamp_ns=-1)


if __name__ == "__main__":
    unittest.main()

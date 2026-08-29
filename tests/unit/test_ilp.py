"""Tests for the pure-Python ILP encoder."""

from __future__ import annotations

import math
import unittest

from custom_components.hass_questdb_writer.ilp import (
    IlpEncodingError,
    IlpTimestampMicros,
    encode_row,
)


class EncodeRowTests(unittest.TestCase):
    def test_encodes_supported_types(self) -> None:
        result = encode_row(
            "events",
            symbols={"entity_id": "sensor.room"},
            fields={
                "state": "on",
                "enabled": True,
                "count": 7,
                "value": 1.5,
                "ingested_at": IlpTimestampMicros(1_700_000_000_123_456),
                "missing": None,
            },
            timestamp_ns=1_700_000_000_000_000_000,
        )
        self.assertEqual(
            result,
            b'events,entity_id=sensor.room state="on",enabled=true,count=7i,'
            b'value=1.5,ingested_at=1700000000123456t '
            b'1700000000000000000\n',
        )

    def test_escapes_identifiers_and_strings(self) -> None:
        result = encode_row(
            "event table",
            symbols={"entity,key": "room = one"},
            fields={"text field": 'line 1\n"quoted"\\tail'},
            timestamp_ns=1,
        )
        self.assertEqual(
            result,
            b'event\\ table,entity\\,key=room\\ \\=\\ one '
            b'text\\ field="line 1\\\n\\"quoted\\"\\\\tail" 1\n',
        )

    def test_omits_null_symbols_and_fields(self) -> None:
        result = encode_row(
            "events",
            symbols={"optional": None},
            fields={"value": 1, "optional": None},
            timestamp_ns=2,
        )
        self.assertEqual(result, b"events value=1i 2\n")

    def test_rejects_non_finite_float(self) -> None:
        for value in (math.nan, math.inf, -math.inf):
            with self.subTest(value=value):
                with self.assertRaises(IlpEncodingError):
                    encode_row(
                        "events",
                        symbols={},
                        fields={"value": value},
                        timestamp_ns=1,
                    )

    def test_rejects_empty_fields(self) -> None:
        with self.assertRaisesRegex(IlpEncodingError, "at least one"):
            encode_row(
                "events", symbols={}, fields={"missing": None}, timestamp_ns=1
            )

    def test_rejects_invalid_integer_range(self) -> None:
        for value in (-(2**63), 2**63):
            with self.subTest(value=value):
                with self.assertRaises(IlpEncodingError):
                    encode_row(
                        "events",
                        symbols={},
                        fields={"value": value},
                        timestamp_ns=1,
                    )

    def test_rejects_invalid_timestamp_field_range(self) -> None:
        for value in (-(2**63), 2**63):
            with self.subTest(value=value):
                with self.assertRaises(IlpEncodingError):
                    encode_row(
                        "events",
                        symbols={},
                        fields={"value": IlpTimestampMicros(value)},
                        timestamp_ns=1,
                    )

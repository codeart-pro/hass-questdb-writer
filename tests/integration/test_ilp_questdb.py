"""QuestDB conformance tests for the pure-Python ILP encoder."""

from __future__ import annotations

import json
import os
import time
import unittest
import urllib.parse
import urllib.request

from custom_components.hass_questdb_writer.ilp import encode_row
from custom_components.hass_questdb_writer.transport import (
    IlpHttpTransport,
    PermanentIlpError,
)


class QuestDbIlpConformanceTests(unittest.TestCase):
    host = os.environ.get("QUESTDB_HTTP_HOST", "questdb")
    port = int(os.environ.get("QUESTDB_HTTP_PORT", "9000"))
    table = "hass_qdb_writer_ilp_conformance"

    def sql(self, statement: str) -> dict:
        query = urllib.parse.urlencode({"query": statement})
        with urllib.request.urlopen(
            f"http://{self.host}:{self.port}/exec?{query}", timeout=10
        ) as response:
            return json.load(response)

    def setUp(self) -> None:
        self.sql(f"drop table if exists {self.table}")
        self.sql(
            f"create table {self.table} ("
            "entity_id symbol, state varchar, enabled boolean, count long, "
            "value double, timestamp timestamp"
            ") timestamp(timestamp) partition by day wal"
        )

    def tearDown(self) -> None:
        self.sql(f"drop table if exists {self.table}")

    def wait_for_rows(self, expected: int) -> dict:
        deadline = time.monotonic() + 10
        while True:
            result = self.sql(
                f"select entity_id, state, enabled, count, value, "
                f"timestamp from {self.table} order by timestamp"
            )
            if result["count"] == expected or time.monotonic() >= deadline:
                return result
            time.sleep(0.01)

    def test_encoded_values_round_trip_through_questdb(self) -> None:
        entity_id = "sensor.kitchen, west"
        state = 'строка 1\n"quoted"\\tail'
        body = encode_row(
            self.table,
            symbols={"entity_id": entity_id},
            fields={
                "state": state,
                "enabled": True,
                "count": 7,
                "value": 1.5,
            },
            timestamp_ns=1_700_000_000_123_456_000,
        )
        with IlpHttpTransport(
            self.host, self.port, use_tls=False, timeout_seconds=10
        ) as transport:
            transport.send_batch(body)

        result = self.wait_for_rows(1)
        self.assertEqual(result["count"], 1)
        row = result["dataset"][0]
        self.assertEqual(row[0], entity_id)
        self.assertEqual(row[1], state)
        self.assertIs(row[2], True)
        self.assertEqual(row[3], 7)
        self.assertEqual(row[4], 1.5)
        self.assertEqual(row[5], "2023-11-14T22:13:20.123456Z")

    def test_transport_reuses_connection_for_multiple_batches(self) -> None:
        first = encode_row(
            self.table,
            symbols={"entity_id": "sensor.first"},
            fields={"state": "on"},
            timestamp_ns=1_700_000_000_123_456_000,
        )
        second = encode_row(
            self.table,
            symbols={"entity_id": "sensor.second"},
            fields={"state": "off"},
            timestamp_ns=1_700_000_000_123_457_000,
        )
        with IlpHttpTransport(
            self.host, self.port, use_tls=False, timeout_seconds=10
        ) as transport:
            transport.send_batch(first)
            connection = transport._connection
            transport.send_batch(second)
            self.assertIs(transport._connection, connection)

        result = self.wait_for_rows(2)
        self.assertEqual(result["count"], 2)
        self.assertEqual(
            [(row[0], row[1]) for row in result["dataset"]],
            [("sensor.first", "on"), ("sensor.second", "off")],
        )

    def test_schema_conflict_is_classified_as_permanent(self) -> None:
        invalid = encode_row(
            self.table,
            symbols={"entity_id": "sensor.invalid"},
            fields={"enabled": "not-a-boolean"},
            timestamp_ns=1_700_000_000_123_456_000,
        )
        with IlpHttpTransport(
            self.host, self.port, use_tls=False, timeout_seconds=10
        ) as transport:
            with self.assertRaises(PermanentIlpError) as caught:
                transport.send_batch(invalid)
        self.assertEqual(caught.exception.status_code, 400)
        self.assertFalse(caught.exception.retryable)
        self.assertFalse(caught.exception.delivery_uncertain)

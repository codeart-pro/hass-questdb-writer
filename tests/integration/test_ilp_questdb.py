"""QuestDB conformance tests for the pure-Python ILP encoder."""

from __future__ import annotations

import json
import os
import time
import unittest
import urllib.parse
import urllib.request

from custom_components.hass_questdb_writer.ilp import encode_row


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
        request = urllib.request.Request(
            f"http://{self.host}:{self.port}/write?precision=n",
            data=body,
            headers={"Content-Type": "text/plain; charset=utf-8"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            self.assertIn(response.status, (200, 204))

        deadline = time.monotonic() + 10
        while True:
            result = self.sql(
                f"select entity_id, state, enabled, count, value, "
                f"timestamp from {self.table}"
            )
            if result["count"] == 1 or time.monotonic() >= deadline:
                break
            time.sleep(0.01)
        self.assertEqual(result["count"], 1)
        row = result["dataset"][0]
        self.assertEqual(row[0], entity_id)
        self.assertEqual(row[1], state)
        self.assertIs(row[2], True)
        self.assertEqual(row[3], 7)
        self.assertEqual(row[4], 1.5)
        self.assertEqual(row[5], "2023-11-14T22:13:20.123456Z")

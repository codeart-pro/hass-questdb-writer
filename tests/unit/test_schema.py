"""Tests for owned QuestDB schema creation and validation."""

from __future__ import annotations

import unittest

from custom_components.hass_questdb_writer.schema import (
    DESIGNATED_COLUMN,
    EXPECTED_COLUMNS,
    UPSERT_KEY_COLUMNS,
    IlpSchemaManager,
    SchemaError,
    SchemaMismatchError,
    create_table_ddl,
    quote_identifier,
    validate_table_columns,
)
from custom_components.hass_questdb_writer.transport import (
    PermanentIlpError,
    RetryableIlpError,
)


class FakeExecTransport:
    """Stand-in for IlpHttpTransport with scripted exec responses."""

    def __init__(self) -> None:
        self.queries: list[str] = []
        self.responses: list[dict] = []
        self.errors: list[BaseException] = []
        self.closed = False

    def exec_query(self, query: str) -> dict:
        self.queries.append(query)
        if self.responses:
            return self.responses.pop(0)
        if self.errors:
            raise self.errors.pop(0)
        raise AssertionError(f"no scripted response for query: {query}")

    def close(self) -> None:
        self.closed = True


class SchemaManagerTests(unittest.TestCase):
    def columns(self) -> list[list[object]]:
        """SHOW COLUMNS dataset matching the owned schema."""
        dataset = []
        for name, typ in EXPECTED_COLUMNS:
            dataset.append(
                [
                    name,
                    typ,
                    False,
                    0,
                    False,
                    0,
                    0,
                    name == DESIGNATED_COLUMN,
                    name in UPSERT_KEY_COLUMNS,
                    "",
                    "",
                ]
            )
        return dataset

    def manager(self, transport: FakeExecTransport) -> IlpSchemaManager:
        return IlpSchemaManager(
            "localhost",
            9000,
            use_tls=False,
            timeout_seconds=1,
            transport=transport,  # type: ignore[arg-type]
        )

    def test_ddl_matches_owned_schema_and_quotes_table_name(self) -> None:
        ddl = create_table_ddl('my"table')
        self.assertIn('CREATE TABLE IF NOT EXISTS "my""table"', ddl)
        self.assertIn("TIMESTAMP(last_updated) PARTITION BY DAY WAL", ddl)
        self.assertIn("DEDUP UPSERT KEYS(last_updated, entity_id)", ddl)
        for name, typ in EXPECTED_COLUMNS:
            self.assertIn(f"{name} {typ}", ddl)

    def test_quote_identifier_doubles_embedded_quotes(self) -> None:
        self.assertEqual(quote_identifier('a"b'), '"a""b"')

    def test_validate_accepts_matching_columns(self) -> None:
        validate_table_columns(self.columns(), "ha_events")

    def test_validate_rejects_missing_column(self) -> None:
        rows = [row for row in self.columns() if row[0] != "state"]
        with self.assertRaisesRegex(SchemaMismatchError, "missing columns: state"):
            validate_table_columns(rows, "ha_events")

    def test_validate_rejects_extra_column(self) -> None:
        rows = self.columns() + [["extra_col", "DOUBLE", False, 0, False, 0, 0, False, False, "", ""]]
        with self.assertRaisesRegex(SchemaMismatchError, "unexpected columns: extra_col"):
            validate_table_columns(rows, "ha_events")

    def test_validate_rejects_wrong_type(self) -> None:
        rows = self.columns()
        for row in rows:
            if row[0] == "state":
                row[1] = "SYMBOL"
        with self.assertRaisesRegex(SchemaMismatchError, "wrong column types: state"):
            validate_table_columns(rows, "ha_events")

    def test_validate_rejects_missing_dedup_keys(self) -> None:
        rows = self.columns()
        for row in rows:
            if row[0] in UPSERT_KEY_COLUMNS:
                row[8] = False
        with self.assertRaisesRegex(SchemaMismatchError, "dedup upsert keys"):
            validate_table_columns(rows, "ha_events")

    def test_validate_rejects_wrong_designated_timestamp(self) -> None:
        rows = self.columns()
        for row in rows:
            row[7] = row[0] == "ingested_at"
        with self.assertRaisesRegex(SchemaMismatchError, "designated timestamp"):
            validate_table_columns(rows, "ha_events")

    def test_validate_rejects_malformed_responses(self) -> None:
        with self.assertRaises(SchemaError):
            validate_table_columns("not-a-list", "ha_events")
        with self.assertRaises(SchemaError):
            validate_table_columns([["entity_id"]], "ha_events")

    def test_ensure_creates_then_validates(self) -> None:
        transport = FakeExecTransport()
        transport.responses = [
            {"ddl": "OK"},
            {"dataset": self.columns(), "count": len(EXPECTED_COLUMNS)},
            self.tables_ttl(None),
        ]
        manager = self.manager(transport)
        manager.ensure("ha_events")
        self.assertEqual(transport.queries[0], create_table_ddl("ha_events"))
        self.assertIn("SHOW COLUMNS FROM", transport.queries[1])
        self.assertFalse(transport.closed)

    def test_ensure_raises_on_mismatch(self) -> None:
        transport = FakeExecTransport()
        rows = self.columns()
        rows.pop()
        transport.responses = [{"ddl": "OK"}, {"dataset": rows}]
        with self.assertRaises(SchemaMismatchError):
            self.manager(transport).ensure("ha_events")

    def test_ensure_table_disappearing_after_create_is_retryable(self) -> None:
        transport = FakeExecTransport()
        transport.responses = [{"ddl": "OK"}]
        transport.errors = [
            PermanentIlpError(
                "QuestDB exec returned HTTP 400: table does not exist "
                "[table=ha_events]",
                retryable=False,
                delivery_uncertain=False,
                status_code=400,
            )
        ]
        with self.assertRaises(RetryableIlpError):
            self.manager(transport).ensure("ha_events")

    def test_ensure_other_permanent_errors_propagate(self) -> None:
        transport = FakeExecTransport()
        transport.errors = [
            PermanentIlpError(
                "QuestDB exec returned HTTP 400: unexpected token",
                retryable=False,
                delivery_uncertain=False,
                status_code=400,
            )
        ]
        with self.assertRaises(PermanentIlpError):
            self.manager(transport).ensure("ha_events")

    def test_close_closes_owned_transport(self) -> None:
        transport = FakeExecTransport()
        manager = self.manager(transport)
        manager.close()
        self.assertTrue(transport.closed)

    def manager_with_retention(
        self, transport: FakeExecTransport, retention_days: int
    ) -> IlpSchemaManager:
        return IlpSchemaManager(
            "localhost",
            9000,
            use_tls=False,
            timeout_seconds=1,
            retention_days=retention_days,
            transport=transport,  # type: ignore[arg-type]
        )

    def tables_ttl(self, ttl_days: int | None) -> dict:
        value = ttl_days or 0
        unit = "DAY" if ttl_days else "null"
        return {"dataset": [["ha_events", value, unit]]}

    def test_retention_applies_ttl_when_unset(self) -> None:
        transport = FakeExecTransport()
        transport.responses = [
            {"dataset": self.columns()},
            {"dataset": self.columns()},
            self.tables_ttl(None),
            {"ddl": "OK"},
        ]
        self.manager_with_retention(transport, 30).ensure("ha_events")
        self.assertEqual(transport.queries[-1], 'ALTER TABLE "ha_events" SET TTL 30 DAYS')

    def test_retention_noop_when_ttl_matches(self) -> None:
        transport = FakeExecTransport()
        transport.responses = [
            {"dataset": self.columns()},
            {"dataset": self.columns()},
            self.tables_ttl(30),
        ]
        self.manager_with_retention(transport, 30).ensure("ha_events")
        self.assertNotIn("SET TTL", " ".join(transport.queries))

    def test_retention_zero_clears_existing_ttl(self) -> None:
        transport = FakeExecTransport()
        transport.responses = [
            {"dataset": self.columns()},
            {"dataset": self.columns()},
            self.tables_ttl(30),
            {"ddl": "OK"},
        ]
        self.manager_with_retention(transport, 0).ensure("ha_events")
        self.assertEqual(transport.queries[-1], 'ALTER TABLE "ha_events" SET TTL 0 DAYS')

    def test_retention_zero_noop_when_no_ttl(self) -> None:
        transport = FakeExecTransport()
        transport.responses = [
            {"dataset": self.columns()},
            {"dataset": self.columns()},
            self.tables_ttl(None),
        ]
        self.manager_with_retention(transport, 0).ensure("ha_events")
        self.assertNotIn("SET TTL", " ".join(transport.queries))

    def test_ensure_rejects_empty_or_nul_table_name(self) -> None:
        transport = FakeExecTransport()
        manager = self.manager(transport)
        for table in ("", "a\x00b"):
            with self.subTest(table=table):
                with self.assertRaises(ValueError):
                    manager.ensure(table)
        self.assertEqual(transport.queries, [])

    def test_ensure_propagates_other_permanent_errors(self) -> None:
        transport = FakeExecTransport()
        transport.responses = [{"ddl": "OK"}]
        transport.errors = [
            PermanentIlpError(
                "table is read only", retryable=False, delivery_uncertain=False
            )
        ]
        with self.assertRaises(PermanentIlpError):
            self.manager(transport).ensure("ha_events")

    def test_ensure_rejects_non_dict_show_columns_response(self) -> None:
        transport = FakeExecTransport()
        transport.responses = [{"ddl": "OK"}, None]  # type: ignore[list-item]
        with self.assertRaises(SchemaError):
            self.manager(transport).ensure("ha_events")

    def test_retention_rejects_malformed_tables_responses(self) -> None:
        cases = [
            [{"ddl": "OK"}, {"dataset": self.columns()}, None],  # type: ignore[list-item]
            [{"ddl": "OK"}, {"dataset": self.columns()}, {"dataset": [123]}],
            [
                {"ddl": "OK"},
                {"dataset": self.columns()},
                {"dataset": [["ha_events", "not-a-number"]]},
            ],
        ]
        for responses in cases:
            with self.subTest(responses=responses):
                transport = FakeExecTransport()
                transport.responses = responses
                with self.assertRaises(SchemaError):
                    self.manager_with_retention(transport, 30).ensure(
                        "ha_events"
                    )


if __name__ == "__main__":
    unittest.main()

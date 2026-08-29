"""Owned QuestDB table schema: create-if-missing and strict validation.

The integration owns the target table. `IlpSchemaManager.ensure` runs the
`CREATE TABLE IF NOT EXISTS` DDL (WAL, daily partitioning, deduplicated upsert
keys) and then validates an existing table with `SHOW COLUMNS`: exact column
names and types, exactly one designated timestamp, and exactly the declared
dedup upsert keys. A table that differs from the owned schema raises
`SchemaMismatchError` instead of being silently written to.
"""

from __future__ import annotations

from typing import Final

from .transport import (
    IlpHttpTransport,
    PermanentIlpError,
    RetryableIlpError,
)

EXPECTED_COLUMNS: Final = (
    ("last_updated", "TIMESTAMP"),
    ("entity_id", "SYMBOL"),
    ("domain", "SYMBOL"),
    ("state", "VARCHAR"),
    ("attributes", "VARCHAR"),
    ("event_id", "VARCHAR"),
    ("context_id", "VARCHAR"),
    ("ingested_at", "TIMESTAMP"),
    ("last_changed", "TIMESTAMP"),
)
DESIGNATED_COLUMN: Final = "last_updated"
UPSERT_KEY_COLUMNS: Final = ("last_updated", "entity_id")

# SHOW COLUMNS result row layout: column, type, indexed, indexBlockCapacity,
# symbolCached, symbolCapacity, symbolTableSize, designated, upsertKey, ...
_SHOW_NAME = 0
_SHOW_TYPE = 1
_SHOW_DESIGNATED = 7
_SHOW_UPSERT_KEY = 8
_TABLE_MISSING_MARKER = "does not exist"


class SchemaError(RuntimeError):
    """Base class for schema management failures."""


class SchemaMismatchError(SchemaError):
    """The existing table does not match the schema owned by the integration."""


def quote_identifier(value: str) -> str:
    """Quote a QuestDB identifier, doubling any embedded double quotes."""
    return '"' + value.replace('"', '""') + '"'


def create_table_ddl(table: str) -> str:
    """Return the owned CREATE TABLE statement for one table name."""
    columns = ", ".join(f"{name} {typ}" for name, typ in EXPECTED_COLUMNS)
    keys = ", ".join(UPSERT_KEY_COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS {quote_identifier(table)} ({columns}) "
        f"TIMESTAMP({DESIGNATED_COLUMN}) PARTITION BY DAY WAL "
        f"DEDUP UPSERT KEYS({keys})"
    )


def validate_table_columns(dataset: object, table: str) -> None:
    """Strictly validate SHOW COLUMNS output against the owned schema.

    Raises SchemaMismatchError with the concrete differences; malformed
    responses raise SchemaError.
    """
    if not isinstance(dataset, list):
        raise SchemaError(f"unexpected SHOW COLUMNS response for {table}")
    actual: dict[str, tuple[str, bool, bool]] = {}
    for row in dataset:
        if (
            not isinstance(row, list)
            or len(row) <= _SHOW_UPSERT_KEY
            or not isinstance(row[_SHOW_NAME], str)
            or not isinstance(row[_SHOW_TYPE], str)
        ):
            raise SchemaError(f"malformed SHOW COLUMNS row for {table}")
        actual[row[_SHOW_NAME]] = (
            row[_SHOW_TYPE],
            bool(row[_SHOW_DESIGNATED]),
            bool(row[_SHOW_UPSERT_KEY]),
        )

    expected_types = dict(EXPECTED_COLUMNS)
    problems: list[str] = []
    missing = sorted(set(expected_types) - set(actual))
    extra = sorted(set(actual) - set(expected_types))
    if missing:
        problems.append(f"missing columns: {', '.join(missing)}")
    if extra:
        problems.append(f"unexpected columns: {', '.join(extra)}")
    wrong_types = [
        name
        for name, typ in expected_types.items()
        if name in actual and actual[name][0] != typ
    ]
    if wrong_types:
        problems.append(
            "wrong column types: "
            + ", ".join(
                f"{name} is {actual[name][0]}, expected {expected_types[name]}"
                for name in wrong_types
            )
        )
    designated = sorted(
        name for name, (_, designated, _) in actual.items() if designated
    )
    if designated != [DESIGNATED_COLUMN]:
        problems.append(
            f"designated timestamp is {designated or 'none'}; "
            f"expected [{DESIGNATED_COLUMN}]"
        )
    upsert_keys = sorted(
        name for name, (_, _, upsert) in actual.items() if upsert
    )
    if upsert_keys != sorted(UPSERT_KEY_COLUMNS):
        problems.append(
            f"dedup upsert keys are {upsert_keys or 'none'}; "
            f"expected {sorted(UPSERT_KEY_COLUMNS)}"
        )
    if problems:
        raise SchemaMismatchError(
            f"table {table} does not match the owned schema: "
            + "; ".join(problems)
        )


class IlpSchemaManager:
    """Own one HTTP connection used for schema DDL and validation."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        use_tls: bool,
        timeout_seconds: float,
        username: str | None = None,
        password: str | None = None,
        transport: IlpHttpTransport | None = None,
    ) -> None:
        self._transport = transport or IlpHttpTransport(
            host,
            port,
            use_tls=use_tls,
            timeout_seconds=timeout_seconds,
            username=username,
            password=password,
        )

    def ensure(self, table: str) -> None:
        """Create the table if missing, then validate it strictly.

        Connection failures raise the classified transport errors; an
        existing table that differs from the owned schema raises
        SchemaMismatchError. A table that disappears between the CREATE and
        the validation step is treated as retryable.
        """
        if not isinstance(table, str) or not table or "\x00" in table:
            raise ValueError("table must be a non-empty string without NUL")
        self._transport.exec_query(create_table_ddl(table))
        try:
            document = self._transport.exec_query(
                f"SHOW COLUMNS FROM {quote_identifier(table)}"
            )
        except PermanentIlpError as exc:
            if _TABLE_MISSING_MARKER in str(exc):
                raise RetryableIlpError(
                    "table disappeared during schema check",
                    retryable=True,
                    delivery_uncertain=False,
                ) from exc
            raise
        if not isinstance(document, dict):
            raise SchemaError(f"unexpected exec response for {table}")
        validate_table_columns(document.get("dataset"), table)

    def close(self) -> None:
        """Close the owned connection; safe to call repeatedly."""
        self._transport.close()

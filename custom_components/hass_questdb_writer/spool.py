"""Durable single-owner SQLite spool for outbound events."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import math
from os import PathLike
import sqlite3
from types import TracebackType
from typing import Final, Self

SCHEMA_VERSION: Final = 1
_SQLITE_INTEGER_MIN: Final = -(2**63)
_SQLITE_INTEGER_MAX: Final = 2**63 - 1


class SpoolError(Exception):
    """Base class for durable spool failures."""


class SpoolClosedError(SpoolError):
    """An operation was attempted after the spool was closed."""


class SpoolStateError(SpoolError):
    """Persistent state does not satisfy a required invariant."""


class UnsupportedSpoolVersionError(SpoolError):
    """The database schema is newer or older than this implementation."""


class SpoolFullError(SpoolError):
    """The pending queue has reached an explicit row or payload-byte limit."""


class DeadLetterFullError(SpoolError):
    """The dead-letter store has reached an explicit capacity limit."""


class EventTooLargeError(SpoolError):
    """One serialized event exceeds the configured per-event limit."""


class BatchLimitTooSmallError(SpoolError):
    """The oldest event cannot fit in an otherwise empty batch."""


@dataclass(frozen=True, slots=True)
class NewSpoolEvent:
    """One validated serialization unit waiting to enter the spool."""

    event_id: str
    payload: bytes
    created_ns: int


@dataclass(frozen=True, slots=True)
class SpoolRecord:
    """One pending event, ordered by its local sequence number."""

    sequence: int
    event_id: str
    payload: bytes
    created_ns: int
    attempt_count: int
    last_error: str | None
    delivery_uncertain: bool


@dataclass(frozen=True, slots=True)
class DeadLetterRecord:
    """One permanently rejected event retained for diagnosis."""

    original_sequence: int
    event_id: str
    payload: bytes
    created_ns: int
    failed_ns: int
    attempt_count: int
    last_error: str
    delivery_uncertain: bool


@dataclass(frozen=True, slots=True)
class SpoolStats:
    """Transactionally maintained payload counters."""

    pending_rows: int
    pending_bytes: int
    dead_letter_rows: int
    dead_letter_bytes: int


_SCHEMA_STATEMENTS: Final = (
    """
    CREATE TABLE IF NOT EXISTS pending (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL UNIQUE CHECK (length(event_id) > 0),
        payload BLOB NOT NULL CHECK (
            typeof(payload) = 'blob' AND length(payload) > 0
        ),
        created_ns INTEGER NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
        last_error TEXT,
        delivery_uncertain INTEGER NOT NULL DEFAULT 0
            CHECK (delivery_uncertain IN (0, 1))
    ) STRICT
    """,
    """
    CREATE TABLE IF NOT EXISTS dead_letter (
        dead_letter_id INTEGER PRIMARY KEY AUTOINCREMENT,
        original_sequence INTEGER NOT NULL UNIQUE,
        event_id TEXT NOT NULL UNIQUE CHECK (length(event_id) > 0),
        payload BLOB NOT NULL CHECK (
            typeof(payload) = 'blob' AND length(payload) > 0
        ),
        created_ns INTEGER NOT NULL,
        failed_ns INTEGER NOT NULL,
        attempt_count INTEGER NOT NULL CHECK (attempt_count > 0),
        last_error TEXT NOT NULL CHECK (length(last_error) > 0),
        delivery_uncertain INTEGER NOT NULL
            CHECK (delivery_uncertain IN (0, 1))
    ) STRICT
    """,
    """
    CREATE TABLE IF NOT EXISTS spool_stats (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        pending_rows INTEGER NOT NULL CHECK (pending_rows >= 0),
        pending_bytes INTEGER NOT NULL CHECK (pending_bytes >= 0),
        dead_letter_rows INTEGER NOT NULL CHECK (dead_letter_rows >= 0),
        dead_letter_bytes INTEGER NOT NULL CHECK (dead_letter_bytes >= 0)
    ) STRICT
    """,
    """
    CREATE TRIGGER IF NOT EXISTS pending_stats_after_insert
    AFTER INSERT ON pending
    BEGIN
        UPDATE spool_stats
        SET pending_rows = pending_rows + 1,
            pending_bytes = pending_bytes + length(NEW.payload)
        WHERE singleton = 1;
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS pending_stats_after_delete
    AFTER DELETE ON pending
    BEGIN
        UPDATE spool_stats
        SET pending_rows = pending_rows - 1,
            pending_bytes = pending_bytes - length(OLD.payload)
        WHERE singleton = 1;
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS dead_letter_stats_after_insert
    AFTER INSERT ON dead_letter
    BEGIN
        UPDATE spool_stats
        SET dead_letter_rows = dead_letter_rows + 1,
            dead_letter_bytes = dead_letter_bytes + length(NEW.payload)
        WHERE singleton = 1;
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS dead_letter_stats_after_delete
    AFTER DELETE ON dead_letter
    BEGIN
        UPDATE spool_stats
        SET dead_letter_rows = dead_letter_rows - 1,
            dead_letter_bytes = dead_letter_bytes - length(OLD.payload)
        WHERE singleton = 1;
    END
    """,
)


def _positive_integer(name: str, value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _sqlite_integer(name: str, value: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not _SQLITE_INTEGER_MIN <= value <= _SQLITE_INTEGER_MAX
    ):
        raise ValueError(f"{name} must be a signed 64-bit integer")
    return value


def _error_text(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("last_error must be a non-empty string")
    return value


def _sequence_tuple(sequences: Iterable[int]) -> tuple[int, ...]:
    result = tuple(sequences)
    for sequence in result:
        _positive_integer("sequence", sequence)
    if len(set(result)) != len(result):
        raise ValueError("sequences must not contain duplicates")
    return result


def _spool_record(row: sqlite3.Row) -> SpoolRecord:
    return SpoolRecord(
        sequence=row["sequence"],
        event_id=row["event_id"],
        payload=row["payload"],
        created_ns=row["created_ns"],
        attempt_count=row["attempt_count"],
        last_error=row["last_error"],
        delivery_uncertain=bool(row["delivery_uncertain"]),
    )


class SQLiteSpool:
    """A bounded, durable queue owned by exactly one worker thread.

    Byte limits count serialized payload bytes. SQLite pages, indexes, WAL data,
    and metadata require additional disk space and are intentionally not hidden
    behind the capacity counters.
    """

    def __init__(
        self,
        path: str | PathLike[str],
        *,
        max_pending_rows: int,
        max_pending_bytes: int,
        max_event_bytes: int,
        max_dead_letter_rows: int,
        max_dead_letter_bytes: int,
        busy_timeout_seconds: float,
    ) -> None:
        self._max_pending_rows = _positive_integer(
            "max_pending_rows", max_pending_rows
        )
        self._max_pending_bytes = _positive_integer(
            "max_pending_bytes", max_pending_bytes
        )
        self._max_event_bytes = _positive_integer(
            "max_event_bytes", max_event_bytes
        )
        self._max_dead_letter_rows = _positive_integer(
            "max_dead_letter_rows", max_dead_letter_rows
        )
        self._max_dead_letter_bytes = _positive_integer(
            "max_dead_letter_bytes", max_dead_letter_bytes
        )
        if self._max_event_bytes > self._max_pending_bytes:
            raise ValueError("max_event_bytes must not exceed max_pending_bytes")
        if self._max_event_bytes > self._max_dead_letter_bytes:
            raise ValueError(
                "max_event_bytes must not exceed max_dead_letter_bytes"
            )
        if (
            not isinstance(busy_timeout_seconds, (int, float))
            or isinstance(busy_timeout_seconds, bool)
            or not math.isfinite(busy_timeout_seconds)
            or busy_timeout_seconds <= 0
        ):
            raise ValueError("busy_timeout_seconds must be positive and finite")

        self._connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(
                path,
                timeout=float(busy_timeout_seconds),
                isolation_level=None,
            )
            connection.row_factory = sqlite3.Row
            self._connection = connection
            journal_mode = connection.execute(
                "PRAGMA journal_mode = WAL"
            ).fetchone()[0]
            if str(journal_mode).lower() != "wal":
                raise SpoolStateError(
                    f"SQLite did not enable WAL mode: {journal_mode}"
                )
            connection.execute("PRAGMA synchronous = FULL")
            synchronous = connection.execute("PRAGMA synchronous").fetchone()[0]
            if synchronous != 2:
                raise SpoolStateError(
                    f"SQLite did not enable synchronous=FULL: {synchronous}"
                )
            self._initialize_schema()
        except SpoolError:
            self.close()
            raise
        except sqlite3.Error as exc:
            self.close()
            raise SpoolError("failed to initialize SQLite spool") from exc

    @property
    def _db(self) -> sqlite3.Connection:
        if self._connection is None:
            raise SpoolClosedError("SQLite spool is closed")
        return self._connection

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        connection = self._db
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    def _initialize_schema(self) -> None:
        connection = self._db
        with self._transaction():
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, SCHEMA_VERSION):
                raise UnsupportedSpoolVersionError(
                    f"unsupported spool schema version {version}; "
                    f"expected {SCHEMA_VERSION}"
                )
            for statement in _SCHEMA_STATEMENTS:
                connection.execute(statement)
            connection.execute(
                """
                INSERT INTO spool_stats (
                    singleton,
                    pending_rows,
                    pending_bytes,
                    dead_letter_rows,
                    dead_letter_bytes
                ) VALUES (1, 0, 0, 0, 0)
                ON CONFLICT(singleton) DO NOTHING
                """
            )
            overlap = connection.execute(
                """
                SELECT pending.event_id
                FROM pending
                JOIN dead_letter
                  ON pending.event_id = dead_letter.event_id
                  OR pending.sequence = dead_letter.original_sequence
                LIMIT 1
                """
            ).fetchone()
            if overlap is not None:
                raise SpoolStateError(
                    "an event exists in both pending and dead-letter state"
                )
            self._reconcile_stats()
            if version == 0:
                connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")

    def _reconcile_stats(self) -> None:
        connection = self._db
        pending = connection.execute(
            """
            SELECT COUNT(*) AS rows, COALESCE(SUM(length(payload)), 0) AS bytes
            FROM pending
            """
        ).fetchone()
        dead_letter = connection.execute(
            """
            SELECT COUNT(*) AS rows, COALESCE(SUM(length(payload)), 0) AS bytes
            FROM dead_letter
            """
        ).fetchone()
        connection.execute(
            """
            UPDATE spool_stats
            SET pending_rows = ?,
                pending_bytes = ?,
                dead_letter_rows = ?,
                dead_letter_bytes = ?
            WHERE singleton = 1
            """,
            (
                pending["rows"],
                pending["bytes"],
                dead_letter["rows"],
                dead_letter["bytes"],
            ),
        )

    def _stats_row(self) -> sqlite3.Row:
        row = self._db.execute(
            """
            SELECT pending_rows,
                   pending_bytes,
                   dead_letter_rows,
                   dead_letter_bytes
            FROM spool_stats
            WHERE singleton = 1
            """
        ).fetchone()
        if row is None:
            raise SpoolStateError("spool stats row is missing")
        return row

    def stats(self) -> SpoolStats:
        """Return current transactionally maintained payload counters."""
        try:
            row = self._stats_row()
        except sqlite3.Error as exc:
            raise SpoolError("failed to read spool stats") from exc
        return SpoolStats(
            pending_rows=row["pending_rows"],
            pending_bytes=row["pending_bytes"],
            dead_letter_rows=row["dead_letter_rows"],
            dead_letter_bytes=row["dead_letter_bytes"],
        )

    def _validate_new_event(self, event: NewSpoolEvent) -> None:
        if not isinstance(event, NewSpoolEvent):
            raise TypeError("events must contain NewSpoolEvent instances")
        if (
            not isinstance(event.event_id, str)
            or not event.event_id
            or "\x00" in event.event_id
        ):
            raise ValueError("event_id must be a non-empty string without NUL")
        if not isinstance(event.payload, bytes) or not event.payload:
            raise ValueError("payload must be non-empty bytes")
        _sqlite_integer("created_ns", event.created_ns)
        payload_bytes = len(event.payload)
        if payload_bytes > self._max_event_bytes:
            raise EventTooLargeError(
                f"event payload is {payload_bytes} bytes; "
                f"limit is {self._max_event_bytes}"
            )

    def enqueue(self, event_id: str, payload: bytes, created_ns: int) -> bool:
        """Persist one event, returning False for an identical existing ID."""
        event = NewSpoolEvent(event_id, payload, created_ns)
        return self.enqueue_many((event,)) == 1

    def enqueue_many(self, events: Iterable[NewSpoolEvent]) -> int:
        """Persist multiple events with one durable transaction.

        Identical event IDs already present in the spool, or repeated
        identically in the input, are idempotent and do not count as inserts.
        """
        event_values = tuple(events)
        for event in event_values:
            self._validate_new_event(event)
        if not event_values:
            return 0

        distinct_events: dict[str, NewSpoolEvent] = {}
        for event in event_values:
            previous = distinct_events.get(event.event_id)
            if previous is None:
                distinct_events[event.event_id] = event
            elif previous != event:
                raise SpoolStateError(
                    "event_id is repeated with different content in one batch"
                )

        try:
            with self._transaction():
                new_events: list[NewSpoolEvent] = []
                for event in distinct_events.values():
                    existing = self._db.execute(
                        """
                        SELECT payload, created_ns
                        FROM pending
                        WHERE event_id = ?
                        UNION ALL
                        SELECT payload, created_ns
                        FROM dead_letter
                        WHERE event_id = ?
                        LIMIT 1
                        """,
                        (event.event_id, event.event_id),
                    ).fetchone()
                    if existing is not None:
                        if (
                            existing["payload"] != event.payload
                            or existing["created_ns"] != event.created_ns
                        ):
                            raise SpoolStateError(
                                "event_id already exists with different content"
                            )
                        continue
                    new_events.append(event)

                stats = self._stats_row()
                if (
                    stats["pending_rows"] + len(new_events)
                    > self._max_pending_rows
                ):
                    raise SpoolFullError(
                        f"pending row limit {self._max_pending_rows} reached"
                    )
                added_bytes = sum(len(event.payload) for event in new_events)
                if (
                    stats["pending_bytes"] + added_bytes
                    > self._max_pending_bytes
                ):
                    raise SpoolFullError(
                        f"pending payload-byte limit "
                        f"{self._max_pending_bytes} reached"
                    )
                self._db.executemany(
                    """
                    INSERT INTO pending (event_id, payload, created_ns)
                    VALUES (?, ?, ?)
                    """,
                    (
                        (event.event_id, event.payload, event.created_ns)
                        for event in new_events
                    ),
                )
        except SpoolError:
            raise
        except sqlite3.Error as exc:
            raise SpoolError("failed to enqueue events") from exc
        return len(new_events)

    def peek_batch(
        self, *, max_rows: int, max_bytes: int
    ) -> tuple[SpoolRecord, ...]:
        """Read the oldest contiguous batch without changing delivery state."""
        _positive_integer("max_rows", max_rows)
        _positive_integer("max_bytes", max_bytes)
        try:
            rows = self._db.execute(
                """
                SELECT sequence,
                       event_id,
                       payload,
                       created_ns,
                       attempt_count,
                       last_error,
                       delivery_uncertain
                FROM pending
                ORDER BY sequence
                LIMIT ?
                """,
                (max_rows,),
            ).fetchall()
        except sqlite3.Error as exc:
            raise SpoolError("failed to read pending batch") from exc

        batch: list[SpoolRecord] = []
        batch_bytes = 0
        for row in rows:
            payload_bytes = len(row["payload"])
            if batch_bytes + payload_bytes > max_bytes:
                if not batch:
                    raise BatchLimitTooSmallError(
                        f"oldest event is {payload_bytes} bytes; "
                        f"batch limit is {max_bytes}"
                    )
                break
            batch.append(_spool_record(row))
            batch_bytes += payload_bytes
        return tuple(batch)

    def mark_delivered(self, sequences: Iterable[int]) -> int:
        """Atomically remove rows only after confirmed remote delivery."""
        sequence_values = _sequence_tuple(sequences)
        if not sequence_values:
            return 0
        try:
            with self._transaction():
                for sequence in sequence_values:
                    cursor = self._db.execute(
                        "DELETE FROM pending WHERE sequence = ?", (sequence,)
                    )
                    if cursor.rowcount != 1:
                        raise SpoolStateError(
                            f"pending sequence {sequence} does not exist"
                        )
        except SpoolError:
            raise
        except sqlite3.Error as exc:
            raise SpoolError("failed to mark events delivered") from exc
        return len(sequence_values)

    def record_attempt(
        self,
        sequences: Iterable[int],
        *,
        last_error: str,
        delivery_uncertain: bool,
    ) -> int:
        """Atomically record one retryable delivery attempt."""
        sequence_values = _sequence_tuple(sequences)
        _error_text(last_error)
        if not isinstance(delivery_uncertain, bool):
            raise ValueError("delivery_uncertain must be a boolean")
        if not sequence_values:
            return 0
        try:
            with self._transaction():
                for sequence in sequence_values:
                    cursor = self._db.execute(
                        """
                        UPDATE pending
                        SET attempt_count = attempt_count + 1,
                            last_error = ?,
                            delivery_uncertain = delivery_uncertain OR ?
                        WHERE sequence = ?
                        """,
                        (last_error, int(delivery_uncertain), sequence),
                    )
                    if cursor.rowcount != 1:
                        raise SpoolStateError(
                            f"pending sequence {sequence} does not exist"
                        )
        except SpoolError:
            raise
        except sqlite3.Error as exc:
            raise SpoolError("failed to record delivery attempt") from exc
        return len(sequence_values)

    def move_to_dead_letter(
        self,
        sequences: Iterable[int],
        *,
        last_error: str,
        failed_ns: int,
        delivery_uncertain: bool,
    ) -> int:
        """Atomically retain permanently failed rows and unblock the queue.

        This operation counts the permanent failure as one delivery attempt.
        A previous uncertain-delivery flag is never cleared.
        """
        sequence_values = _sequence_tuple(sequences)
        _error_text(last_error)
        _sqlite_integer("failed_ns", failed_ns)
        if not isinstance(delivery_uncertain, bool):
            raise ValueError("delivery_uncertain must be a boolean")
        if not sequence_values:
            return 0

        try:
            with self._transaction():
                records: list[SpoolRecord] = []
                for sequence in sequence_values:
                    row = self._db.execute(
                        """
                        SELECT sequence,
                               event_id,
                               payload,
                               created_ns,
                               attempt_count,
                               last_error,
                               delivery_uncertain
                        FROM pending
                        WHERE sequence = ?
                        """,
                        (sequence,),
                    ).fetchone()
                    if row is None:
                        raise SpoolStateError(
                            f"pending sequence {sequence} does not exist"
                        )
                    records.append(_spool_record(row))

                stats = self._stats_row()
                added_bytes = sum(len(record.payload) for record in records)
                if (
                    stats["dead_letter_rows"] + len(records)
                    > self._max_dead_letter_rows
                ):
                    raise DeadLetterFullError(
                        f"dead-letter row limit "
                        f"{self._max_dead_letter_rows} reached"
                    )
                if (
                    stats["dead_letter_bytes"] + added_bytes
                    > self._max_dead_letter_bytes
                ):
                    raise DeadLetterFullError(
                        f"dead-letter payload-byte limit "
                        f"{self._max_dead_letter_bytes} reached"
                    )

                for record in records:
                    self._db.execute(
                        """
                        INSERT INTO dead_letter (
                            original_sequence,
                            event_id,
                            payload,
                            created_ns,
                            failed_ns,
                            attempt_count,
                            last_error,
                            delivery_uncertain
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            record.sequence,
                            record.event_id,
                            record.payload,
                            record.created_ns,
                            failed_ns,
                            record.attempt_count + 1,
                            last_error,
                            int(
                                record.delivery_uncertain
                                or delivery_uncertain
                            ),
                        ),
                    )
                    cursor = self._db.execute(
                        "DELETE FROM pending WHERE sequence = ?",
                        (record.sequence,),
                    )
                    if cursor.rowcount != 1:
                        raise SpoolStateError(
                            f"pending sequence {record.sequence} disappeared"
                        )
        except SpoolError:
            raise
        except sqlite3.Error as exc:
            raise SpoolError("failed to move events to dead letter") from exc
        return len(sequence_values)

    def peek_dead_letters(self, *, limit: int) -> tuple[DeadLetterRecord, ...]:
        """Read the oldest dead-letter rows for diagnostics."""
        _positive_integer("limit", limit)
        try:
            rows = self._db.execute(
                """
                SELECT original_sequence,
                       event_id,
                       payload,
                       created_ns,
                       failed_ns,
                       attempt_count,
                       last_error,
                       delivery_uncertain
                FROM dead_letter
                ORDER BY dead_letter_id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        except sqlite3.Error as exc:
            raise SpoolError("failed to read dead letters") from exc
        return tuple(
            DeadLetterRecord(
                original_sequence=row["original_sequence"],
                event_id=row["event_id"],
                payload=row["payload"],
                created_ns=row["created_ns"],
                failed_ns=row["failed_ns"],
                attempt_count=row["attempt_count"],
                last_error=row["last_error"],
                delivery_uncertain=bool(row["delivery_uncertain"]),
            )
            for row in rows
        )

    def delete_dead_letters(self, original_sequences: Iterable[int]) -> int:
        """Atomically delete selected dead letters after operator review."""
        sequence_values = _sequence_tuple(original_sequences)
        if not sequence_values:
            return 0
        try:
            with self._transaction():
                for sequence in sequence_values:
                    cursor = self._db.execute(
                        """
                        DELETE FROM dead_letter
                        WHERE original_sequence = ?
                        """,
                        (sequence,),
                    )
                    if cursor.rowcount != 1:
                        raise SpoolStateError(
                            f"dead-letter sequence {sequence} does not exist"
                        )
        except SpoolError:
            raise
        except sqlite3.Error as exc:
            raise SpoolError("failed to delete dead letters") from exc
        return len(sequence_values)

    def close(self) -> None:
        """Close the owning connection; safe to call repeatedly."""
        connection, self._connection = self._connection, None
        if connection is not None:
            try:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
            finally:
                connection.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

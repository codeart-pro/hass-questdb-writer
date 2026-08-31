"""Tests for the durable single-owner SQLite spool."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from custom_components.hass_questdb_writer.spool import (
    BatchLimitTooSmallError,
    EventTooLargeError,
    NewSpoolEvent,
    SQLiteSpool,
    SpoolClosedError,
    SpoolError,
    SpoolFullError,
    SpoolStateError,
    UnsupportedSpoolVersionError,
)


class SQLiteSpoolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.path = Path(self.temporary_directory.name) / "events.db"

    def open_spool(self, **overrides: object) -> SQLiteSpool:
        options: dict[str, object] = {
            "max_pending_rows": 10,
            "max_pending_bytes": 100,
            "max_event_bytes": 50,
            "max_dead_letter_rows": 10,
            "max_dead_letter_bytes": 100,
            "busy_timeout_seconds": 0.25,
        }
        options.update(overrides)
        return SQLiteSpool(self.path, **options)

    def test_enqueue_many_preserves_order_and_is_idempotent(self) -> None:
        events = (
            NewSpoolEvent("event-1", b"one", 1),
            NewSpoolEvent("event-2", b"two-two", 2),
        )
        with self.open_spool() as spool:
            self.assertEqual(spool.enqueue_many(events), 2)
            self.assertEqual(spool.enqueue_many((events[0], events[0])), 0)

            batch = spool.peek_batch(max_rows=10, max_bytes=100)
            self.assertEqual(
                [record.event_id for record in batch], ["event-1", "event-2"]
            )
            self.assertEqual([record.sequence for record in batch], [1, 2])
            self.assertEqual(spool.stats().pending_rows, 2)
            self.assertEqual(spool.stats().pending_bytes, 10)

    def test_duplicate_id_with_different_content_is_rejected(self) -> None:
        with self.open_spool() as spool:
            spool.enqueue("event-1", b"one", 1)
            with self.assertRaises(SpoolStateError):
                spool.enqueue("event-1", b"different", 1)
            with self.assertRaises(SpoolStateError):
                spool.enqueue_many(
                    (
                        NewSpoolEvent("event-2", b"one", 2),
                        NewSpoolEvent("event-2", b"two", 2),
                    )
                )
            self.assertEqual(spool.stats().pending_rows, 1)

    def test_pending_row_and_byte_limits_are_atomic(self) -> None:
        with self.open_spool(
            max_pending_rows=2,
            max_pending_bytes=7,
            max_event_bytes=7,
        ) as spool:
            spool.enqueue("event-1", b"1234", 1)
            with self.assertRaises(SpoolFullError):
                spool.enqueue_many(
                    (
                        NewSpoolEvent("event-2", b"12", 2),
                        NewSpoolEvent("event-3", b"3", 3),
                    )
                )
            self.assertEqual(spool.stats().pending_rows, 1)
            self.assertEqual(spool.stats().pending_bytes, 4)

            with self.assertRaises(SpoolFullError):
                spool.enqueue("event-4", b"1234", 4)
            self.assertEqual(spool.stats().pending_rows, 1)

    def test_event_size_limit_is_checked_before_writing(self) -> None:
        with self.open_spool(max_event_bytes=3) as spool:
            with self.assertRaises(EventTooLargeError):
                spool.enqueue("event-1", b"1234", 1)
            self.assertEqual(spool.stats().pending_rows, 0)

    def test_peek_batch_honors_row_and_byte_limits(self) -> None:
        with self.open_spool() as spool:
            spool.enqueue_many(
                (
                    NewSpoolEvent("event-1", b"123", 1),
                    NewSpoolEvent("event-2", b"4567", 2),
                    NewSpoolEvent("event-3", b"89", 3),
                )
            )
            batch = spool.peek_batch(max_rows=3, max_bytes=7)
            self.assertEqual(
                [record.event_id for record in batch], ["event-1", "event-2"]
            )
            self.assertEqual(
                [
                    record.event_id
                    for record in spool.peek_batch(max_rows=1, max_bytes=100)
                ],
                ["event-1"],
            )
            with self.assertRaises(BatchLimitTooSmallError):
                spool.peek_batch(max_rows=3, max_bytes=2)

    def test_mark_delivered_is_atomic_when_a_sequence_is_missing(self) -> None:
        with self.open_spool() as spool:
            spool.enqueue_many(
                (
                    NewSpoolEvent("event-1", b"one", 1),
                    NewSpoolEvent("event-2", b"two", 2),
                )
            )
            with self.assertRaises(SpoolStateError):
                spool.mark_delivered((1, 999))
            self.assertEqual(spool.stats().pending_rows, 2)

            self.assertEqual(spool.mark_delivered((1, 2)), 2)
            self.assertEqual(spool.stats().pending_rows, 0)
            self.assertEqual(spool.stats().pending_bytes, 0)

    def test_retry_metadata_survives_reopen_and_uncertainty_is_sticky(self) -> None:
        spool = self.open_spool()
        spool.enqueue("event-1", b"payload", 1)
        spool.record_attempt(
            (1,), last_error="response lost", delivery_uncertain=True
        )
        spool.record_attempt(
            (1,), last_error="connection refused", delivery_uncertain=False
        )
        spool.close()

        with self.open_spool() as reopened:
            record = reopened.peek_batch(max_rows=1, max_bytes=100)[0]
            self.assertEqual(record.attempt_count, 2)
            self.assertEqual(record.last_error, "connection refused")
            self.assertTrue(record.delivery_uncertain)

    def test_dead_letter_transition_and_delete_are_atomic(self) -> None:
        with self.open_spool() as spool:
            spool.enqueue("event-1", b"payload", 1)
            spool.record_attempt(
                (1,), last_error="temporary", delivery_uncertain=True
            )
            spool.move_to_dead_letter(
                (1,),
                last_error="schema mismatch",
                failed_ns=10,
                delivery_uncertain=False,
            )

            self.assertEqual(spool.stats().pending_rows, 0)
            self.assertEqual(spool.stats().dead_letter_rows, 1)
            dead = spool.peek_dead_letters(limit=10)[0]
            self.assertEqual(dead.original_sequence, 1)
            self.assertEqual(dead.attempt_count, 2)
            self.assertEqual(dead.last_error, "schema mismatch")
            self.assertTrue(dead.delivery_uncertain)
            self.assertFalse(spool.enqueue("event-1", b"payload", 1))

            with self.assertRaises(SpoolStateError):
                spool.delete_dead_letters((1, 999))
            self.assertEqual(spool.stats().dead_letter_rows, 1)
            self.assertEqual(spool.delete_dead_letters((1,)), 1)
            self.assertEqual(spool.stats().dead_letter_rows, 0)

    def test_dead_letter_evicts_oldest_rows_when_capacity_is_exceeded(self) -> None:
        with self.open_spool(
            max_dead_letter_rows=2,
            max_dead_letter_bytes=100,
            max_event_bytes=50,
        ) as spool:
            spool.enqueue_many(
                (
                    NewSpoolEvent("event-1", b"one", 1),
                    NewSpoolEvent("event-2", b"two", 2),
                    NewSpoolEvent("event-3", b"three", 3),
                )
            )
            moved, evicted = spool.move_to_dead_letter(
                (1, 2, 3),
                last_error="invalid",
                failed_ns=10,
                delivery_uncertain=False,
            )
            self.assertEqual((moved, evicted), (3, 1))
            self.assertEqual(spool.stats().pending_rows, 0)
            self.assertEqual(spool.stats().dead_letter_rows, 2)
            dead = spool.peek_dead_letters(limit=10)
            self.assertEqual(
                [record.event_id for record in dead], ["event-2", "event-3"]
            )
            self.assertEqual(
                spool.stats().dead_letter_bytes,
                sum(len(record.payload) for record in dead),
            )

    def test_dead_letter_evicts_oldest_rows_by_byte_limit(self) -> None:
        with self.open_spool(
            max_dead_letter_rows=10,
            max_dead_letter_bytes=6,
            max_event_bytes=6,
        ) as spool:
            spool.enqueue_many(
                (
                    NewSpoolEvent("event-1", b"1234", 1),
                    NewSpoolEvent("event-2", b"5678", 2),
                )
            )
            moved, evicted = spool.move_to_dead_letter(
                (1, 2),
                last_error="invalid",
                failed_ns=10,
                delivery_uncertain=False,
            )
            self.assertEqual((moved, evicted), (2, 1))
            self.assertEqual(spool.stats().dead_letter_rows, 1)
            self.assertEqual(spool.stats().dead_letter_bytes, 4)
            dead = spool.peek_dead_letters(limit=10)
            self.assertEqual([record.event_id for record in dead], ["event-2"])

    def test_stats_are_reconciled_from_records_on_reopen(self) -> None:
        spool = self.open_spool()
        spool.enqueue("event-1", b"payload", 1)
        spool.close()

        connection = sqlite3.connect(self.path)
        connection.execute(
            """
            UPDATE spool_stats
            SET pending_rows = 0,
                pending_bytes = 0,
                dead_letter_rows = 0,
                dead_letter_bytes = 0
            WHERE singleton = 1
            """
        )
        connection.commit()
        connection.close()

        with self.open_spool() as reopened:
            self.assertEqual(reopened.stats().pending_rows, 1)
            self.assertEqual(reopened.stats().pending_bytes, 7)

    def test_committed_event_survives_process_exit_without_close(self) -> None:
        script = """
import os
import sys
from custom_components.hass_questdb_writer.spool import SQLiteSpool

spool = SQLiteSpool(
    sys.argv[1],
    max_pending_rows=10,
    max_pending_bytes=100,
    max_event_bytes=50,
    max_dead_letter_rows=10,
    max_dead_letter_bytes=100,
    busy_timeout_seconds=0.25,
)
spool.enqueue("event-1", b"committed", 1)
os._exit(0)
"""
        result = subprocess.run(
            [sys.executable, "-c", script, str(self.path)],
            cwd=Path(__file__).parents[2],
            check=False,
        )
        self.assertEqual(result.returncode, 0)

        with self.open_spool() as reopened:
            self.assertEqual(reopened.stats().pending_rows, 1)
            self.assertEqual(
                reopened.peek_batch(max_rows=1, max_bytes=100)[0].payload,
                b"committed",
            )
            self.assertEqual(
                reopened._db.execute("PRAGMA integrity_check").fetchone()[0],
                "ok",
            )

    def test_uncommitted_event_is_rolled_back_after_process_exit(self) -> None:
        self.open_spool().close()
        script = """
import os
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1], isolation_level=None)
connection.execute("BEGIN IMMEDIATE")
connection.execute(
    "INSERT INTO pending (event_id, payload, created_ns) VALUES (?, ?, ?)",
    ("event-uncommitted", b"not-committed", 2),
)
os._exit(0)
"""
        result = subprocess.run(
            [sys.executable, "-c", script, str(self.path)],
            check=False,
        )
        self.assertEqual(result.returncode, 0)

        with self.open_spool() as reopened:
            self.assertEqual(reopened.stats().pending_rows, 0)
            self.assertEqual(reopened.peek_batch(max_rows=1, max_bytes=100), ())
            self.assertEqual(
                reopened._db.execute("PRAGMA integrity_check").fetchone()[0],
                "ok",
            )

    def test_connection_is_restricted_to_its_owner_thread(self) -> None:
        with self.open_spool() as spool:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(spool.stats)
                with self.assertRaises(SpoolError) as caught:
                    future.result()
            self.assertIsInstance(caught.exception.__cause__, sqlite3.ProgrammingError)
            self.assertEqual(spool.stats().pending_rows, 0)

    def test_close_is_idempotent_and_prevents_further_use(self) -> None:
        spool = self.open_spool()
        spool.close()
        spool.close()
        with self.assertRaises(SpoolClosedError):
            spool.stats()

    def test_rejects_unknown_schema_version(self) -> None:
        connection = sqlite3.connect(self.path)
        connection.execute("PRAGMA user_version = 99")
        connection.close()
        with self.assertRaises(UnsupportedSpoolVersionError):
            self.open_spool()

    def test_rejects_event_present_in_two_delivery_states(self) -> None:
        spool = self.open_spool()
        spool.enqueue("event-1", b"payload", 1)
        spool.close()

        connection = sqlite3.connect(self.path)
        connection.execute(
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
            ) VALUES (999, 'event-1', X'01', 1, 2, 1, 'corrupt', 0)
            """
        )
        connection.commit()
        connection.close()

        with self.assertRaises(SpoolStateError):
            self.open_spool()

    def test_options_reject_invalid_values(self) -> None:
        with self.assertRaises(ValueError):
            self.open_spool(max_pending_rows=0)
        with self.assertRaises(ValueError):
            self.open_spool(busy_timeout_seconds=0)
        with self.assertRaises(ValueError):
            self.open_spool(max_event_bytes=200, max_pending_bytes=100)
        with self.assertRaises(ValueError):
            self.open_spool(max_event_bytes=200, max_dead_letter_bytes=100)

    def test_enqueue_rejects_out_of_range_timestamp(self) -> None:
        with self.open_spool() as spool:
            with self.assertRaises(ValueError):
                spool.enqueue("event-1", b"x", 2**70)

    def test_record_attempt_rejects_empty_error_or_duplicate_sequences(
        self,
    ) -> None:
        with self.open_spool() as spool:
            spool.enqueue("event-1", b"x", 1)
            batch = spool.peek_batch(max_rows=10, max_bytes=100)
            sequences = tuple(record.sequence for record in batch)
            with self.assertRaises(ValueError):
                spool.record_attempt(
                    sequences,
                    last_error="",
                    delivery_uncertain=False,
                )
            with self.assertRaises(ValueError):
                spool.record_attempt(
                    (sequences[0], sequences[0]),
                    last_error="boom",
                    delivery_uncertain=False,
                )

    def test_dead_letter_rejects_out_of_range_timestamp(self) -> None:
        with self.open_spool() as spool:
            spool.enqueue("event-1", b"x", 1)
            batch = spool.peek_batch(max_rows=10, max_bytes=100)
            sequences = tuple(record.sequence for record in batch)
            with self.assertRaises(ValueError):
                spool.move_to_dead_letter(
                    sequences,
                    last_error="boom",
                    failed_ns=2**70,
                    delivery_uncertain=False,
                )


if __name__ == "__main__":
    unittest.main()

"""Tests for the durable single-owner SQLite spool."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, PropertyMock, patch

from custom_components.hass_questdb_writer.spool import (
    BatchLimitTooSmallError,
    classify_storage_error,
    DeadLetterFullError,
    EventTooLargeError,
    NewSpoolEvent,
    SQLiteSpool,
    SpoolClosedError,
    SpoolDiskFullError,
    SpoolError,
    SpoolFullError,
    SpoolReadOnlyError,
    SpoolStateError,
    UnsupportedSpoolVersionError,
    classify_storage_error,
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

    def test_enqueue_reports_a_full_database_as_a_storage_error(self) -> None:
        # SQLITE_FULL without filling the disk: a small page limit plus a payload
        # too large for the WAL makes SQLite refuse the write (a few kilobytes
        # still fit in the WAL and never touch the capped database file).
        with self.open_spool(
            max_pending_bytes=200_000,
            max_event_bytes=100_000,
            max_dead_letter_bytes=200_000,
        ) as spool:
            spool._db.execute("PRAGMA max_page_count = 8")
            with self.assertRaises(SpoolDiskFullError) as caught:
                spool.enqueue_many((NewSpoolEvent("event-1", b"x" * 60_000, 1),))
            self.assertIsInstance(caught.exception, SpoolError)
            self.assertIn("database or disk is full", str(caught.exception))
            cause = caught.exception.__cause__
            self.assertEqual(getattr(cause, "sqlite_errorcode", None), 13)

    def test_enqueue_reports_a_read_only_database_as_a_storage_error(self) -> None:
        with self.open_spool() as spool:
            spool._db.execute("PRAGMA query_only = ON")
            with self.assertRaises(SpoolReadOnlyError) as caught:
                spool.enqueue_many((NewSpoolEvent("event-1", b"payload", 1),))
            self.assertIn("read-only", str(caught.exception))

    def test_unclassified_sqlite_errors_stay_generic(self) -> None:
        # Only conditions a retry can clear are classified: anything else must
        # stay fatal rather than looping forever.
        error = classify_storage_error(sqlite3.Error("disk I/O error"), "failed")
        self.assertIs(type(error), SpoolError)
        self.assertNotIsInstance(error, (SpoolDiskFullError, SpoolReadOnlyError))

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


class SQLiteSpoolDefensiveBranchTests(unittest.TestCase):
    """Guards, wrappers and no-op paths (rule test-coverage)."""

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
        return SQLiteSpool(self.path, **options)  # type: ignore[arg-type]

    def test_constructor_rejects_inconsistent_limits(self) -> None:
        with self.assertRaises(ValueError):
            self.open_spool(max_event_bytes=60, max_pending_bytes=50)
        with self.assertRaises(ValueError):
            self.open_spool(max_event_bytes=60, max_dead_letter_bytes=50)
        with self.assertRaises(ValueError):
            self.open_spool(busy_timeout_seconds=0)

    def test_constructor_wraps_sqlite_failures(self) -> None:
        with patch.object(
            sqlite3, "connect", side_effect=sqlite3.OperationalError("boom")
        ):
            with self.assertRaises(SpoolError) as caught:
                self.open_spool()
        self.assertIn("failed to initialize", str(caught.exception))

    def test_missing_stats_row_is_reported(self) -> None:
        with self.open_spool() as spool:
            other = sqlite3.connect(self.path)
            other.execute("DELETE FROM spool_stats")
            other.commit()
            other.close()
            with self.assertRaises(SpoolStateError):
                spool.stats()

    def test_event_validators_reject_bad_input(self) -> None:
        with self.open_spool() as spool:
            with self.assertRaises(TypeError):
                spool.enqueue_many(("not-an-event",))  # type: ignore[arg-type]
            with self.assertRaises(ValueError):
                spool.enqueue_many((NewSpoolEvent("", b"one", 1),))
            with self.assertRaises(ValueError):
                spool.enqueue_many((NewSpoolEvent("id\x00suffix", b"one", 1),))
            with self.assertRaises(ValueError):
                spool.enqueue_many((NewSpoolEvent("event-1", b"", 1),))

    def test_attempt_validators_reject_bad_input(self) -> None:
        with self.open_spool() as spool:
            spool.enqueue("event-1", b"one", 1)
            with self.assertRaises(ValueError):
                spool.record_attempt(
                    [1], last_error="boom", delivery_uncertain="yes"  # type: ignore[arg-type]
                )
            with self.assertRaises(ValueError):
                spool.move_to_dead_letter(
                    [1],
                    last_error="boom",
                    failed_ns=1,
                    delivery_uncertain=None,  # type: ignore[arg-type]
                )

    def test_no_operation_calls_are_noops(self) -> None:
        with self.open_spool() as spool:
            self.assertEqual(spool.enqueue_many(()), 0)
            self.assertEqual(spool.mark_delivered([]), 0)
            self.assertEqual(
                spool.record_attempt(
                    [], last_error="boom", delivery_uncertain=False
                ),
                0,
            )
            self.assertEqual(
                spool.move_to_dead_letter(
                    [],
                    last_error="boom",
                    failed_ns=1,
                    delivery_uncertain=False,
                ),
                (0, 0),
            )
            self.assertEqual(spool.delete_dead_letters([]), 0)

    def test_missing_sequences_are_reported(self) -> None:
        with self.open_spool() as spool:
            spool.enqueue("event-1", b"one", 1)
            with self.assertRaises(SpoolStateError):
                spool.record_attempt(
                    [999], last_error="boom", delivery_uncertain=False
                )
            with self.assertRaises(SpoolStateError):
                spool.move_to_dead_letter(
                    [999],
                    last_error="boom",
                    failed_ns=1,
                    delivery_uncertain=False,
                )

    def test_sqlite_failures_are_wrapped(self) -> None:
        with self.open_spool() as spool:
            spool.enqueue("event-1", b"one", 1)
            broken = Mock()
            broken.execute.side_effect = sqlite3.OperationalError("boom")
            with patch.object(
                SQLiteSpool, "_db", new_callable=PropertyMock, return_value=broken
            ):
                for call in (
                    lambda: spool.enqueue_many((NewSpoolEvent("event-2", b"two", 2),)),
                    lambda: spool.peek_batch(max_rows=1, max_bytes=10),
                    lambda: spool.mark_delivered([1]),
                    lambda: spool.record_attempt(
                        [1], last_error="boom", delivery_uncertain=False
                    ),
                    lambda: spool.move_to_dead_letter(
                        [1],
                        last_error="boom",
                        failed_ns=1,
                        delivery_uncertain=False,
                    ),
                    lambda: spool.peek_dead_letters(limit=1),
                    lambda: spool.delete_dead_letters([1]),
                ):
                    with self.assertRaises(SpoolError):
                        call()

    def test_counters_follow_out_of_band_changes(self) -> None:
        """The triggers keep the counters honest, so the full-store guard is defensive.

        `move_to_dead_letter` still raises DeadLetterFullError when the counters
        claim space that the store does not have; because the schema maintains
        `spool_stats` with triggers on both tables, that can only happen with a
        broken database, not through normal eviction.
        """
        with self.open_spool(max_dead_letter_rows=1) as spool:
            spool.enqueue("event-1", b"one", 1)
            spool.move_to_dead_letter(
                [1], last_error="boom", failed_ns=1, delivery_uncertain=False
            )
            self.assertEqual(spool.stats().dead_letter_rows, 1)

            other = sqlite3.connect(self.path)
            other.execute("DELETE FROM dead_letter")
            other.commit()
            other.close()

            self.assertEqual(spool.stats().dead_letter_rows, 0)
            self.assertEqual(spool.stats().dead_letter_bytes, 0)
            spool.enqueue("event-2", b"two", 2)
            self.assertEqual(
                spool.move_to_dead_letter(
                    [2], last_error="boom", failed_ns=1, delivery_uncertain=False
                ),
                (1, 0),
            )

    def test_close_rolls_back_an_open_transaction(self) -> None:
        spool = self.open_spool()
        spool._db.execute("BEGIN IMMEDIATE")
        spool.close()
        self.assertIsNone(spool._connection)

    def test_new_spool_uses_incremental_auto_vacuum(self) -> None:
        # Measured on a sized filesystem: deleting delivered rows frees pages
        # inside the file, not space on the disk, so a spool that cannot give
        # pages back keeps the writer paused forever on a disk it filled itself
        # (docs/benchmarks/spool-pressure.md). Incremental auto-vacuum is the
        # mechanism that returns them, and it only takes effect on a database
        # that had it set before the first table existed.
        with self.open_spool() as spool:
            other = sqlite3.connect(self.path)
            mode = other.execute("PRAGMA auto_vacuum").fetchone()[0]
            other.close()
        self.assertEqual(mode, 2)

    def test_reclaim_returns_the_space_of_delivered_rows(self) -> None:
        spool = self.open_spool(
            max_pending_rows=5_000,
            max_pending_bytes=10_000_000,
            max_event_bytes=4_096,
            max_dead_letter_bytes=10_000_000,
        )
        with spool:
            self._fill_and_deliver(spool, rows=4_000)
            before = self.path.stat().st_size
            freelist_before = spool._db.execute(
                "PRAGMA freelist_count"
            ).fetchone()[0]
            reclaim = spool.reclaim(max_pages=4_096)
        self.assertEqual(reclaim.error, None)
        self.assertEqual(reclaim.auto_vacuum, 2)
        # `PRAGMA incremental_vacuum(N)` reports one row per page it moves, so a
        # cursor that is not drained moves a single page per call. The bound
        # here is the whole freelist of an emptied spool, not a token page.
        self.assertGreaterEqual(reclaim.freed_pages, freelist_before - 1)
        self.assertTrue(reclaim.wal_truncated)
        after = self.path.stat().st_size
        self.assertLess(after, before / 2)

    def test_reclaim_frees_no_more_pages_than_asked(self) -> None:
        # The worker runs this between durable writes, so one call has to be
        # bounded: the tail it does not reach is taken by the next call.
        spool = self.open_spool(
            max_pending_rows=5_000,
            max_pending_bytes=10_000_000,
            max_event_bytes=4_096,
            max_dead_letter_bytes=10_000_000,
        )
        with spool:
            self._fill_and_deliver(spool, rows=4_000)
            first = spool.reclaim(max_pages=64)
            second = spool.reclaim(max_pages=64)
        self.assertLessEqual(first.freed_pages, 64)
        self.assertLessEqual(second.freed_pages, 64)

    def test_compact_rewrites_a_spool_created_without_incremental_mode(self) -> None:
        # Databases created before this policy cannot switch mode in place: a
        # rewrite is the only way to give their space back, so it has to work and
        # to leave the database in incremental mode for good.
        legacy = sqlite3.connect(self.path)
        legacy.execute("CREATE TABLE pre_existing (id INTEGER)")
        legacy.commit()
        legacy.close()
        spool = self.open_spool(
            max_pending_rows=5_000,
            max_pending_bytes=10_000_000,
            max_event_bytes=4_096,
            max_dead_letter_bytes=10_000_000,
        )
        with spool:
            other = sqlite3.connect(self.path)
            self.assertEqual(other.execute("PRAGMA auto_vacuum").fetchone()[0], 0)
            other.close()
            self._fill_and_deliver(spool, rows=4_000)
            before = self.path.stat().st_size
            compact = spool.compact()
            after = self.path.stat().st_size
        self.assertEqual(compact.error, None)
        self.assertTrue(compact.rewrote_file)
        self.assertLess(after, before)
        other = sqlite3.connect(self.path)
        self.assertEqual(other.execute("PRAGMA auto_vacuum").fetchone()[0], 2)
        other.close()

    def test_reclaim_and_compact_report_failures_instead_of_raising(self) -> None:
        # Reclamation runs while the writer is paused on a full disk: an error
        # there must not turn a pause into a failure. A read-only connection is
        # the easiest way to make both mechanisms fail for real (setting
        # `max_page_count` below the current size is ignored by SQLite, so it
        # cannot be used once the database has pages).
        spool = self.open_spool(
            max_pending_rows=5_000,
            max_pending_bytes=10_000_000,
            max_event_bytes=4_096,
            max_dead_letter_bytes=10_000_000,
        )
        with spool:
            payload = b"x" * 2_011
            spool.enqueue_many(
                tuple(
                    NewSpoolEvent(f"event-{index}", payload, index)
                    for index in range(200)
                )
            )
            spool._db.execute("PRAGMA query_only = ON")
            reclaim = spool.reclaim(max_pages=64)
            compact = spool.compact()
            spool._db.execute("PRAGMA query_only = OFF")
        self.assertIsNotNone(reclaim.error)
        self.assertIsNotNone(compact.error)
        # A spool that is already closed is a caller mistake, not a crash.
        self.assertIsNotNone(spool.reclaim(max_pages=1).error)
        self.assertIsNotNone(spool.compact().error)

    def test_open_classifies_a_full_database_as_a_storage_error(self) -> None:
        # A disk that is full when the spool opens is not a broken spool: the
        # entry has to name the storage condition (ADR 0015) even though it
        # still fails the setup, which is what makes Home Assistant retry it.
        # The error is a real SQLITE_FULL produced by real storage and then
        # replayed through the open path: `max_page_count` is per connection, so
        # a fresh connection cannot be pinned, and a filesystem cannot be filled
        # from a test. The natural occurrence - a spool opened on a filesystem
        # that really is full - is measured by benchmarks/spool_pressure.py,
        # which runs in a container with its own tmpfs (`startup_on_full_filesystem`).
        source = Path(self.temporary_directory.name) / "full.db"
        raw = sqlite3.connect(source, isolation_level=None)
        raw.execute("PRAGMA max_page_count = 8")
        raw.execute("CREATE TABLE t (x BLOB)")
        full_error: sqlite3.OperationalError | None = None
        try:
            raw.execute("INSERT INTO t VALUES (?)", (b"x" * 60_000,))
        except sqlite3.OperationalError as exc:
            full_error = exc
        raw.close()
        self.assertIsNotNone(full_error)
        self.assertEqual(
            getattr(full_error, "sqlite_errorcode", None), sqlite3.SQLITE_FULL
        )
        with patch(
            "custom_components.hass_questdb_writer.spool.sqlite3.connect",
            side_effect=full_error,
        ):
            with self.assertRaises(SpoolDiskFullError):
                self.open_spool()

    def test_out_of_space_codes_are_classified_only_with_no_free_space(self) -> None:
        # A filesystem with nothing left does not answer SQLITE_FULL when the
        # database file itself cannot be created: it answers CANTOPEN or IOERR -
        # the same codes a bad path or a permission problem produces. The free
        # space the caller measured is what tells them apart, which is why the
        # classification takes it as an argument and why this behaviour was found
        # on a real tmpfs (benchmarks/spool_pressure.py) rather than in a test.

        class _CannotOpen(sqlite3.OperationalError):
            sqlite_errorcode = sqlite3.SQLITE_CANTOPEN

        class _CannotOpenExtended(sqlite3.OperationalError):
            # What a real filesystem reports: the extended code, not the primary
            # one. Matching primary codes only would have missed it.
            sqlite_errorcode = sqlite3.SQLITE_CANTOPEN | (15 << 8)

        error = _CannotOpen("unable to open database file")
        self.assertIsInstance(
            classify_storage_error(error, "open", 0), SpoolDiskFullError
        )
        self.assertIsInstance(
            classify_storage_error(error, "open", 8 * 1024 * 1024), SpoolError
        )
        self.assertEqual(type(classify_storage_error(error, "open")), SpoolError)
        self.assertIsInstance(
            classify_storage_error(_CannotOpenExtended("unable to open"), "open", 0),
            SpoolDiskFullError,
        )

    def test_reclaim_returns_space_to_the_filesystem(self) -> None:
        # The tests above measure the file; this measures the filesystem - what
        # the guard reads and what the operator sees - which a mocked usage
        # source cannot show. The bound is deliberately loose: the filesystem
        # also accounts for the WAL and for whatever else runs next to it.
        spool = self.open_spool(
            max_pending_rows=5_000,
            max_pending_bytes=10_000_000,
            max_event_bytes=4_096,
            max_dead_letter_bytes=10_000_000,
        )
        with spool:
            self._fill_and_deliver(spool, rows=4_000)
            page_size = spool._db.execute("PRAGMA page_size").fetchone()[0]
            before = shutil.disk_usage(self.path.parent).free
            reclaim = spool.reclaim(max_pages=4_096)
            after = shutil.disk_usage(self.path.parent).free
        freed_bytes = reclaim.freed_pages * page_size
        self.assertGreater(freed_bytes, 1_000_000)
        self.assertGreaterEqual(after - before, freed_bytes // 2)

    def test_the_full_database_recipe_leaves_no_room_for_the_payload(self) -> None:
        # The recipe is empirical (a small page cap plus a payload that cannot
        # fit the WAL), so it asserts its own precondition instead of trusting
        # the SQLite version to behave the same way: SQLite refuses to cap the
        # database below its current size, so what matters is the room left
        # between the cap and the pages already in use.
        with self.open_spool() as spool:
            spool._db.execute("PRAGMA max_page_count = 8")
            page_size = spool._db.execute("PRAGMA page_size").fetchone()[0]
            cap = spool._db.execute("PRAGMA max_page_count").fetchone()[0]
            pages = spool._db.execute("PRAGMA page_count").fetchone()[0]
        self.assertGreaterEqual(cap, pages)
        self.assertLess((cap - pages) * page_size, 60_000)

    def _fill_and_deliver(self, spool: SQLiteSpool, *, rows: int) -> None:
        payload = b"x" * 2_011
        for start in range(0, rows, 100):
            spool.enqueue_many(
                tuple(
                    NewSpoolEvent(f"event-{index}", payload, index)
                    for index in range(start, min(start + 100, rows))
                )
            )
        batch = spool.peek_batch(max_rows=rows, max_bytes=10_000_000)
        self.assertEqual(len(batch), rows)
        spool.mark_delivered(tuple(record.sequence for record in batch))


if __name__ == "__main__":
    unittest.main()

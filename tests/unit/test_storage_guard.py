"""Tests for the spool filesystem free-space guard."""

from __future__ import annotations

from pathlib import Path
import unittest

from custom_components.hass_questdb_writer.storage_guard import (
    FilesystemGuard,
    StorageStatus,
    StorageUsage,
    reserve_bytes_for,
    validate_free_space_settings,
)

_GIB = 1_024 * 1_024 * 1_024


class FakeFilesystem:
    """A filesystem whose numbers the test chooses, including no numbers."""

    def __init__(
        self, *, total_bytes: int = 0, free_bytes: int = 0, error: OSError | None = None
    ) -> None:
        self.total_bytes = total_bytes
        self.free_bytes = free_bytes
        self.error = error
        self.calls: list[Path] = []

    def usage(self, path: Path) -> StorageUsage:
        self.calls.append(path)
        if self.error is not None:
            raise self.error
        return StorageUsage(total_bytes=self.total_bytes, free_bytes=self.free_bytes)


class ReserveTests(unittest.TestCase):
    def test_absolute_floor_wins_on_a_small_filesystem(self) -> None:
        # 5% of 16 GiB is 819 MiB, so the 512 MiB floor is not the binding side.
        self.assertEqual(
            reserve_bytes_for(16 * _GIB, min_free_bytes=512 * 1_024 * 1_024,
                              min_free_ratio=0.05),
            858_993_460,
        )

    def test_floor_wins_when_the_share_is_smaller(self) -> None:
        self.assertEqual(
            reserve_bytes_for(1 * _GIB, min_free_bytes=512 * 1_024 * 1_024,
                              min_free_ratio=0.05),
            512 * 1_024 * 1_024,
        )

    def test_share_wins_on_a_large_filesystem(self) -> None:
        self.assertEqual(
            reserve_bytes_for(1_024 * _GIB, min_free_bytes=512 * 1_024 * 1_024,
                              min_free_ratio=0.05),
            54_975_581_389,
        )

    def test_rounds_the_share_up(self) -> None:
        # The reserve is a floor, never a fraction of a byte short of it.
        self.assertEqual(
            reserve_bytes_for(999, min_free_bytes=0, min_free_ratio=0.5), 500
        )
        self.assertEqual(
            reserve_bytes_for(1_000_000_000, min_free_bytes=0, min_free_ratio=0.05),
            50_000_000,
        )

    def test_zero_ratio_leaves_only_the_floor(self) -> None:
        self.assertEqual(
            reserve_bytes_for(1_024 * _GIB, min_free_bytes=1_024,
                              min_free_ratio=0.0),
            1_024,
        )


class GuardTests(unittest.TestCase):
    def guard(self, filesystem: FakeFilesystem, **overrides: object) -> FilesystemGuard:
        options: dict[str, object] = {
            "min_free_bytes": 512 * 1_024 * 1_024,
            "min_free_ratio": 0.05,
        }
        options.update(overrides)
        return FilesystemGuard(usage_source=filesystem.usage, **options)  # type: ignore[arg-type]

    def test_measures_the_directory_that_holds_the_spool(self) -> None:
        filesystem = FakeFilesystem(total_bytes=64 * _GIB, free_bytes=10 * _GIB)
        status = self.guard(filesystem).check(Path("/config/.storage/app/entry.db"))
        self.assertEqual(filesystem.calls, [Path("/config/.storage/app")])
        self.assertFalse(status.blocked)
        self.assertEqual(status.free_bytes, 10 * _GIB)

    def test_blocks_once_the_reserve_is_consumed(self) -> None:
        filesystem = FakeFilesystem(total_bytes=64 * _GIB, free_bytes=1 * _GIB)
        status = self.guard(filesystem).check(Path("/config/spool.db"))
        # 5% of 64 GiB = 3.2 GiB is the binding reserve.
        self.assertEqual(status.reserve_bytes, 3_435_973_837)
        self.assertTrue(status.blocked)

    def test_equal_free_space_and_reserve_is_not_blocked(self) -> None:
        status = self.guard(
            FakeFilesystem(total_bytes=0, free_bytes=512 * 1_024 * 1_024)
        ).check(Path("/config/spool.db"))
        self.assertFalse(status.blocked)

    def test_an_unreadable_filesystem_blocks(self) -> None:
        filesystem = FakeFilesystem(error=OSError("Stale file handle"))
        status = self.guard(filesystem).check(Path("/config/spool.db"))
        self.assertTrue(status.blocked)
        self.assertEqual(status.error, "OSError: Stale file handle")
        self.assertEqual(status.free_bytes, 0)


class ValidationTests(unittest.TestCase):
    def test_rejects_negative_bytes(self) -> None:
        with self.assertRaises(ValueError):
            validate_free_space_settings(min_free_bytes=-1, min_free_ratio=0.0)

    def test_rejects_a_ratio_of_one_or_more(self) -> None:
        for ratio in (1.0, 1.5):
            with self.assertRaises(ValueError):
                validate_free_space_settings(
                    min_free_bytes=0, min_free_ratio=ratio
                )

    def test_rejects_booleans_and_non_finite_ratios(self) -> None:
        with self.assertRaises(ValueError):
            validate_free_space_settings(min_free_bytes=True, min_free_ratio=0.0)
        with self.assertRaises(ValueError):
            validate_free_space_settings(
                min_free_bytes=0, min_free_ratio=float("nan")
            )

    def test_rejects_a_non_callable_usage_source(self) -> None:
        with self.assertRaises(ValueError):
            FilesystemGuard(
                min_free_bytes=0, min_free_ratio=0.0, usage_source=None  # type: ignore[arg-type]
            )

    def test_status_reports_its_own_verdict(self) -> None:
        blocked = StorageStatus(
            free_bytes=1, total_bytes=10, reserve_bytes=2
        )
        self.assertTrue(blocked.blocked)
        self.assertFalse(
            StorageStatus(free_bytes=2, total_bytes=10, reserve_bytes=2).blocked
        )


if __name__ == "__main__":
    unittest.main()

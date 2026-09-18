"""Filesystem free-space guard for the durable spool.

The spool's own capacity limits count serialized payload bytes: they cannot see
SQLite pages, the WAL, or the recorder, logs and backups that share the same
filesystem. This module turns the one signal those counters cannot produce - the
free space left on the filesystem holding the spool - into an explicit,
testable value. It runs on the worker thread and never touches Home Assistant
state.

Measured cost: one ``statvfs`` per persist attempt. At the measured production
profile (100-event batches, ~2 ms per durable transaction) that is well below
one percent of the persist path.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import math
import shutil
from pathlib import Path


@dataclass(frozen=True, slots=True)
class StorageUsage:
    """The two numbers the guard needs from the filesystem."""

    total_bytes: int
    free_bytes: int


@dataclass(frozen=True, slots=True)
class StorageStatus:
    """One free-space measurement of the spool filesystem."""

    free_bytes: int
    total_bytes: int
    reserve_bytes: int
    error: str | None = None

    @property
    def blocked(self) -> bool:
        """True when the reserve is consumed, or free space cannot be read.

        An unreadable filesystem blocks persistence: the writer cannot prove it
        has room, and guessing "yes" is what turns a full disk into a lost
        queue. The guard therefore fails closed.
        """
        return self.error is not None or self.free_bytes < self.reserve_bytes


def reserve_bytes_for(
    total_bytes: int, *, min_free_bytes: int, min_free_ratio: float
) -> int:
    """Return how much free space the guard keeps untouched.

    The larger of an absolute floor and a share of the filesystem: the floor
    protects small disks, where 5% would be nothing, and the share protects
    large ones, where the floor would leave the writer free to fill terabytes.
    """
    return max(min_free_bytes, math.ceil(total_bytes * min_free_ratio))


def _shutil_usage(path: Path) -> StorageUsage:
    """Read the filesystem that holds ``path`` with :func:`shutil.disk_usage`."""
    usage = shutil.disk_usage(path)
    return StorageUsage(total_bytes=usage.total, free_bytes=usage.free)


def validate_free_space_settings(
    *, min_free_bytes: int, min_free_ratio: float
) -> None:
    """Reject reserve settings the guard could not act on."""
    if (
        not isinstance(min_free_bytes, int)
        or isinstance(min_free_bytes, bool)
        or min_free_bytes < 0
    ):
        raise ValueError("min_free_bytes must be a non-negative integer")
    if (
        not isinstance(min_free_ratio, (int, float))
        or isinstance(min_free_ratio, bool)
        or not math.isfinite(min_free_ratio)
        or not 0 <= min_free_ratio < 1
    ):
        raise ValueError("min_free_ratio must be finite and in [0, 1)")


class FilesystemGuard:
    """Measure the free space the spool is allowed to consume."""

    def __init__(
        self,
        *,
        min_free_bytes: int,
        min_free_ratio: float,
        usage_source: Callable[[Path], StorageUsage] = _shutil_usage,
    ) -> None:
        validate_free_space_settings(
            min_free_bytes=min_free_bytes, min_free_ratio=min_free_ratio
        )
        if not callable(usage_source):
            raise ValueError("usage_source must be callable")
        self._min_free_bytes = min_free_bytes
        self._min_free_ratio = float(min_free_ratio)
        self._usage_source = usage_source

    def check(self, spool_path: Path) -> StorageStatus:
        """Measure the filesystem holding ``spool_path``.

        Never raises: an unreadable filesystem comes back as a blocked status
        carrying the reason, so the caller has one decision to make instead of
        an exception to swallow.
        """
        try:
            usage = self._usage_source(spool_path.parent)
        except OSError as exc:
            return StorageStatus(
                free_bytes=0,
                total_bytes=0,
                reserve_bytes=self._min_free_bytes,
                error=f"{type(exc).__name__}: {exc}",
            )
        total = int(usage.total_bytes)
        free = int(usage.free_bytes)
        return StorageStatus(
            free_bytes=free,
            total_bytes=total,
            reserve_bytes=reserve_bytes_for(
                total,
                min_free_bytes=self._min_free_bytes,
                min_free_ratio=self._min_free_ratio,
            ),
        )

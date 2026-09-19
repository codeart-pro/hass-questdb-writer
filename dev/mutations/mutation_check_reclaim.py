#!/usr/bin/env python3
"""Mutation check for the spool reclamation work.

Runs INSIDE the container against /tmp/run. Each mutation removes one piece of
the fix; the test that exists to protect that piece must go red.
"""
from __future__ import annotations

import pathlib
import re
import subprocess

TREE = pathlib.Path("/tmp/run")
PKG = TREE / "custom_components/hass_questdb_writer"

MUTATIONS = [
    (
        "new spools stop using incremental auto-vacuum",
        PKG / "spool.py",
        'connection.execute("PRAGMA auto_vacuum = INCREMENTAL")\n            self._auto_vacuum',
        'connection.execute("PRAGMA auto_vacuum = NONE")\n            self._auto_vacuum',
        ["tests/unit/test_spool.py", "-k", "incremental_auto_vacuum"],
    ),
    (
        "the vacuum statement is not drained",
        PKG / "spool.py",
        'f"PRAGMA incremental_vacuum({max_pages})"\n                ).fetchall()',
        'f"PRAGMA incremental_vacuum({max_pages})"\n                )',
        ["tests/unit/test_spool.py", "-k", "reclaim_returns"],
    ),
    (
        "the WAL is not truncated after vacuuming",
        PKG / "spool.py",
        "            wal_truncated = _truncate_wal(connection)\n        except (sqlite3.Error, SpoolClosedError) as exc:\n            return SpoolReclaim(\n                auto_vacuum=self._auto_vacuum,\n                freed_pages=freed_pages,",
        "            wal_truncated = False\n        except (sqlite3.Error, SpoolClosedError) as exc:\n            return SpoolReclaim(\n                auto_vacuum=self._auto_vacuum,\n                freed_pages=freed_pages,",
        ["tests/unit/test_spool.py", "-k", "reclaim_returns"],
    ),
    (
        "the worker never asks the spool to reclaim",
        PKG / "worker.py",
        "            if self._reclaim_spool(spool):\n                status = self._check_storage(spool)",
        "            if False:\n                status = self._check_storage(spool)",
        ["tests/unit/test_worker.py", "-k", "reclaim"],
    ),
    (
        "storage_blocks counts attempts again",
        PKG / "worker.py",
        "            first_attempt = self._storage_episode is None",
        "            first_attempt = True",
        ["tests/unit/test_worker.py", "-k", "counts_pauses_not_attempts"],
    ),
    (
        "the shutdown flush spins again",
        PKG / "worker.py",
        "                await asyncio.sleep(remaining)",
        "                await asyncio.sleep(0)",
        ["tests/unit/test_worker.py", "-k", "does_not_spin"],
    ),
    # Added while answering the external review of this change set.
    (
        "a delivery-side recovery no longer closes the pause",
        PKG / "worker.py",
        "        self._resume_after_storage_block()\n        with self._lock:\n            self._delivered_events",
        "        with self._lock:\n            self._delivered_events",
        ["tests/unit/test_worker.py", "-k", "delivery_side"],
    ),
    (
        "startup storage errors lose their classification",
        PKG / "spool.py",
        "            raise classify_storage_error(\n                exc,\n                \"failed to initialize SQLite spool\",\n                self._free_bytes(),\n            ) from exc",
        '            raise SpoolError("failed to initialize SQLite spool") from exc',
        ["tests/unit/test_spool.py", "-k", "open_classifies"],
    ),
    (
        "the reserve blocks at equality too",
        PKG / "storage_guard.py",
        "return self.error is not None or self.free_bytes < self.reserve_bytes",
        "return self.error is not None or self.free_bytes <= self.reserve_bytes",
        ["tests/unit/test_storage_guard.py", "-k", "reserve"],
    ),
]


def main() -> None:
    originals: dict[pathlib.Path, str] = {}
    try:
        for name, path, needle, replacement, selection in MUTATIONS:
            original = originals.setdefault(path, path.read_text())
            mutated = original.replace(needle, replacement, 1)
            if mutated == original:
                print(f"{name}: SKIPPED (anchor not found)")
                continue
            path.write_text(mutated)
            result = subprocess.run(
                ["python3", "-m", "pytest", *selection, "-q", "--no-header"],
                cwd=TREE,
                capture_output=True,
                text=True,
            )
            print(f"{name}: {_verdict(result)} :: {_summary(result.stdout)}")
    finally:
        # In a `finally`, so an interrupt in the middle of a run still leaves the
        # tree as it was: a checker that can corrupt the code it checks is worse
        # than no checker.
        for path, original in originals.items():
            path.write_text(original)
        print("restored:", all(p.read_text() == o for p, o in originals.items()))


_FAILED = re.compile(r"\b\d+ failed\b")
_ERRORS = re.compile(r"\b\d+ errors?\b")


def _summary(output: str) -> str:
    return (output.strip().splitlines() or ["<no output>"])[-1]


def _verdict(result: subprocess.CompletedProcess[str]) -> str:
    """CAUGHT only when the selected tests ran and failed.

    A non-zero exit code is not enough: an import error, a collection error or a
    wrong test path exits non-zero without a single assertion having run, which
    would report an infrastructure failure as a killed mutant. The summary line is
    matched by shape (`N failed`, `N errors`) instead of by the substring "error",
    which any assertion message could contain.
    """
    summary = _summary(result.stdout + result.stderr)
    if result.returncode == 0:
        return "MISSED (still green!)"
    if _ERRORS.search(summary) or not _FAILED.search(summary):
        return "UNDECIDED (the selection did not run and fail)"
    return "CAUGHT (red)"


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Mutation check for the reauth/reconfigure identity work.

Runs INSIDE the container against /tmp/run. A mutation that leaves the target
tests green means the test proves nothing about the behaviour.
"""
from __future__ import annotations

import pathlib
import subprocess

TREE = pathlib.Path("/tmp/run")
FLOW = TREE / "custom_components/hass_questdb_writer/config_flow.py"
ORIGINAL = FLOW.read_text()

MUTATIONS = [
    (
        "reauth destination guard removed",
        """            if self.source == SOURCE_REAUTH and source_entry is not None:
                # Reauthentication repairs credentials only (ADR-0012): the
                # destination comes from the entry, so neither the form nor a
                # hand-made submit can move the writer to another host or table.
                user_input = {
                    **source_entry.data,
                    CONF_USERNAME: user_input.get(CONF_USERNAME, ""),
                    CONF_PASSWORD: user_input.get(CONF_PASSWORD, ""),
                }
""",
        "",
        ["tests/unit/test_config_flow.py", "-k", "cannot_move_the_entry"],
    ),
    (
        "reconfigure unique_id update removed",
        "                        unique_id=unique_id,\n",
        "",
        ["tests/unit/test_config_flow.py", "-k", "moves_the_unique_id"],
    ),
    (
        "duplicate-destination abort removed",
        """                    if any(
                        entry.unique_id == unique_id""",
        """                    if False and any(
                        entry.unique_id == unique_id""",
        ["tests/unit/test_config_flow.py", "-k", "another_entry_owns"],
    ),
]


def main() -> None:
    try:
        for name, needle, replacement, selection in MUTATIONS:
            mutated = ORIGINAL.replace(needle, replacement, 1)
            if mutated == ORIGINAL:
                print(f"{name}: SKIPPED (anchor not found)")
                continue
            FLOW.write_text(mutated)
            result = subprocess.run(
                ["python3", "-m", "pytest", *selection, "-q", "--no-header"],
                cwd=TREE, capture_output=True, text=True,
            )
            print(f"{name}: {_verdict(result)} :: {_summary(result.stdout)}")
    finally:
        # The original text is restored from memory in a `finally`, so an
        # interrupt in the middle of a mutation cannot leave the tree mutated.
        FLOW.write_text(ORIGINAL)
        print("config_flow.py restored:", FLOW.read_text() == ORIGINAL)


def _summary(output: str) -> str:
    return (output.strip().splitlines() or ["<no output>"])[-1]


def _verdict(result: subprocess.CompletedProcess[str]) -> str:
    """CAUGHT only when the selected tests ran and failed.

    A non-zero exit code is not enough: an import error, a collection error or a
    wrong test path exits non-zero without a single assertion having run, which
    would report an infrastructure failure as a killed mutant.
    """
    summary = _summary(result.stdout + result.stderr).lower()
    if result.returncode == 0:
        return "MISSED (still green!)"
    infrastructure_markers = ("error", "no tests ran", "usage error", "interrupted")
    if any(marker in summary for marker in infrastructure_markers):
        return "UNDECIDED (infrastructure failure, not a killed mutant)"
    if "failed" in summary:
        return "CAUGHT (red)"
    return "UNDECIDED (unrecognised outcome)"


if __name__ == "__main__":
    main()

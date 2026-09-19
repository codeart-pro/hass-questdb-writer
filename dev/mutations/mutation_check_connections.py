#!/usr/bin/env python3
"""Mutation check: do the connection-sharing tests catch a regression?

Runs INSIDE the container against /tmp/run. Each mutation reintroduces the kind
of drift the change removed; the target test must go red.
"""
from __future__ import annotations

import pathlib
import subprocess

TREE = pathlib.Path("/tmp/run")
PKG = TREE / "custom_components/hass_questdb_writer"

MUTATIONS = [
    (
        "sensor ignores the configured options",
        PKG / "sensor.py",
        "connection_configuration(self._entry_data, self._entry_options)",
        "connection_configuration(self._entry_data, {})",
        ["tests/unit/test_sensor_platform.py", "-k", "connection_settings"],
    ),
    (
        "diagnostics ignores the configured options",
        PKG / "diagnostics.py",
        "connection_configuration(data, entry.options)",
        "connection_configuration(data, {})",
        ["tests/unit/test_diagnostics.py", "-k", "connection_settings"],
    ),
    (
        "config flow ignores the entry options",
        PKG / "config_flow.py",
        "options = dict(source.options) if source is not None else {}",
        "options = {}",
        ["tests/unit/test_config_flow.py", "-k", "probe_uses_the_entry_options"],
    ),
    (
        "self-signed flag dropped",
        PKG / "runtime.py",
        "tls_self_signed=bool(data.get(CONF_TLS_SELF_SIGNED, False)),",
        "tls_self_signed=False,",
        ["tests/unit/test_init.py", "-k", "connection_configuration"],
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
                cwd=TREE, capture_output=True, text=True,
            )
            print(f"{name}: {_verdict(result)} :: {_summary(result.stdout)}")
    finally:
        # In a `finally`, so an interrupt in the middle of a run still leaves the
        # tree as it was: a checker that can corrupt the code it checks is worse
        # than no checker.
        for path, original in originals.items():
            path.write_text(original)
        print("restored:", all(p.read_text() == o for p, o in originals.items()))


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

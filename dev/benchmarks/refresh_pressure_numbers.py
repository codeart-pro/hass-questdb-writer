#!/usr/bin/env python3
"""Render (or check) the two result tables of the pressure document from the artifacts.

Numbers typed by hand drift away from the artifacts they claim to come from, so
they are rendered from the JSON files in `dev/scratch/pressure-run/` instead. The
`--check` mode compares without writing and exits non-zero when the document has
drifted, which is what makes the claim checkable in a review.

    python3 dev/benchmarks/refresh_pressure_numbers.py [--check]
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[2]
RUNS = REPO / "dev" / "scratch" / "pressure-run"
DOC = REPO / "docs" / "benchmarks" / "spool-pressure.md"


def load(name: str) -> dict:
    return json.loads((RUNS / f"result-{name}.json").read_text())


def percent(cpu: float | None, seconds: float) -> str:
    if cpu is None or not seconds:
        return "—"
    return f"{cpu / seconds * 100:.1f} %"


def rows() -> dict[str, str]:
    b, f = load("baseline-64m"), load("fixed-64m")
    p61, p13 = load("fixed-rate61"), load("fixed-rate13")
    out = {}

    def pair(label: str, left: str, right: str) -> None:
        out[label] = f"| {label} | {left} | {right} |"

    pair(
        "Accepted events",
        f"{b['identity']['accepted_events']:,}".replace(",", ","),
        f"{f['identity']['accepted_events']:,}".replace(",", ","),
    )
    pair(
        "Persisted events",
        f"{b['identity']['persisted_events']:,}".replace(",", ","),
        f"{f['identity']['persisted_events']:,}".replace(",", ","),
    )
    pair(
        "Distinct events in QuestDB",
        f"{b['identity']['distinct_events_in_questdb']:,}".replace(",", ","),
        f"{f['identity']['distinct_events_in_questdb']:,}".replace(",", ",")
        + f" (+ {f['identity']['events_still_in_spool']:,}".replace(",", ",")
        + " still durable in the spool)",
    )
    pair(
        "Accepted events unaccounted for anywhere",
        f"**{b['identity']['unaccounted_events']:,}**".replace(",", ","),
        f"**{f['identity']['unaccounted_events']:,}**".replace(",", ","),
    )
    pair(
        "Accepted ids verified at the destination and missing",
        f"**{b['identity']['missing_events']:,} of {b['identity']['verified_ids']:,}**".replace(",", ","),
        f"**{f['identity']['missing_events']:,} of {f['identity']['verified_ids']:,}**".replace(",", ","),
    )
    pair(
        "Duplicate rows in the destination",
        f"{b['identity']['duplicate_rows_in_table']}",
        f"{f['identity']['duplicate_rows_in_table']}",
    )
    pair(
        "Pauses ended by the writer (`storage_recoveries`)",
        f"**{b['recovery']['storage_recoveries']}**",
        f"**{f['recovery']['storage_recoveries']}**",
    )
    pair(
        "`storage_blocks` in the blocked window",
        f"{b['blocked_steady_state']['storage_blocks_at_exit']} (one pause, counted "
        f"per attempt: ~{b['blocked_steady_state']['storage_blocks_per_second']:.0f}/s)",
        f"{f['blocked_steady_state']['storage_blocks_at_exit']} "
        f"({f['recovery']['storage_recoveries']} pauses in the run)",
    )
    pair(
        "CPU in the blocked state, in the window",
        f"{b['blocked_steady_state']['cpu_seconds_in_blocked_state']:.2f} s over "
        f"{b['blocked_steady_state']['blocked_seconds_in_window']:.2f} s = "
        f"**{percent(b['blocked_steady_state']['cpu_seconds_in_blocked_state'], b['blocked_steady_state']['blocked_seconds_in_window'])} of one core**",
        f"{f['blocked_steady_state']['cpu_seconds_in_blocked_state']:.2f} s over "
        f"{f['blocked_steady_state']['blocked_seconds_in_window']:.2f} s = "
        f"**{percent(f['blocked_steady_state']['cpu_seconds_in_blocked_state'], f['blocked_steady_state']['blocked_seconds_in_window'])}**",
    )
    pair(
        "A spool opened while the filesystem is full",
        f"`{b['startup_on_full_filesystem']['raised']}: failed to initialize SQLite spool` (**unclassified**)",
        f"**`{f['startup_on_full_filesystem']['raised']}: database or disk is full`**",
    )
    pair(
        "Shutdown with a flush that could not proceed",
        f"{b['shutdown']['seconds']:.1f} s, **{b['shutdown']['cpu_percent_of_one_core']:.1f} %** of one core, "
        f"`stopped_cleanly: {str(b['shutdown']['stopped_cleanly']).lower()}`",
        f"**{f['shutdown']['seconds']:.1f} s, {f['shutdown']['cpu_percent_of_one_core']:.1f} %**, "
        f"`stopped_cleanly: {str(f['shutdown']['stopped_cleanly']).lower()}`",
    )
    pair(
        "Draining after the destination returned",
        f"{b['recovery']['seconds_to_drain']:.1f} s",
        f"{f['recovery']['seconds_to_drain']:.1f} s",
    )
    pair(
        "Disk cost",
        f"{b['disk_cost']['bytes_per_pending_byte']:.2f} B per payload byte, "
        f"{b['disk_cost']['bytes_per_pending_row']:,.0f} B per row".replace(",", ","),
        f"{f['disk_cost']['bytes_per_pending_byte']:.2f} B per payload byte, "
        f"{f['disk_cost']['bytes_per_pending_row']:,.0f} B per row".replace(",", ","),
    )

    def pair_rate(label: str, left: str, right: str) -> None:
        out[f"rate:{label}"] = f"| {label} | {left} | {right} |"

    pair_rate(
        "Time in the blocked state inside the window",
        f"{p61['blocked_steady_state']['blocked_seconds_in_window']:.1f} s of 20 s",
        "**0 s**"
        if not p13["blocked_steady_state"]["blocked_seconds_in_window"]
        else f"{p13['blocked_steady_state']['blocked_seconds_in_window']:.1f} s of 20 s",
    )
    pair_rate(
        "CPU in the blocked state",
        f"{p61['blocked_steady_state']['cpu_seconds_in_blocked_state']:.2f} s over "
        f"{p61['blocked_steady_state']['blocked_seconds_in_window']:.1f} s = "
        f"**{percent(p61['blocked_steady_state']['cpu_seconds_in_blocked_state'], p61['blocked_steady_state']['blocked_seconds_in_window'])} of one core**",
        "— (the window contains no pause)"
        if not p13["blocked_steady_state"]["blocked_seconds_in_window"]
        else f"**{percent(p13['blocked_steady_state']['cpu_seconds_in_blocked_state'], p13['blocked_steady_state']['blocked_seconds_in_window'])}**",
    )
    pair_rate(
        "New pauses inside the window",
        f"{p61['blocked_steady_state']['new_pauses_in_window']} "
        f"({p61['blocked_steady_state']['storage_blocks_at_exit']} in the whole run)",
        f"{p13['blocked_steady_state']['new_pauses_in_window']} "
        f"({p13['blocked_steady_state']['storage_blocks_at_exit']} in the whole run, before the window)",
    )
    pair_rate(
        "Time in delivery `retry_wait` in the run",
        f"{p61['writer_states']['retry_wait']['seconds']:.1f} s at "
        f"{p61['writer_states']['retry_wait']['cpu_percent_of_one_core']:.1f} % of one core",
        f"{p13['writer_states']['retry_wait']['seconds']:.1f} s at "
        f"{p13['writer_states']['retry_wait']['cpu_percent_of_one_core']:.1f} % of one core",
    )
    pair_rate(
        "Accepted / delivered",
        f"{p61['identity']['accepted_events']:,}".replace(",", ",")
        + f" / {p61['identity']['distinct_events_in_questdb']:,}".replace(",", ",")
        + (
            f" (+ {p61['identity']['events_still_in_spool']:,}".replace(",", ",")
            + " durable)"
            if p61["identity"]["events_still_in_spool"]
            else ""
        ),
        f"{p13['identity']['accepted_events']:,}".replace(",", ",")
        + f" / {p13['identity']['distinct_events_in_questdb']:,}".replace(",", ","),
    )
    pair_rate(
        "Shutdown",
        f"{p61['shutdown']['seconds']:.2f} s, {p61['shutdown']['cpu_percent_of_one_core']:.1f} % of one core",
        f"{p13['shutdown']['seconds']:.3f} s",
    )
    return out


def replace_row(text: str, label: str, row: str) -> str:
    prefix = f"| {label} |"
    lines = text.splitlines(keepends=True)
    for index, line in enumerate(lines):
        if line.startswith(prefix):
            lines[index] = row + "\n"
            return "".join(lines)
    raise SystemExit(f"row not found: {label}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="report drift without rewriting the document",
    )
    arguments = parser.parse_args()

    text = DOC.read_text(encoding="utf-8")
    rendered = text
    for label, row in rows().items():
        label = label.removeprefix("rate:")
        rendered = replace_row(rendered, label, row)
    if arguments.check:
        if rendered != text:
            print("the pressure tables have drifted from the artifacts:")
            for line in rendered.splitlines():
                if line not in text.splitlines():
                    print("  expected:", line)
            raise SystemExit(1)
        print("the pressure tables match the artifacts")
        return
    DOC.write_text(rendered, encoding="utf-8")
    print("rows refreshed from the run artifacts")


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Compare QuestDB ILP/HTTP and ILP/TCP with identical payloads.

This is a transport benchmark. It deliberately uses the Python standard
library so it can run natively on development hosts where the pinned QuestDB
Python client wheel is unavailable. TCP send timing is not treated as a commit
acknowledgement; end-to-end timing continues until all rows are SQL-visible.
"""

from __future__ import annotations

import argparse
import http.client
import json
import math
import socket
import statistics
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Result:
    transport: str
    batch_size: int
    repeat: int
    rows: int
    payload_bytes: int
    send_seconds: float
    visible_seconds: float
    send_rows_per_second: float
    visible_rows_per_second: float
    operation_p50_ms: float
    operation_p95_ms: float


def percentile(values: list[float], fraction: float) -> float:
    """Return a nearest-rank percentile for a non-empty sample."""
    ordered = sorted(values)
    rank = max(1, math.ceil(fraction * len(ordered)))
    return ordered[rank - 1]


def sql(http_host: str, http_port: int, statement: str) -> dict:
    """Execute SQL through QuestDB's REST API."""
    query = urllib.parse.urlencode({"query": statement})
    with urllib.request.urlopen(
        f"http://{http_host}:{http_port}/exec?{query}", timeout=30
    ) as response:
        return json.load(response)


def row_count(http_host: str, http_port: int, table: str) -> int:
    """Read the current row count."""
    result = sql(http_host, http_port, f"select count() from {table}")
    return int(result["dataset"][0][0])


def wait_visible(
    http_host: str, http_port: int, table: str, expected: int, deadline: float
) -> None:
    """Wait until every sent row is query-visible."""
    while time.perf_counter() < deadline:
        if row_count(http_host, http_port, table) == expected:
            return
        time.sleep(0.01)
    actual = row_count(http_host, http_port, table)
    raise TimeoutError(f"{table}: expected {expected} rows, found {actual}")


def payload(table: str, start: int, count: int, base_ns: int) -> bytes:
    """Build deterministic ILP rows for one table."""
    lines = []
    for index in range(start, start + count):
        entity = index % 100
        state = index % 2
        timestamp = base_ns + index * 1_000
        lines.append(
            f'{table},entity_id=sensor.bench_{entity} '
            f'state="{state}",value={index / 10.0},event_id="event-{index}" '
            f"{timestamp}\n"
        )
    return "".join(lines).encode("utf-8")


def send_http(
    host: str, port: int, table: str, rows: int, batch_size: int, base_ns: int
) -> tuple[int, float, list[float]]:
    """Send batches over one persistent HTTP/1.1 connection."""
    connection = http.client.HTTPConnection(host, port, timeout=30)
    durations: list[float] = []
    sent_bytes = 0
    try:
        for start in range(0, rows, batch_size):
            body = payload(table, start, min(batch_size, rows - start), base_ns)
            sent_bytes += len(body)
            before = time.perf_counter()
            connection.request(
                "POST",
                "/write?precision=n",
                body=body,
                headers={"Content-Type": "text/plain"},
            )
            response = connection.getresponse()
            response_body = response.read()
            if response.status not in (200, 204):
                detail = response_body.decode("utf-8", errors="replace")
                raise RuntimeError(
                    f"HTTP {response.status}: {response.reason}: {detail}"
                )
            durations.append(time.perf_counter() - before)
    finally:
        connection.close()
    return sent_bytes, sum(durations), durations


def send_tcp(
    host: str, port: int, table: str, rows: int, batch_size: int, base_ns: int
) -> tuple[int, float, list[float]]:
    """Send batches over one persistent TCP connection."""
    durations: list[float] = []
    sent_bytes = 0
    with socket.create_connection((host, port), timeout=30) as connection:
        for start in range(0, rows, batch_size):
            body = payload(table, start, min(batch_size, rows - start), base_ns)
            sent_bytes += len(body)
            before = time.perf_counter()
            connection.sendall(body)
            durations.append(time.perf_counter() - before)
        connection.shutdown(socket.SHUT_WR)
    return sent_bytes, sum(durations), durations


def run_case(
    transport: str,
    batch_size: int,
    repeat: int,
    rows: int,
    host: str,
    http_port: int,
    tcp_port: int,
) -> Result:
    """Run one isolated case and remove its table afterward."""
    transport_token = "http" if transport == "http" else "tcp_"
    table = f"ilp_bench_{transport_token}_{batch_size}_{repeat}"
    sql(host, http_port, f"drop table if exists {table}")
    sql(
        host,
        http_port,
        f"create table {table} ("
        "entity_id symbol, state varchar, value double, event_id varchar, "
        "timestamp timestamp"
        ") timestamp(timestamp) partition by day wal",
    )
    base_ns = time.time_ns() - rows * 1_000
    started = time.perf_counter()
    try:
        if transport == "http":
            sent_bytes, send_seconds, operations = send_http(
                host, http_port, table, rows, batch_size, base_ns
            )
        else:
            sent_bytes, send_seconds, operations = send_tcp(
                host, tcp_port, table, rows, batch_size, base_ns
            )
        wait_visible(host, http_port, table, rows, time.perf_counter() + 30)
        visible_seconds = time.perf_counter() - started
        return Result(
            transport=transport,
            batch_size=batch_size,
            repeat=repeat,
            rows=rows,
            payload_bytes=sent_bytes,
            send_seconds=send_seconds,
            visible_seconds=visible_seconds,
            send_rows_per_second=rows / send_seconds,
            visible_rows_per_second=rows / visible_seconds,
            operation_p50_ms=statistics.median(operations) * 1_000,
            operation_p95_ms=percentile(operations, 0.95) * 1_000,
        )
    finally:
        sql(host, http_port, f"drop table if exists {table}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--http-port", type=int, default=19000)
    parser.add_argument("--tcp-port", type=int, default=19009)
    parser.add_argument("--rows", type=int, default=5_000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--batches", type=int, nargs="+", default=[1, 10, 100, 1000])
    args = parser.parse_args()

    results: list[Result] = []
    for repeat in range(args.repeats):
        for batch_size in args.batches:
            for transport in ("http", "tcp"):
                result = run_case(
                    transport,
                    batch_size,
                    repeat,
                    args.rows,
                    args.host,
                    args.http_port,
                    args.tcp_port,
                )
                results.append(result)
                print(json.dumps(asdict(result), sort_keys=True), flush=True)

    summaries = []
    for batch_size in args.batches:
        for transport in ("http", "tcp"):
            group = [
                result
                for result in results
                if result.batch_size == batch_size and result.transport == transport
            ]
            summaries.append(
                {
                    "transport": transport,
                    "batch_size": batch_size,
                    "repeats": len(group),
                    "rows_per_repeat": args.rows,
                    "payload_bytes": group[0].payload_bytes,
                    "median_send_rows_per_second": statistics.median(
                        result.send_rows_per_second for result in group
                    ),
                    "median_visible_rows_per_second": statistics.median(
                        result.visible_rows_per_second for result in group
                    ),
                    "median_visible_seconds": statistics.median(
                        result.visible_seconds for result in group
                    ),
                    "median_operation_p50_ms": statistics.median(
                        result.operation_p50_ms for result in group
                    ),
                    "median_operation_p95_ms": statistics.median(
                        result.operation_p95_ms for result in group
                    ),
                }
            )
    print(json.dumps({"summaries": summaries}, indent=2))


if __name__ == "__main__":
    main()

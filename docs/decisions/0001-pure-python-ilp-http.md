# ADR 0001: Use a pure-Python ILP/HTTP transport

Status: accepted

Date: 2026-08-29

## Context

The production QSS installation runs on amd64 and can install
`questdb==4.1.0`. The local Home Assistant development environment is ARM64
Alpine. Version 4.1.0 does not provide the required
`musllinux_1_2_aarch64` wheel, so Home Assistant cannot install the dependency.

Building the package locally also requires a Rust/Cargo native build toolchain.
Running the amd64 Home Assistant image under QEMU was tested and terminated with
`SIGSEGV`, so emulation is not an acceptable development or support strategy.

HASS QuestDB Writer is intended to be an independent, cross-platform Home
Assistant integration rather than an amd64-only wrapper around QSS.

## Decision

HASS QuestDB Writer will not have a runtime dependency on the native QuestDB
Python package. It will implement a focused, pure-Python ILP/HTTP transport:

- an ILP encoder owned by this project;
- one persistent HTTP connection per config entry;
- explicit single-table batches sent to QuestDB's `/write` endpoint;
- explicit request, connection, and retry timeouts;
- server-response parsing and error classification;
- integration-managed retry and durable spool semantics;
- HTTP/HTTPS and authentication support without logging secrets.

ILP/TCP is not part of the first production implementation. It remains only in
the standalone transport benchmark for comparison.

## Why HTTP

- QuestDB recommends ILP/HTTP for most ingestion workloads.
- HTTP returns schema and parsing errors; ILP/TCP does not provide equivalent
  error feedback.
- A request containing rows for one table can be handled transactionally.
- The local benchmark measured about 124,000 SQL-visible rows/s for 1,000-row
  HTTP batches, versus about 144,000 rows/s for TCP. The measured HTTP penalty
  does not justify losing acknowledgement and error feedback for this use case.
- A pure HTTP implementation removes the native-wheel platform restriction.

## Consequences

Positive:

- ARM64 and amd64 Home Assistant installations use the same code.
- No Rust/C toolchain or project-built binary wheels are required.
- Retry, acknowledgement, and spool behavior are under one explicit state
  machine.
- The local Home Assistant container can run the complete integration natively.

Negative:

- This project owns correct ILP escaping and type encoding.
- TLS, authentication, connection reuse, timeout behavior, and HTTP errors need
  dedicated tests.
- New ILP data types are not inherited automatically from the official client.
- We must maintain conformance tests against supported QuestDB releases.

## Required verification

Before production use, tests must cover:

- measurement, symbol, column, and string escaping;
- Unicode, quotes, backslashes, newlines, nulls, booleans, integers, and floats;
- non-finite numbers and unsupported values;
- timestamp precision and timezone conversion;
- payload and attribute size limits;
- HTTP 2xx, 400, 401, 403, 404, 408, 429, and 5xx handling;
- connect, read, and partial-write failures;
- response loss after a possible server commit;
- persistent connection reuse and reconnect;
- identical behavior on ARM64 and amd64 Home Assistant.

## References

- QuestDB ILP syntax:
  <https://questdb.com/docs/ingestion/ilp/advanced-settings/>
- QuestDB transport selection:
  <https://questdb.com/docs/connect/compatibility/ilp/overview/>
- Local benchmark:
  [../benchmarks/ilp-http-vs-tcp.md](../benchmarks/ilp-http-vs-tcp.md)

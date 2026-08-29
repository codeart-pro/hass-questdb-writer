# ILP/HTTP versus ILP/TCP transport benchmark

Date: 2026-08-29

Status: local transport benchmark; not a production capacity result.

## Question

Does ILP/HTTP impose enough throughput overhead to justify using ILP/TCP for
HASS QuestDB Writer?

## Environment

- QuestDB server image: `questdb/questdb:10.0.1`, native ARM64 container.
- Podman VM: 5 CPUs, 3.725 GiB memory.
- Client host: macOS ARM64, Python 3.9.6 standard library.
- Network path: macOS loopback to Podman forwarded ports.
- HTTP endpoint: persistent HTTP/1.1 connection to `/write?precision=n`.
- TCP endpoint: persistent socket to port 9009.
- Tables: identical predeclared WAL schemas, one temporary table per case.
- Payload: identical ILP shape and byte length for each HTTP/TCP pair.

The exact benchmark is reproducible with:

```text
python3 benchmarks/ilp_transport.py \
  --rows 20000 --repeats 5 --batches 100 1000
```

The benchmark creates and removes only temporary `ilp_bench_*` tables.

## Measurement rule

TCP `sendall()` only proves that bytes entered the local socket buffer. It does
not prove that QuestDB parsed or committed them. Therefore the comparison uses
end-to-end time from the first send until all expected rows are visible through
`select count()`.

HTTP operation latency is request/response latency. TCP operation latency is
only `sendall()` latency and must not be interpreted as acknowledgement latency.

## Primary result

Median of five repeats, 20,000 rows per repeat:

| Batch | Transport | SQL-visible rows/s | End-to-end time | Relative result |
|---:|---|---:|---:|---|
| 100 | HTTP | 36,201 | 0.552 s | baseline |
| 100 | TCP | 117,400 | 0.170 s | TCP 3.24x faster |
| 1,000 | HTTP | 124,003 | 0.161 s | baseline |
| 1,000 | TCP | 144,481 | 0.138 s | TCP 1.17x faster |

At batch 1,000, HTTP took about 16.5% longer end to end than TCP. At batch 100,
HTTP took about 3.24 times as long.

## Small-batch result

Median of three repeats, 5,000 rows per repeat:

| Batch | HTTP visible rows/s | TCP visible rows/s | Observation |
|---:|---:|---:|---|
| 1 | 446 | 29,485 | HTTP request per row is unsuitable |
| 10 | 3,263 | 52,431 | HTTP remains dominated by round trips |
| 100 | 23,134 | 26,935 | Short runs showed high WAL variability |
| 1,000 | 75,643 | 56,082 | Short run too variable for ranking |

These short cases motivated the longer five-repeat primary run. They are useful
for rejecting per-row HTTP flushing, not for estimating production capacity.

## Interpretation

1. HTTP performance depends primarily on batch size. Per-row or very small
   HTTP flushes are not acceptable.
2. With 1,000-row batches, the local end-to-end penalty was modest compared
   with the reliability benefits of server error feedback and transactional
   single-table requests.
3. TCP's measured socket-send rate reached millions of rows per second, but
   that number is not a delivery guarantee and is intentionally excluded from
   the decision metric.
4. The observed HTTP throughput is a local capacity ceiling, not evidence of
   the production event rate. Production rate and latency still need a
   read-only measurement before defaults are frozen.

## Limitations

- The pinned `questdb==4.1.0` package has no native macOS ARM wheel. Building it
  from source required an unavailable Rust/Cargo toolchain.
- An amd64 Home Assistant container under QEMU crashed with `SIGSEGV`, so no
  emulated result was accepted.
- The benchmark sends valid ILP directly with the Python standard library. It
  measures QuestDB transport and server behavior, not serialization or retry
  overhead inside the official Python Sender.
- The loopback/Podman network has much lower and more stable latency than a NAS
  reached over the production LAN.
- WAL scheduling produced visible run-to-run variance, especially in short
  cases. Medians are reported instead of best-case values.
- No concurrent queries or production-sized table were present.

## Decision impact

The result does not justify giving up HTTP error feedback for this integration.
ILP/HTTP remains the target transport, with explicit batching and a persistent
connection. ILP/TCP remains a benchmark comparison and is not part of the
initial runtime.

Batch-size and latency-trigger defaults remain undecided. They require:

1. a measured production state-change rate;
2. a benchmark of the project's pure-Python `IlpHttpTransport` inside HA;
3. outage/retry tests with the durable spool;
4. acceptable end-to-end freshness requirements.

## References

- QuestDB transport comparison:
  <https://questdb.com/docs/connect/compatibility/ilp/overview/>
- QuestDB HTTP server and ILP endpoints:
  <https://questdb.com/docs/configuration/http-server/>
- Accepted transport decision:
  [../decisions/0001-pure-python-ilp-http.md](../decisions/0001-pure-python-ilp-http.md)

# ADR 0016: Repeat a failed table-size query on a fresh connection

Status: accepted.

Date: 2026-10-11

## Context

ADR 0011 gave `QuestDbTableSizeSensor` its own 5-minute refresh timer and put one
consequence in writing: a failed query leaves the entity `unavailable` until the
next tick, i.e. for up to 5 minutes instead of up to the 30 s of a platform poll.
In the field that window turned out to be the normal state of the entity, not an
outage marker.

Measured on a home instance (Home Assistant 2026.10.0, QuestDB 10.0.1, one
config entry, table `hass`): the sensor reported `unavailable` three times in
50 minutes, every time with the same log line and every time back to its value
exactly one tick later —

```
WARNING (MainThread) [custom_components.hass_questdb_writer.sensor]
QuestDB table size query for hass failed: QuestDB exec request failed:
RemoteDisconnected; the sensor reports unavailable and retries in 0:05:00
```

QuestDB itself was healthy throughout (the worker delivered continuously, the
sink had no gap in its per-minute row counts), so the failure was the connection,
not the server or the query. The sensor holds ONE HTTP connection and reuses it.
That connection was already closed when the sensor sent its next request.

Two events produce that state and both are silent to the client:

- **QuestDB reaps an idle connection.** `http.net.idle.connection.timeout`
  defaults to 300000 ms — "TCP connection idle timeout in milliseconds. The
  connection is closed by the HTTP server when this timeout lapses" (HTTP server
  configuration reference) — and every response carries `Keep-Alive: timeout=5,
  max=10000`. The refresh interval is 300 s as well, so the next query knocks on
  a connection the server is closing at that very moment. Measured from the Home
  Assistant host against the instance above, with the transport's own request
  shape:

  - reuse after 60 s idle → HTTP 200 in 16 ms.
  - reuse after 300 s idle → `ConnectionResetError` (Errno 104, "Connection reset
    by peer").
- **A QuestDB restart, or any close on the path.** The same instance logged a
  restart window of 22 s (the worker saw `RemoteDisconnected` and then
  `gaierror` while the `questdb.local` mDNS advertisement was gone). The worker
  recovered in seconds because it retries from its spool. It logged no second
  window. The sensor's next tick failed on the now-dead connection, and the value
  returned 5 minutes later.

A server cannot tell an HTTP client that the client's connection is closed. The
client finds out when it sends the next request. Reconnecting is therefore only
possible as a repeat.

## Decision

- `IlpHttpTransport.exec_query(query, *, retries=0)` repeats a failure that is
  classified RETRYABLE, and every repetition runs on a FRESH connection
  (`self.close()` before it). A `PermanentIlpError` — a rejected statement, wrong
  credentials — is never repeated, and the repetitions are finite.
- `sensor.py` defines `TABLE_SIZE_QUERY_RETRIES = 1` and passes it to the
  table-size query, so one tick costs at most two requests: 289 per day instead
  of 288 in the worst case.
- The default stays `0`: the schema check, the diagnostics download and the
  config-flow validation use a short-lived connection and keep their
  single-attempt behaviour.

## Alternatives considered

- **Close the connection after every query**, so the sensor never holds an idle
  one: rejected as the primary fix — it removes the reaped-connection case but
  not the restart case (the server can still go away between connecting and
  requesting), and the reaped case is exactly what a repeat handles.
- **Refresh faster while the entity is unavailable** (e.g. every 30 s): rejected
  — it multiplies the query rate for a value that only grows, to make a growth
  monitor behave like a watchdog, which the README explicitly says it is not.
- **Retry in the sensor (`async_update`) instead of in the transport**: rejected
  — the connection, its error classification and its close/reconnect bookkeeping
  already live in the transport. The sensor would then answer "is this failure
  the connection" a second time and hold a copy of that rule.
- **`DataUpdateCoordinator` with its own error handling**: already rejected in
  ADR 0011 for a single integer sensor.

## Consequences

- A connection that the server closed while it was idle no longer costs a polling
  interval: the value stays and the entity stays available.
- A real outage still leaves the entity `unavailable` while it lasts, but no
  longer adds a second interval after the server is back: the tick that follows
  the restart succeeds on a fresh connection.
- Worst case per tick against a dead sink: two attempts, i.e. twice the configured
  HTTP timeout on the executor thread, once per 5 minutes.
- The failure stays a single log line per availability transition (Silver rule
  *log-when-unavailable*), so the repetition adds no log noise.
- The README and `docs/troubleshooting.md` name the two causes of a flapping
  `table_size`, so a transition is not read as data loss.

## Verification

- `tests/unit/test_transport.py`:
  - a `RemoteDisconnected` on the first attempt is repeated on a second
    connection.
  - a retryable HTTP status error is repeated.
  - the default repeats nothing.
  - a rejected statement is not repeated.
  - the repetitions are bounded.
  - a negative `retries` fails before the transport opens a connection.
- `tests/unit/test_sensor_platform.py`: the sensor asks for the repetition and
  stays available when the query returns a value.
- Socket-level probe with the real `transport.py` (repository-local, gitignored:
  `dev/scratch/query_retry_probe.py`): a loopback server answers one request and
  then shuts the connection down while the client is idle. Without the repetition
  the next query fails with `RemoteDisconnected` on the reaped connection. With
  `retries=1` it succeeds, and the server counts TWO accepted connections. That
  second connection is the reconnect itself. With the server down the error still
  reaches the caller after two attempts, so a repetition does not mask an outage.
  Once the server answers again, the next query succeeds.

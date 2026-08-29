# HASS QuestDB Writer architecture

Status: proposed architecture for the clean implementation.

This document defines the target design. The runtime code lives in
`custom_components/hass_questdb_writer` and is independent of QSS.

## Goals

- Export selected Home Assistant state changes to QuestDB without blocking the
  Home Assistant event loop.
- Keep one long-lived QuestDB connection and write events in batches.
- Survive temporary QuestDB, DNS, network, and NAS outages.
- Shut down and reload without duplicate listeners or abandoned worker threads.
- Make queue depth, retries, delivery progress, and failures observable.
- Keep the local development stack isolated from production.

## Non-goals

- Replacing Home Assistant Recorder.
- Providing exactly-once delivery. ILP does not provide an end-to-end
  transaction identifier that makes an uncertain retry inherently idempotent.
- Running SQL queries from the Home Assistant event loop.
- Preserving an unlimited backlog. Disk and retention limits must be explicit.

## System context

```text
Home Assistant state_changed event
                 |
                 v
        Event listener / filter
        (HA main event loop)
                 |
                 v
        bounded ingress queue
                 |
                 v
      writer service (one thread)
          |               |
          v               v
   durable SQLite     batch builder
       spool              |
          ^               v
          +-------- IlpHttpTransport
                    persistent HTTP
                           |
                           v
                       QuestDB
```

The event listener never performs network or disk I/O. A single worker owns the
spool, the HTTP connection, retry state, and batching state. This keeps all
blocking operations outside Home Assistant's event loop and avoids concurrent
access to the transport.

## Components

### Integration lifecycle

The integration uses Home Assistant config entries:

- `async_setup_entry` validates configuration and starts one runtime instance.
- The runtime instance registers exactly one `state_changed` listener.
- `async_unload_entry` unsubscribes the listener before stopping the worker.
- Runtime state is stored per config-entry ID, not in module globals.
- Reload creates a new runtime only after the previous runtime has stopped.

YAML may be supported later as an import path, but it is not the ownership model
for the runtime.

### Event listener and filter

The listener runs in the Home Assistant event loop and performs only bounded,
non-blocking work. When explicit entity IDs are configured, it uses Home
Assistant's indexed state-change helper. When all entities are selected, it
uses `hass.bus.async_listen(EVENT_STATE_CHANGED, ...)`: in Home Assistant
2026.7.2 the indexed helper does not interpret `MATCH_ALL` as a wildcard.

1. Reject events without a new state.
2. Apply include/exclude rules by entity ID, domain, and entity glob.
3. Convert the HA event into an immutable internal `EventEnvelope`.
4. Offer the envelope to a bounded in-memory ingress queue without waiting.

The envelope contains a generated event ID, entity ID, state, selected state
metadata, attributes, HA event timestamp, and ingestion timestamp. It contains
plain serializable values and no live Home Assistant objects.

Before entering the bounded queue, the envelope is serialized as versioned,
deterministic UTF-8 JSON. The worker stores those exact bytes in SQLite and
only converts them to ILP when constructing a delivery batch. Consequently, a
serialization or ILP-encoding failure can be retained in dead-letter state
instead of disappearing before durable persistence.

If the ingress queue cannot accept an event, the runtime records an explicit
overflow metric and follows the configured overflow policy. Silent loss is not
allowed. Exact queue limits and the default overflow policy remain open until
load tests establish safe values.

### Writer service

One dedicated thread owns all blocking state:

- drains the ingress queue;
- writes new envelopes to the durable spool;
- reads pending spool rows in order;
- builds bounded batches;
- owns one long-lived `IlpHttpTransport`;
- marks rows delivered only after a successful flush;
- reconnects and retries after recoverable failures;
- exposes immutable health snapshots to Home Assistant.

Unexpected exceptions are caught at the outer worker boundary, recorded, and
cause a controlled degraded state. A retry-library wrapper must not be able to
terminate the worker silently.

The implementation uses an explicit internal state machine rather than a
retry-library decorator:

```text
starting -> running -> retry_wait -> running
                 |          |
                 +-> blocked+
                 |
                 +-> stopping -> stopped
                 |
                 +-> failed
```

- Retryable transport failures retain the batch, record attempt metadata, and
  schedule capped exponential backoff with bounded jitter.
- During retry wait, newly accepted ingress continues moving to SQLite.
- HTTP 400 on a multi-row batch narrows delivery to one row. Only a row that
  also fails individually is moved to dead-letter.
- Authentication and non-row permanent errors enter `blocked` while ingress
  continues to spool within its limits.
- Any unexpected exception reaches the outer boundary, leaves pending rows in
  SQLite, and produces a visible `failed` snapshot.

All capacities, batch thresholds, retry bounds, jitter, latency, and shutdown
flush behavior are required constructor inputs. Runtime defaults will not be
selected before production-rate and filesystem tests. The detailed decision is
recorded in
[decisions/0003-explicit-worker-state-machine.md](decisions/0003-explicit-worker-state-machine.md).

### Durable spool

The spool is a private SQLite database on the local filesystem under the
integration's storage area. It uses WAL journal mode, `synchronous=FULL`, and
explicit write transactions. SQLite WAL files must not be placed on a network
filesystem. Only the writer thread owns the connection; the default SQLite
same-thread check remains enabled.

Pending rows have a local sequence number, stable event ID, opaque serialized
payload, creation time, attempt count, last error, and sticky
delivery-uncertain flag. The sequence number preserves local FIFO order; it
does not claim global ordering across multiple HA instances. Confirmed rows
are deleted atomically. Permanently rejected rows move atomically into a
separate dead-letter table.

Pending and dead-letter stores both have explicit row and payload-byte limits.
The limits do not pretend to include SQLite page, index, WAL, or metadata
overhead, so the integration also needs a filesystem free-space guard before
production release. Reaching any limit must be visible through logs and
diagnostics and invoke an explicit policy. The project will not advertise
unlimited outage retention.

The worker persists multiple ingress events in one transaction. The local
ARM64 benchmark measured a median of about 19,900 events/s for one transaction
per event and 52,600 events/s for 100-event transactions with 256-byte
payloads. These numbers validate the mechanism, not production defaults; the
production storage filesystem must be measured separately. Details are in
[benchmarks/sqlite-spool.md](benchmarks/sqlite-spool.md).

### QuestDB transport

The target transport is ILP over HTTP using a focused pure-Python transport
owned by this project. The integration has no runtime dependency on the native
QuestDB Python package. This is an accepted architecture decision documented in
[decisions/0001-pure-python-ilp-http.md](decisions/0001-pure-python-ilp-http.md).

The worker keeps one HTTP connection alive across single-table batches and
recreates it only after a transport failure or configuration change. ILP/TCP
remains a standalone benchmark comparison and is not part of the initial
production runtime.

The transport is responsible only for ILP encoding, HTTP connection reuse,
timeouts, authentication, response parsing, and sanitized errors. Spooling,
retry policy, batching policy, and delivery state remain owned by the writer
service.

The measured local transport comparison and its limitations are recorded in
[benchmarks/ilp-http-vs-tcp.md](benchmarks/ilp-http-vs-tcp.md).

Batch flush is triggered by configurable size and latency thresholds. Concrete
defaults will be set from local load and outage tests.

`timestamp` is sent as the ILP designated timestamp in nanoseconds. Additional
timestamp fields use QuestDB's ILP timestamp-field representation: epoch
microseconds with a `t` suffix. Converting Home Assistant nanoseconds to these
fields intentionally truncates sub-microsecond precision; Home Assistant state
timestamps currently originate at microsecond resolution.

## Delivery semantics

The target guarantee is **at least once from the durable spool**:

- A row is removed or marked delivered only after the HTTP request returns a
  confirmed successful response.
- If HA stops before an event reaches the spool, the bounded ingress queue can
  still contain unpersisted events; shutdown attempts to persist them.
- If the connection fails before a successful flush, the rows remain pending.
- If QuestDB commits a batch but the client loses the acknowledgement, retrying
  can create duplicates.

Every event therefore carries a stable `event_id`. This makes duplicates
detectable in queries and leaves room for a future deduplication process, but it
does not by itself make QuestDB ILP writes exactly once.

## Failure handling

### QuestDB unavailable

- Persist incoming events locally while capacity remains.
- Close the failed HTTP connection and enter a disconnected state.
- Retry with capped exponential backoff and jitter.
- Reconnect automatically and drain the oldest pending rows first.
- Do not hold a queue item in an unfinished state while sleeping between
  retries.

### DNS, refused connection, timeout, or broken socket

These are transport failures and follow the same reconnect path. Error
classification will be tested against the exact QuestDB client version used by
the integration.

### Invalid event or schema rejection

Deterministically invalid rows must not block the entire spool forever. After
classification, they move to a dead-letter state with the full error available
in diagnostics. Automatic dropping is not silent.

### Spool full or unwritable

The runtime enters a degraded state, emits a rate-limited error, increments a
loss/overflow counter, and applies the configured overflow policy. This case is
covered by explicit disk-full and permission tests.

### Worker crash

The outer worker boundary records the traceback and changes health state. Home
Assistant diagnostics must show that the worker is dead. Automatic supervision
may restart the worker, but restart loops must be rate-limited and tested.

## Shutdown and reload sequence

```text
unsubscribe HA listener
        -> stop accepting new envelopes
        -> persist the remaining ingress queue
        -> flush a ready batch when possible
        -> close HTTP transport and spool
        -> join worker with a bounded timeout
        -> expose a clear timeout error if it did not stop
```

Home Assistant shutdown must not wait indefinitely for an unavailable QuestDB.
Persisting pending events has priority over completing remote delivery.
Remote flush during shutdown is an explicit setting. A stop deadline returns a
visible timeout while a network request is still blocked; the configured HTTP
timeout remains the final bound on that request.

## QuestDB data model

Initial logical columns:

| Column | Purpose | Candidate type |
|---|---|---|
| `timestamp` | Home Assistant state timestamp | `TIMESTAMP` designated timestamp |
| `ingested_at` | Time accepted by this integration | `TIMESTAMP` |
| `event_id` | Stable ID for duplicate detection | `VARCHAR` |
| `entity_id` | Full HA entity ID | `SYMBOL` candidate |
| `domain` | HA entity domain | `SYMBOL` candidate |
| `state` | State value without forced numeric conversion | `VARCHAR` |
| `attributes` | Serialized selected attributes | `VARCHAR` |
| `last_changed` | HA state metadata | `TIMESTAMP` |
| `last_updated` | HA state metadata | `TIMESTAMP` |
| `context_id` | HA context correlation when present | `VARCHAR` |

Symbol choices, partitioning, WAL mode, attribute policy, and table name are
provisional. They will be validated against production cardinality and query
patterns before a migration schema is declared stable. The development table
name is `hass_questdb_writer_events`.

## Configuration model

The config entry will own:

- QuestDB URL and authentication material;
- target table;
- include/exclude filters;
- batching thresholds;
- retry bounds;
- spool limits and overflow policy;
- attribute inclusion policy.

Secrets are stored through Home Assistant's config-entry mechanisms and are
redacted from logs and diagnostics. Changes that alter connection ownership or
schema trigger a controlled reload.

The current `0.1.0-dev0` runtime has a deliberately named `PROVISIONAL_*`
development profile. Its queue, spool, retry, timeout, and batch values exist
to run the local end-to-end integration; they are not declared supported
production defaults. The profile and remaining measurements are recorded in
[decisions/0004-home-assistant-lifecycle.md](decisions/0004-home-assistant-lifecycle.md).

## Observability

Diagnostics expose at least:

- lifecycle state and worker liveness;
- connection state and last successful flush time;
- ingress queue depth and high-water mark;
- pending spool rows and bytes;
- delivered, retried, rejected, overflowed, and dead-letter counts;
- current retry delay and sanitized last error;
- client, integration, HA, and server compatibility versions where available.

Logs are structured around state transitions and are rate-limited during long
outages. Per-event success logging is disabled outside targeted debugging.

## Testing strategy

### Unit tests

- event conversion and filtering;
- serialization edge cases;
- batch boundaries;
- retry classification and backoff state;
- spool ordering, recovery, limits, and dead-letter transitions;
- lifecycle idempotency and shutdown state machine.

### Container integration tests

- clean HA startup and config-entry reload;
- one state change creates one queryable row;
- batching reuses a persistent HTTP connection;
- QuestDB unavailable before HA starts;
- QuestDB stopped during a flush;
- short and long outage followed by ordered drain;
- DNS failure, refused port, and broken connection;
- HA restart with pending spool rows;
- repeated integration reload without duplicate listeners or threads;
- queue pressure, spool full, malformed attributes, and shutdown timeout.

### Production acceptance

Production deployment happens only after the isolated tests pass. It starts
with a separate QuestDB table and a narrow entity allow-list. The existing QSS
integration remains the rollback path until row counts, timestamps, outage
recovery, and resource use have been compared.

## Open decisions

The following require evidence before implementation defaults are frozen:

- queue, batch, retry, and spool limits;
- HTTP versus TCP performance for the measured production event rate;
- table partitioning and WAL settings;
- full attributes versus an allow-list;
- overflow policy under a prolonged full-disk outage;
- automatic worker restart policy;
- whether dead-letter events need a separate QuestDB table or only local
  diagnostics.

## Authoritative references

- Home Assistant config-entry lifecycle:
  <https://developers.home-assistant.io/docs/config_entries_index/>
- Home Assistant event listening:
  <https://developers.home-assistant.io/docs/integration_listen_events/>
- Home Assistant thread-safety guidance:
  <https://developers.home-assistant.io/docs/asyncio_thread_safety/>
- QuestDB ILP overview:
  <https://questdb.com/docs/connect/compatibility/ilp/overview/>
- QuestDB ILP syntax:
  <https://questdb.com/docs/ingestion/ilp/advanced-settings/>

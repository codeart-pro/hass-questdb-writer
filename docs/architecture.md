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

The listener uses Home Assistant's optimized state-change event helper, runs in
the Home Assistant event loop, and performs only bounded, non-blocking work:

1. Reject events without a new state.
2. Apply include/exclude rules by entity ID, domain, and entity glob.
3. Convert the HA event into an immutable internal `EventEnvelope`.
4. Offer the envelope to a bounded in-memory ingress queue without waiting.

The envelope contains a generated event ID, entity ID, state, selected state
metadata, attributes, HA event timestamp, and ingestion timestamp. It contains
plain serializable values and no live Home Assistant objects.

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

### Durable spool

The spool is a private SQLite database under the integration's storage area.
Only the writer thread accesses it. WAL mode and schema details will be selected
after filesystem and shutdown tests in the HA container.

Each row has a local sequence number, event ID, serialized envelope, creation
time, attempt metadata, and delivery state. The sequence number preserves local
ordering; it does not claim global ordering across multiple HA instances.

The spool has configurable byte/row/age limits. Reaching a limit must be
visible through logs and diagnostics and must invoke an explicit policy. The
project will not advertise unlimited outage retention.

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

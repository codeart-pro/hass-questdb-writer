# HASS QuestDB Writer

[![Validate](https://github.com/codeart-pro/hass-questdb-writer/actions/workflows/validate.yml/badge.svg?branch=main)](https://github.com/codeart-pro/hass-questdb-writer/actions/workflows/validate.yml) [![Tests](https://github.com/codeart-pro/hass-questdb-writer/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/codeart-pro/hass-questdb-writer/actions/workflows/tests.yml) [![Release](https://img.shields.io/github/v/release/codeart-pro/hass-questdb-writer?sort=semver)](https://github.com/codeart-pro/hass-questdb-writer/releases)

Stream every Home Assistant state change into
[QuestDB](https://questdb.com/) — durable, at-least-once, with server-side
deduplication.

A `service`-type integration: it subscribes to the Home Assistant event bus
and writes `state_changed` events to a QuestDB table over pure-Python
ILP/HTTP. No native QuestDB client dependency, no C extensions.

## Requirements

- **Home Assistant 2025.1.0 or newer** — the minimum `hacs.json` declares. CI
  runs the test suite against that version *and* against the current stable
  release, so both ends of the range are exercised.
- **A reachable QuestDB instance** (10.x is what the tests and the bench use).
  The integration talks to its ILP/HTTP and REST endpoints on port `9000`;
  PGWire `8812` is only needed if you also want the Home Assistant SQL
  integration to read data back.
  There is **no Home Assistant add-on for QuestDB** — the official and community
  add-on repositories were checked. Run it as a container, package or binary.
- **QuestDB Open Source** if you want the retention (TTL) option: QuestDB
  Enterprise rejects a non-zero TTL and the integration then logs once and keeps
  writing without it.
- Nothing to install on the Home Assistant side: no extra package, no native
  client, no YAML.

## Features

- **Durable spool first**: events land in a local SQLite spool (WAL,
  `synchronous=FULL`) before anything touches the network, so an outage does not
  lose accepted data while the spool has room; retries back off exponentially,
  and the capacity bounds are in
  [ADR 0014](docs/decisions/0014-spool-pressure-policy.md).
- **Idempotent replays**: delivery is at-least-once, and
  `DEDUP UPSERT KEYS(last_updated, entity_id)` collapses a repeated row into the
  same record, so retries, restarts and an uncertain acknowledgement do not
  duplicate data. Two *different* state changes carrying the same
  `(last_updated, entity_id)` do collapse into one row — see
  [Known limitations](#known-limitations)
  ([ADR 0005](docs/decisions/0005-questdb-record-format.md)).
- **Owned schema**: the integration creates and strictly validates its table
  (`TIMESTAMP(last_updated) PARTITION BY DAY WAL DEDUP UPSERT KEYS(...)`);
  delivery is gated until the schema check passes.
- **UI-driven configuration** (config entry + options flow): connection,
  HTTP Basic auth, entity include/exclude filter, and all tuning limits are
  set in the UI — no YAML.
- **Grafana-ready**: `entity_id`/`domain` as SYMBOL columns, numeric states
  castable (`CAST(state AS DOUBLE)`), `SAMPLE BY`/`LATEST ON` work out of
  the box.

## Installation

### HACS (recommended)

1. Add this repository as a custom repository (HACS → ⋯ → Custom
   repositories, category **Integration**).
2. Install **HASS QuestDB Writer** and restart Home Assistant.

### Manual

Copy `custom_components/hass_questdb_writer/` into your Home Assistant
`config/custom_components/` directory and restart.

## Setup

1. Settings → Devices & Services → Add Integration → **HASS QuestDB Writer**.
2. Enter the QuestDB connection:
   - **Host / port** — QuestDB REST/ILP endpoint (default port `9000`)
   - **Table** — table name to own (default `hass`)
   - **Username / password** — optional HTTP Basic auth, both or neither

> QuestDB runs **separately** — there is no Home Assistant add-on for it
> (checked: official and community add-on repositories). Install it any
> way you like: [Docker container](https://questdb.com/docs/getting-started/quick-start/),
> package, or binary — the integration just needs a reachable
> REST/ILP endpoint.
3. Finish the flow. The worker creates the table on first delivery; nothing
   is written until the schema is created and validated.

The connection parameters can be changed later via the **Reconfigure**
action; filtering and tuning options live in **Configure**.

## Removal

1. **Delete the config entry**: Settings → Devices & Services → **HASS QuestDB
   Writer** → ⋯ → **Delete**. Home Assistant unloads the entry first: the
   state-change listener is removed, the writer persists what it already accepted
   to the spool and stops, and the table-size timer is cancelled. The sensors and
   the device disappear from the UI.
2. **Remove the integration code**: in HACS → **HASS QuestDB Writer** → ⋯ →
   **Remove**, or delete `custom_components/hass_questdb_writer/` by hand, then
   restart Home Assistant.
3. **Clean up what stays behind** — deleting the entry removes neither of these:
   - the **QuestDB table**: the integration creates and owns it but never drops
     it, so the history stays until you delete it yourself:

     ```sql
     DROP TABLE hass;   -- the table name from your configuration
     ```

   - the **local spool file**: `/config/.storage/hass_questdb_writer/<entry_id>.db`
     is the SQLite spool, including events that were never delivered. Remove the
     file by hand once you are sure you do not need its contents.

## Options

Everything is configured in the UI — no YAML. There are three surfaces:
**Reconfigure** (connection), **Configure → Entity filter** (what gets
written) and **Configure → Show advanced settings → Advanced** (tuning and
retention).

### Connection (Setup / Reconfigure)

| Option | Default | Notes |
|---|---|---|
| **Host** | — | QuestDB host as seen from the HA container (e.g. `questdb`) |
| **Port** | `9000` | QuestDB REST/ILP port, 1–65535 |
| **Table** | `hass` | UTF-8, ≤ 127 bytes; the integration owns the table |
| **Use HTTPS** | off | TLS for the REST/ILP connection |
| **Accept self-signed TLS certificates** | off | skip certificate verification (for self-signed certs on local proxies); only meaningful with Use HTTPS |
| **Username / password** | empty | HTTP Basic auth; both or neither. Reconfigure keeps the stored password when left empty |

The **Submit** button verifies the connection first: an unreachable host
or rejected credentials keep the form open with an error.

### Entity filter (Configure)

Empty lists mean *no restriction*. An include list acts as a **strict
allowlist**; excludes cut into whatever remains; **exclude wins**.
Items present in both sides of a pair are rejected at submit with an
error naming them. `*` and `?` wildcards are supported in glob and
attribute patterns.

| Option | Control | Notes |
|---|---|---|
| **Entities to include** | entity picker | only these entities are written (strict allowlist) |
| **Entities to exclude** | entity picker | these entities are never written |
| **Domains to include / exclude** | multi-select dropdown | domains with usable entities, from the entity registry |
| **Glob patterns to include / exclude** | text (comma-separated) | e.g. `sensor.garden_*, light.*` |
| **Attribute patterns to include** | text (comma-separated) | only matching attributes are written, e.g. `friendly_name, unit_*, rssi` |
| **Attribute patterns to exclude** | text (comma-separated) | matching attributes are removed (deny wins), e.g. `rssi, update.*` |
| **Show advanced settings** | checkbox | reveals the next step (see below) |

### Advanced (Configure → Show advanced settings)

Values are **provisional development defaults** until production-rate
benchmarks settle them; they can be left untouched.

| Option | Default | Range | Notes |
|---|---|---|---|
| **Data retention (days)** | `0` (no limit) | 0–3650 | QuestDB [TTL](https://questdb.com/docs/concepts/ttl/): day partitions older than the window are dropped automatically (`ALTER TABLE … SET TTL n DAYS`); `0` disables. Requires QuestDB **Open Source** — Enterprise rejects non-zero TTL (it uses storage policies instead); there the integration logs once and keeps writing without TTL |
| **Ingress queue capacity** | `1000` | 10–100000 | in-memory event queue between the HA listener and the SQLite spool |
| **Max serialized event bytes** | `65536` (64 KiB) | 1024–1048576 | largest event written; larger events are skipped (counted, never crash) |
| **Persist batch rows** | `100` | 1–10000 | rows per SQLite insert |
| **Delivery batch rows** | `1000` | 1–100000 | events per ILP batch |
| **Delivery batch bytes** | `524288` (512 KiB) | 4096–16777216 | bytes per ILP batch (whichever limit hits first) |
| **Flush interval (s)** | `1.0` | 0.05–300 | spool → delivery cadence at low event rates |
| **Persist idle poll fallback (s)** | `1.0` | 0.05–60 | lost-wakeup fallback of the persist loop, not a latency bound: a new event is persisted as soon as `submit()` signals the worker, so this only bounds the recovery time if that signal was dropped ([ADR 0013](docs/decisions/0013-idle-persist-polling.md)). Raising it lowers the idle wake-up rate |
| **Retry initial (s)** | `1.0` | 0.1–300 | first backoff delay after a failed delivery |
| **Retry max (s)** | `60` | 1–3600 | backoff ceiling |
| **Retry multiplier** | `2.0` | 1–10 | exponential backoff factor |
| **Retry jitter ratio** | `0.2` | 0–1 | random jitter added to each delay (0–20%) |
| **Flush on shutdown** | off | on/off | try to deliver remaining events when HA stops; when off they stay in the spool and are delivered on next start (at-least-once) |
| **Max pending rows** | `100000` | 100–10000000 | SQLite spool capacity (rows) |
| **Max pending bytes** | `67108864` (64 MiB) | 1 MiB–1 GiB | SQLite spool capacity (bytes) |
| **Max dead-letter rows** | `1000` | 10–1000000 | ring buffer of undeliverable events (FIFO eviction) |
| **Max dead-letter bytes** | `16777216` (16 MiB) | 64 KiB–256 MiB | dead-letter capacity (bytes) |
| **Min free disk space (bytes)** | `536870912` (512 MiB) | 0–1 GiB | free space the writer keeps untouched on the filesystem that holds the spool, so SQLite pages, the WAL, the recorder and backups never lose the last of the disk to the writer; `0` disables the floor ([ADR 0014](docs/decisions/0014-spool-pressure-policy.md)) |
| **Min free disk space (ratio)** | `0.05` | 0–0.5 | the same reserve as a share of the filesystem; the larger of the two wins, so small disks are protected by the floor and large ones by the share |
| **SQLite busy timeout (s)** | `1.0` | 0.05–30 | retry window for spool lock contention |
| **HTTP timeout (s)** | `10` | 1–120 | per-request timeout for REST/schema and ILP POST |
| **Start timeout (s)** | `10` | 1–120 | how long setup waits for the worker thread |
| **Stop timeout (s)** | `15` | 1–300 | how long unload waits for the worker to drain |

## Health sensors

The integration provides five polled sensors (under the device
**HASS QuestDB Writer**) that read the in-memory writer snapshot — they
never touch QuestDB, so they keep reporting (and raising alarms) while
the server is unreachable. They are polled **every 30 seconds**;
`homeassistant.update_entity` forces an immediate refresh on demand.

Four of the five values are **run-scoped**: `state`,
`seconds_since_last_delivery`, `events_delivered` and `last_delivery_error`
come from the worker process, so they start over with it. After a Home
Assistant restart (including the one a HACS update requires) or an
integration reload, `events_delivered` is 0 again and
`seconds_since_last_delivery` reads `unknown` until the first successful
delivery of that run. That is a restart marker, not data loss: undelivered
events wait in the SQLite spool. The two values that do survive a restart are
`pending_rows` (read from that spool) and `table_size` (queried from QuestDB).
For a total that never resets, use the long-term statistics of
`events_delivered` — its `sum` keeps accumulating across restarts.

Entity labels read as fields of the device, so Home Assistant shows them as
**HASS QuestDB Writer State**, **HASS QuestDB Writer Table size**, and so on. The
entity IDs below are what a **fresh install** gets; entities registered by an
earlier version keep the ID they already have (only their friendly name changes).

| Entity | Meaning |
|---|---|
| `state` | worker state: `new`/`starting`/`running`/`retry_wait`/`blocked`/`stopping`/`stopped`/`failed` |
| `seconds_since_last_delivery` | age of the last successful delivery (s) — **grows during an outage**; `unknown` until the first delivery of the current run |
| `pending_rows` | undelivered rows buffered in SQLite — survives a restart |
| `events_delivered` | events delivered **since this worker started** (total_increasing; in-memory, so it is 0 again after every restart or reload — for a lifetime total use its statistics) |
| `last_delivery_error` | text of the last delivery error, `none` when clean |
| `table_size` | on-disk size of the entry's table (MB, decimal) — **queried from QuestDB every 5 minutes** by its own timer (not the 30 s health poll), goes `unavailable` during an outage; use it to plan retention, not for watchdog triggers |

Because the SQL integration's sensors freeze on their last value while
QuestDB is down, a write watchdog must trigger on `seconds_since_last_delivery`
— not on SQL-derived values. That trigger alone has one blind spot: the sensor
is `unknown` until the first successful delivery of the current run, and a
`numeric_state` trigger never fires on `unknown`. An outage that began before a
Home Assistant restart and continues after it is therefore invisible to it.
The spool-backed `pending_rows` covers that window — its value is reconciled
from the rows when the spool is opened, so it is correct even before the first
delivery — so watch both:

```yaml
alias: QuestDB write watchdog
triggers:
  # Deliveries stopped during the current run.
  - trigger: numeric_state
    entity_id: sensor.hass_questdb_writer_seconds_since_last_delivery
    above: 300
  # Deliveries never started after a restart. Tune the row threshold to your
  # event rate: at the 100-500 events/min assumed in "Outage behavior" below,
  # 1000 rows is roughly 2-10 minutes of backlog.
  - trigger: numeric_state
    entity_id: sensor.hass_questdb_writer_pending_rows
    above: 1000
    for: "00:05:00"
conditions:
  - condition: or
    conditions:
      - condition: numeric_state
        entity_id: sensor.hass_questdb_writer_seconds_since_last_delivery
        above: 300
      - condition: numeric_state
        entity_id: sensor.hass_questdb_writer_pending_rows
        above: 1000
actions:
  - action: notify.mobile_app_phone
    data:
      title: "⚠️ QuestDB writes have stopped"
      message: "Events have not been delivered for over 5 minutes. Check the integration, QuestDB and the network."
mode: single
```

## Outage behavior

While QuestDB is unreachable the writer keeps buffering events in the
SQLite spool (at-least-once, bounded):

- **Capacity**: 100,000 rows / 64 MiB spool plus 1,000 / 16 MiB
  dead-letter, tunable in **Configure → Show advanced settings**
  (see [Options](#options) above); at a typical 100–500 events/min
  that covers roughly 3–17 h of downtime
- **Full spool or full disk**: persistence pauses instead of the writer dying.
  The state becomes `blocked` and diagnostics name the reason (`spool_full`,
  `disk_space`, `disk_full` or `readonly`). Accepted events stay queued, the
  writer keeps retrying and resumes by itself once QuestDB is back or storage
  recovers — including when the spool is what filled the disk: it returns the
  pages of already delivered rows and truncates its WAL while the reserve is
  consumed ([ADR 0015](docs/decisions/0015-spool-space-reclamation.md)). Only
  events that no longer fit the in-memory queue are dropped, counted in
  `overflowed_events`
- **Free-space reserve**: the writer keeps the `Min free disk space` options
  untouched, so the recorder, logs and backups are not the ones that lose the
  last of the disk to the spool. The reserve is a pause threshold checked before
  every durable write, not an untouched buffer: one write may cross it by up to
  one persist batch before the next check stops the writer
  ([ADR 0014](docs/decisions/0014-spool-pressure-policy.md))
- **Logs**: rate-limited retry warnings (1st, 2nd, 4th… attempt), no spam

## Reading the data

The writer only writes; reading is done with any QuestDB client
([InfluxDB Line Protocol](https://questdb.com/docs/connect/compatibility/ilp/overview/)
for ingestion, SQL/REST/PGWire for queries):

- **Web Console** (`http://<host>:9000`) for ad-hoc queries,
- **Grafana** with the
  [official QuestDB data source](https://grafana.com/grafana/plugins/questdb-questdb-datasource/) —
  the panel queries are worked through in [docs/grafana.md](docs/grafana.md), and
  sample dashboards ship in the dev-stack repository,
- **Home Assistant's built-in SQL integration** over the PostgreSQL wire
  protocol (`postgresql://admin:quest@questdb:8812/qdb`) to bring values
  into HA states and automations — see the section below,
- any PostgreSQL driver (`psycopg2`, `psql`, …).

### Example queries (verified on QuestDB 10)

The table stores one row per state change: `last_updated` (designated
timestamp), `entity_id`, `domain`, `state` (text), `attributes` (JSON),
`event_id`, `context_id`, `ingested_at`, `last_changed`.

**Latest value of an entity** ([`LATEST ON`](https://questdb.com/docs/reference/sql/latest-on/),
the WHERE clause goes first):

```sql
SELECT entity_id, state
FROM hass
WHERE entity_id = 'sensor.example_temperature'
LATEST ON last_updated PARTITION BY entity_id;
```

The newest stored row — a sensor that stopped reporting shows its last value, not
a fresh one.

**Events per hour** ([`SAMPLE BY`](https://questdb.com/docs/reference/sql/sample-by/),
[`dateadd`](https://questdb.com/docs/query/functions/date-time/)):

```sql
SELECT entity_id, count() AS events
FROM hass
WHERE last_updated > dateadd('h', -6, now())
SAMPLE BY 1h
ORDER BY events DESC LIMIT 20;
```

One row per entity per hour — thousands of rows on a large instance, so the
busiest few are kept in view.

**Hourly average of a numeric entity** — states are stored as text, so
numeric aggregates need a [cast](https://questdb.com/docs/reference/sql/cast/):

```sql
SELECT entity_id, avg(CAST(state AS DOUBLE)) AS avg_state
FROM hass
WHERE entity_id = 'sensor.example_temperature'
  AND last_updated > dateadd('h', -2, now())
SAMPLE BY 1h;
```

**Event volume (for retention planning)** ([meta functions](https://questdb.com/docs/query/functions/meta/)):

```sql
SELECT count(), size_pretty(sum(diskSize)) AS table_size
FROM table_partitions('hass');
```

These queries are the basis of the Grafana panels; turning them into a panel is
covered in [docs/grafana.md](docs/grafana.md), with one example reading a value
from `state` and one reading it out of the `attributes` JSON.

### Reading inside Home Assistant (SQL integration)

Add the [SQL integration](https://www.home-assistant.io/integrations/sql/) (UI or YAML)
pointing at QuestDB's [PostgreSQL wire protocol](https://questdb.com/docs/connect/compatibility/pgwire/overview/)
port, alias aggregate columns, and use the sensor like any other:

```yaml
sql:
  - name: QuestDB total events
    db_url: postgresql://admin:***@questdb:8812/qdb
    query: SELECT count() AS total FROM hass
    column: total
    unit_of_measurement: events
```

Notes: PGWire defaults are `admin`/`quest`; aggregate columns arrive as
`count()` and must be aliased; no SSL, no `DELETE`, no `HAVING`.
Sensors **freeze on their last value while QuestDB is down** — build
watchdog automations on the integration's health sensors instead (see
above).

**Alternatives**: [QSS](https://github.com/CM000n/qss) is another
option for writing HA states to QuestDB.

## Known limitations

- **Delivery is at-least-once, not exactly-once.** Server-side dedup on
  `(last_updated, entity_id)` collapses a re-delivered row into the same record,
  but two *different* state changes carrying the same `(last_updated, entity_id)`
  also collapse into one row: the key's uniqueness is an assumption about Home
  Assistant semantics, not something the integration can check
  ([ADR 0005](docs/decisions/0005-questdb-record-format.md)).
- **The health counters are run-scoped.** `events_delivered` counts the current
  worker run and reads 0 again after a Home Assistant restart or an integration
  reload; `seconds_since_last_delivery` is `unknown` until the first delivery of
  that run. `pending_rows` (read from the spool) and `table_size` (queried from
  QuestDB) do survive. A watchdog watching only `seconds_since_last_delivery`
  cannot see an outage that began before a restart — watch `pending_rows` too
  ([Health sensors](#health-sensors)).
- **Reading through the Home Assistant SQL integration freezes.** Those sensors
  hold their last value while QuestDB is unreachable, which makes them useless as
  a write watchdog. The integration's own health sensors keep reporting, because
  they never touch QuestDB.
- **`table_size` goes `unavailable` during an outage**: it is queried from
  QuestDB every 5 minutes. Use it to plan retention, not to raise alarms.
- **The integration owns its table.** A table created outside the integration
  (Web Console, Grafana, another writer) fails the schema check and no delivery
  happens until it is fixed — drop it or point the entry at a fresh name. The
  integration never drops the table, not even when the config entry is deleted.
- **Retention needs QuestDB Open Source.** QuestDB Enterprise rejects a non-zero
  TTL (it uses storage policies instead): there the integration logs once and
  keeps writing without TTL. Where it is active, TTL removes whole day
  partitions, asynchronously.
- **Events can be skipped, and the counters that tell you are only in the
  diagnostics download.** `unknown` states are never written; events larger than
  **Max serialized event bytes** are skipped (`oversized_events`); events that no
  longer fit the in-memory ingress queue are dropped (`overflowed_events`).
- **No QuestDB add-on for Home Assistant** — the official and community add-on
  repositories were checked; QuestDB runs separately and is not managed by the
  integration.
- **Verified against QuestDB 10.0.1** (the bench and the CI service image).
  Other QuestDB releases have not been exercised.
- **The development stack, dashboards and benchmarks are kept in a private
  repository.** Of the measurements, only what is published under
  [`docs/benchmarks/`](docs/benchmarks/) and the ADRs is reproducible from here.

## Troubleshooting

The full symptom → cause → fix guide is
**[docs/troubleshooting.md](docs/troubleshooting.md)**. The three checks that
resolve most cases:

1. **Integration state** — Devices & Services → **HASS QuestDB Writer** → ⋯ →
   **System options**. A **Repair issue** ("QuestDB rejected the credentials")
   means the HTTP Basic credentials are wrong: fix them in **Reconfigure**.
2. **Health sensors** — `state` says what the worker is doing and
   `last_delivery_error` names the last failure; the watchdog automation is in
   [Health sensors](#health-sensors) above.
3. **Logs** — Settings → System → Logs, filtered by `hass_questdb_writer`. A
   blocked worker logs the last delivery error, rate-limited, so only a few
   lines per outage.

| Symptom | Where it is worked through |
|---|---|
| Nothing reaches QuestDB | credentials, host/port as seen from the HA container, schema check errors |
| The table is missing | it is created on the first delivery attempt; check the container-network port |
| Rows look duplicated | dedup keeps `(last_updated, entity_id)` unique |
| Old data disappears | the **Data retention** (TTL) option drops whole day partitions |
| Alarming values right after a restart | `events_delivered` = 0 and `seconds_since_last_delivery` = `unknown` are expected |

### Getting help

Include in any bug report: HA version, QuestDB version, the integration
diagnostics (downloadable from Devices & Services), the log excerpt filtered by
`hass_questdb_writer`, and the table DDL (`SHOW CREATE TABLE …`).

## Data model

Table `hass` (owned by the integration):
`TIMESTAMP(last_updated) PARTITION BY DAY WAL
DEDUP UPSERT KEYS(last_updated, entity_id)`.

| Column | Type | Notes |
|---|---|---|
| `last_updated` | TIMESTAMP | **designated** timestamp, **dedup key** |
| `entity_id` | SYMBOL | **dedup key** |
| `domain` | SYMBOL | |
| `state` | VARCHAR | numeric states castable: `CAST(state AS DOUBLE)` |
| `attributes` | VARCHAR | JSON |
| `event_id` | VARCHAR | UUID generated by the integration for every accepted event — **not** the HA event id |
| `context_id` | VARCHAR | HA context id |
| `ingested_at` | TIMESTAMP | when the listener accepted the event, i.e. before the spool write (not the persistence time) |
| `last_changed` | TIMESTAMP | |

`unknown` states are skipped by the listener; `unavailable` is written.

## Architecture

State changes are captured by a single listener, wrapped into an
in-memory envelope (`entity_id`, state, attributes as JSON, the HA context id,
an integration-generated `event_id`, and the `last_updated`/`last_changed`/
ingestion timestamps) and pushed into a bounded queue. A background worker
drains the queue into a durable SQLite spool, batches rows and delivers them to
QuestDB over HTTP/ILP. Delivery is **at-least-once**: while QuestDB is
unreachable the spool grows (bounded); after recovery the backlog is delivered
and the dedup keys (`last_updated`, `entity_id`) make re-delivery idempotent.
The worker is an explicit state machine (`new → starting → running →
retry_wait → blocked → stopping → stopped`, with `failed` for a worker thread
that died) with exponential backoff, and it survives HA restarts: setup resumes
from the spool.

## Development

Unit and integration tests run inside a Home Assistant container
(`pytest tests/unit tests/integration`), pointed at a local QuestDB. The
mutation checkers under `dev/mutations/` re-run focused tests against a
deliberately broken copy of one file, so a test that cannot fail is visible;
`dev/mutations/README.md` has the exact commands. The
dev environment — compose stack with HA/QuestDB/Grafana, dashboards,
live-run checklist — is kept in a separate private repository.

## License

MIT — see [LICENSE](LICENSE).

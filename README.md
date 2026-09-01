# HASS QuestDB Writer

Stream every Home Assistant state change into
[QuestDB](https://questdb.com/) — durable, at-least-once, with server-side
deduplication.

A `service`-type integration: it subscribes to the Home Assistant event bus
and writes `state_changed` events to a QuestDB table over pure-Python
ILP/HTTP. No native QuestDB client dependency, no C extensions.

## Features

- **Durable spool first**: events land in a local SQLite spool (WAL,
  `synchronous=FULL`) before anything touches the network, so a QuestDB
  outage never loses data. Delivery retries with exponential backoff.
- **Exactly-once semantics**: at-least-once delivery from the durable spool
  plus QuestDB server-side dedup
  (`DEDUP UPSERT KEYS(last_updated, entity_id)`) makes replays and
  restarts no-ops.
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
| **Data retention (days)** | `0` (no limit) | 0–3650 | QuestDB [TTL](https://questdb.com/docs/concepts/ttl/): day partitions older than the window are dropped automatically (`ALTER TABLE … SET TTL n DAYS`); `0` disables |
| **Ingress queue capacity** | `1000` | 10–100000 | in-memory event queue between the HA listener and the SQLite spool |
| **Max serialized event bytes** | `65536` (64 KiB) | 1024–1048576 | largest event written; larger events are skipped (counted, never crash) |
| **Persist batch rows** | `100` | 1–10000 | rows per SQLite insert |
| **Delivery batch rows** | `1000` | 1–100000 | events per ILP batch |
| **Delivery batch bytes** | `524288` (512 KiB) | 4096–16777216 | bytes per ILP batch (whichever limit hits first) |
| **Flush interval (s)** | `1.0` | 0.05–300 | spool → delivery cadence at low event rates |
| **Retry initial (s)** | `1.0` | 0.1–300 | first backoff delay after a failed delivery |
| **Retry max (s)** | `60` | 1–3600 | backoff ceiling |
| **Retry multiplier** | `2.0` | 1–10 | exponential backoff factor |
| **Retry jitter ratio** | `0.2` | 0–1 | random jitter added to each delay (0–20%) |
| **Flush on shutdown** | off | on/off | try to deliver remaining events when HA stops; when off they stay in the spool and are delivered on next start (at-least-once) |
| **Max pending rows** | `100000` | 100–10000000 | SQLite spool capacity (rows) |
| **Max pending bytes** | `67108864` (64 MiB) | 1 MiB–1 GiB | SQLite spool capacity (bytes) |
| **Max dead-letter rows** | `1000` | 10–1000000 | ring buffer of undeliverable events (FIFO eviction) |
| **Max dead-letter bytes** | `16777216` (16 MiB) | 64 KiB–256 MiB | dead-letter capacity (bytes) |
| **SQLite busy timeout (s)** | `1.0` | 0.05–30 | retry window for spool lock contention |
| **HTTP timeout (s)** | `10` | 1–120 | per-request timeout for REST/schema and ILP POST |
| **Start timeout (s)** | `10` | 1–120 | how long setup waits for the worker thread |
| **Stop timeout (s)** | `15` | 1–300 | how long unload waits for the worker to drain |

## Health sensors

The integration provides five polled sensors (under the device
**HASS QuestDB Writer**) that read the in-memory writer snapshot — they
never touch QuestDB, so they keep reporting (and raising alarms) while
the server is unreachable:

| Entity | Meaning |
|---|---|
| `writer_state` | worker state: `new`/`starting`/`running`/`retry_wait`/`blocked`/`stopping`/`stopped`/`failed` |
| `seconds_since_last_delivery` | age of the last successful delivery (s) — **grows during an outage** |
| `pending_rows_in_spool` | undelivered rows buffered in SQLite |
| `events_delivered` | total events delivered (total_increasing) |
| `last_delivery_error` | text of the last delivery error, `none` when clean |
| `table_size_on_disk` | on-disk size of the entry's table (MB, decimal) — **queried from QuestDB**, goes `unavailable` during an outage; use it to plan retention, not for watchdog triggers |

Because the SQL integration's sensors freeze on their last value while
QuestDB is down, a write watchdog must trigger on `seconds_since_last_delivery`
(the health sensor keeps counting up) — not on SQL-derived values:

```yaml
alias: QuestDB write watchdog
triggers:
  - trigger: numeric_state
    entity_id: sensor.hass_questdb_writer_seconds_since_last_delivery
    above: 300
conditions:
  - condition: numeric_state
    entity_id: sensor.hass_questdb_writer_seconds_since_last_delivery
    above: 300
actions:
  - action: notify.mobile_app_phone
    data:
      title: "⚠️ Запись в QuestDB остановилась"
      message: "Последняя успешная доставка была более 5 минут назад."
mode: single
```

## Outage behavior

While QuestDB is unreachable the writer keeps buffering events in the
SQLite spool (at-least-once, bounded):

- **Capacity**: 100,000 rows / 64 MiB spool + 1,000 / 16 MiB
  dead-letter, tunable in **Configure → Show advanced settings**
  (see [Options](#options) above); at a typical 100–500 events/min
  that covers roughly 3–17 h of downtime
- **Full spool**: new events are dropped, counted in
  `overflow_events` (visible in diagnostics); the writer keeps retrying
  and delivers everything buffered once QuestDB is back
- **Logs**: rate-limited retry warnings (1st, 2nd, 4th… attempt), no spam

## Reading the data

The writer only writes; reading is done with any QuestDB client
([InfluxDB Line Protocol](https://questdb.com/docs/connect/compatibility/ilp/overview/)
for ingestion, SQL/REST/PGWire for queries):

- **Web Console** (`http://<host>:9000`) for ad-hoc queries,
- **Grafana** with the QuestDB data source (sample dashboards ship in the
  dev-stack repository),
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
WHERE entity_id = 'sensor.carbon_monoxide'
LATEST ON last_updated PARTITION BY entity_id;
```

**Events per hour** ([`SAMPLE BY`](https://questdb.com/docs/reference/sql/sample-by/),
[`dateadd`](https://questdb.com/docs/query/functions/date-time/)):

```sql
SELECT entity_id, count() AS events
FROM hass
WHERE last_updated > dateadd('h', -6, now())
SAMPLE BY 1h;
```

**Hourly average of a numeric entity** — states are stored as text, so
numeric aggregates need a [cast](https://questdb.com/docs/reference/sql/cast/):

```sql
SELECT entity_id, avg(CAST(state AS DOUBLE)) AS avg_state
FROM hass
WHERE entity_id = 'sensor.carbon_monoxide'
  AND last_updated > dateadd('h', -2, now())
SAMPLE BY 1h;
```

**Event volume (for retention planning)** ([meta functions](https://questdb.com/docs/query/functions/meta/)):

```sql
SELECT count(), size_pretty(sum(diskSize)) AS table_size
FROM table_partitions('hass');
```

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

## Data model

Table `hass` (owned by the integration, see
`docs/architecture.md`): `TIMESTAMP(last_updated) PARTITION BY DAY WAL
DEDUP UPSERT KEYS(last_updated, entity_id)`.

Troubleshooting: see [docs/troubleshooting.md](docs/troubleshooting.md).

| Column | Type | Notes |
|---|---|---|
| `last_updated` | TIMESTAMP | **designated** timestamp, **dedup key** |
| `entity_id` | SYMBOL | **dedup key** |
| `domain` | SYMBOL | |
| `state` | VARCHAR | numeric states castable: `CAST(state AS DOUBLE)` |
| `attributes` | VARCHAR | JSON |
| `event_id` | VARCHAR | HA event id |
| `context_id` | VARCHAR | HA context id |
| `ingested_at` | TIMESTAMP | when the writer persisted the event |
| `last_changed` | TIMESTAMP | |

`unknown` states are skipped by the listener; `unavailable` is written.
Full record-format rationale: [`docs/decisions/0005-questdb-record-format.md`](docs/decisions/0005-questdb-record-format.md).

## Architecture

- [`docs/architecture.md`](docs/architecture.md) — pipeline, worker state
  machine, durability model
- [`docs/decisions/`](docs/decisions/) — ADRs (0001–0007)

## Development

The dev environment (compose stack with HA/QuestDB/Grafana, dashboards,
live-run checklist) lives in the separate
[`hass-questdb-writer-devstack`](https://github.com/codeart/hass-questdb-writer-devstack)
repository.

Unit and integration tests run inside a Home Assistant container, see the
devstack `docs/development.md` for the commands.

## License

MIT — see [LICENSE](LICENSE).

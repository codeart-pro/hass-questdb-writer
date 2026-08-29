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
   - **Table** — table name to own (default `hass_questdb_writer_events`)
   - **Username / password** — optional HTTP Basic auth, both or neither
3. Finish the flow. The worker creates the table on first delivery; nothing
   is written until the schema is created and validated.

### Options (Configure)

- **Include / exclude** — entities, domains, globs (include-only acts as a
  strict allowlist; excludes cut, everything else is written)
- **Advanced** — queue/spool/dead-letter capacities, batch sizes, retry
  and timeout tuning (all have provisional defaults, see the architecture
  doc)

## Data model

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

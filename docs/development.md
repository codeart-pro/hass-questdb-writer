# Development environment

The local development stack runs Home Assistant, QuestDB, and Grafana as
podman containers via `compose.yaml`. The integration code is mounted
read-only into the Home Assistant container directly from the repository, so
edits are picked up on the next Home Assistant restart.

## Services and ports

| Service        | Container | Host port | Purpose                                   |
|----------------|-----------|-----------|-------------------------------------------|
| QuestDB        | `questdb` | `19000`   | REST API and Web Console (`:9000` inside) |
| QuestDB        | `questdb` | `19009`   | ILP TCP (`:9009` inside)                  |
| QuestDB        | `questdb` | `19003`   | Min server (`:9003` inside)               |
| QuestDB        | `questdb` | `18812`   | PostgreSQL wire protocol (`:8812` inside) |
| Home Assistant | `homeassistant` | `18123` | UI and API (`:8123` inside)           |
| Grafana        | `grafana` | `13000`   | UI and API (`:3000` inside)               |

Grafana uses the official QuestDB data source plugin
(`questdb-questdb-datasource`), installed automatically via
`GF_INSTALL_PLUGINS`, and connects to QuestDB over the PostgreSQL wire
protocol (`18812`) with the QuestDB default read-only user `admin`/`quest`.
Data sources and dashboards are provisioned from `dev/grafana/`:

- `dev/grafana/provisioning/datasources/questdb.yaml` — the QuestDB data
  source (uid `questdb`);
- `dev/grafana/provisioning/dashboards/dashboards.yaml` — the dashboard
  provider;
- `dev/grafana/dashboards/questdb-events.json` — the `QuestDB Events`
  dashboard (entity dropdown, numeric state values, events-per-hour,
  per-entity volume table).

Grafana admin credentials for the local stack: **`test` / `test`**. They are
development-only and configured through `GF_SECURITY_ADMIN_USER` and
`GF_SECURITY_ADMIN_PASSWORD` in `compose.yaml`.

The Home Assistant development configuration lives in
`dev/homeassistant/` (mounted as `/config`): a minimal `configuration.yaml`
with the integration logger at debug level and one `input_boolean.questdb_test`
helper. `.storage` and the databases there are gitignored.

## Starting the stack

```shell
podman compose up -d
```

Recreating the QuestDB container (for example after changing the published
ports) keeps the `questdb-data` volume, so previously written tables survive.

## Live run checklist

1. **Add the integration** in the Home Assistant UI
   (`http://localhost:18123`, Settings → Devices & Services → Add
   Integration → HASS QuestDB Writer): host `questdb`, port `9000`, leave
   the table at its default. The worker creates the table with the
   ADR-0005 schema (WAL, daily partitions, `DEDUP UPSERT KEYS`) and blocks
   delivery until the schema check passes.
2. **Enable the Demo platform** in the same UI to generate a steady stream
   of self-updating sensors, or toggle `input_boolean.questdb_test` by hand.
3. Optionally restrict the written entities through the integration's
   *Configure* options flow (include/exclude entities, domains, globs).
4. Verify the data in QuestDB (`http://localhost:19000`, Web Console):

   ```sql
   SHOW COLUMNS FROM hass_questdb_writer_events;   -- designated + upsert keys
   SELECT count() FROM hass_questdb_writer_events;
   SELECT entity_id, count() FROM hass_questdb_writer_events
     SAMPLE BY 1d ALIGN TO CALENDAR;
   ```

   Numeric states are stored as VARCHAR; cast them in queries:
   `CAST(state AS DOUBLE)`. `unknown` states are skipped by the listener;
   `unavailable` is written.

5. **Restart the Home Assistant container** (`podman restart
   hass-questdb-writer-homeassistant-1`). Restored states are re-emitted at
   startup with their original `last_updated`; the dedup upsert keys make
   these replays no-ops, so row counts must not double. This closes the
   ADR-0005 "Home Assistant restart with restored states" verification.
6. Open Grafana (`http://localhost:13000`, `test`/`test`) and check the
   `QuestDB Events` dashboard: pick an entity in the dropdown, confirm the
   numeric series and the volume table.

## Running the tests

Unit and integration tests run inside the Home Assistant container:

```shell
podman cp custom_components hass-questdb-writer-homeassistant-1:/tmp/run/
podman cp tests hass-questdb-writer-homeassistant-1:/tmp/run/
podman exec -w /tmp/run -e PYTHONPATH=/tmp/run \
  hass-questdb-writer-homeassistant-1 python3 -m pytest tests/unit -q
podman exec -w /tmp/run -e PYTHONPATH=/tmp/run \
  hass-questdb-writer-homeassistant-1 python3 -m pytest tests/integration -q
```

Integration tests hit the real QuestDB container over its internal network
name `questdb:9000`. The dev Home Assistant instance must be running for the
tests that boot their own Home Assistant harness (port 8123 is already taken
inside the container).

# Troubleshooting

Symptoms, causes, and fixes for the HASS QuestDB Writer integration.

## Nothing is written to QuestDB

The writer pauses delivery while the schema is being created/validated and
while the connection is failing. Check the integration state:

1. **Devices & Services → HASS QuestDB Writer → ⋯ → System options**
   — if a **Repair issue** is shown («QuestDB rejected the credentials»):
   the HTTP Basic credentials are wrong. Open **Reconfigure** and update
   the username/password.
2. Otherwise open **Settings → System → Logs** and filter by
   `hass_questdb_writer`. A blocked worker logs the last delivery error
   (rate-limited, so only a few lines). Typical messages:

   | Message | Meaning | Fix |
   |---|---|---|
   | `could not reach QuestDB` / connection refused | Wrong host/port or QuestDB down | Reconfigure; check the host reachability from the HA container |
   | `HTTP 401` / rejected credentials | Wrong username/password | Reconfigure |
   | `table ... does not match the owned schema` | Table was created outside the integration (Web Console, Grafana…) | Drop the table, or point the entry at a fresh table name |
   | `spool is full` | SQLite spool hit its capacity while QuestDB was unreachable | Fix the connection; events stay in the dead letter |

## The table is not created

The table is created by the worker on the first delivery attempt, and only
when the schema check passes. Nothing is created at setup time. If the
table is missing after the first event:

- verify the entry host/port from **inside** the HA container — with a
  podman/docker compose stack, use the **container-network** port (e.g.
  `questdb:9000`), not the host-published port;
- check the logs for a schema error (`SHOW COLUMNS` failure).

## Duplicate-looking rows

The table is `DEDUP UPSERT KEYS(last_updated, entity_id)`: re-delivered
events replace the existing row instead of inserting a second one. A
state that genuinely changed twice at different `last_updated` values
produces two rows — that is history, not a duplicate.

## Data disappears / retention

The **Data retention** advanced option (TTL) makes QuestDB drop whole day
partitions older than the window. The TTL applies asynchronously; check
it with:

```sql
SELECT table_name, ttlValue, ttlUnit FROM tables() WHERE table_name = '…';
```

`SHOW CREATE TABLE` does not render the TTL clause on deduplicated WAL
tables even when it is active.

## Restarting Home Assistant

Delivery is at-least-once: events buffered in the SQLite spool survive a
HA restart and are re-delivered; the dedup keys make the re-delivery
idempotent.

## Getting help

Include in any bug report: HA version, QuestDB version, the integration
diagnostics (downloadable from Devices & Services), the log excerpt
filtered by `hass_questdb_writer`, and the table DDL
(`SHOW CREATE TABLE …`).

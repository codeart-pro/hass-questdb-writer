# Real-world QSS data analysis

Date: 2026-08-29

## Source

A production QuestDB instance on the private LAN (`http://192.168.1.215:9000`)
whose table `qss` has been written continuously for months by the
[QSS](https://github.com/CM000n/qss) Home Assistant component. Inspected
read-only; the instance has authentication disabled. Used to calibrate the
record format and sizing decisions of HASS QuestDB Writer.

Table characteristics:

| Property | Value |
|---|---|
| Schema | `entity_id` SYMBOL, `state` VARCHAR, `attributes` VARCHAR, `timestamp` TIMESTAMP (designated) |
| Partitioning / WAL | DAY / WAL |
| Dedup | none |
| Rows | 137,060,056 |
| Range | 2026-02-14T14:45:09Z .. 2026-08-29T11:05:10Z (still flowing) |
| Days with data | 134 |

## Volume and event rate

| Metric | Value |
|---|---:|
| Events per day, median | 1,086,380 |
| Events per day, mean | 1,022,846 |
| Events per day, max | 1,675,948 |
| Events per day, min (partial day) | 184,607 |
| Events per minute, max (2026-07-09) | 3,680 (~61/s) |
| Events per minute, p99 (2026-07-09) | 1,638 (~27/s) |
| Events per minute, median (2026-07-09) | 780 (~13/s) |

The daily rate grew from roughly 200k-500k events in February to about 1.2M in
August. The hour-of-day distribution is nearly flat (24/7 operation): the
quietest hour (00:00) carried 4.99M events over the whole range, the busiest
(13:00) 6.56M.

## Outage behavior

Three gaps contain no rows at all:

| Gap | Days |
|---|---:|
| 2026-03-21 .. 2026-03-23 | 3 |
| 2026-05-02 .. 2026-05-11 | 10 |
| 2026-07-10 .. 2026-08-28 | 50 |

QSS keeps events only in an in-memory queue with a bounded retry window, so
every outage gap is permanently lost data (roughly 55M events for the 50-day
gap at the then-current rate). This is the real-world failure mode the durable
spool of HASS QuestDB Writer is designed against.

## Cardinality and payload shape

| Metric | Value |
|---|---:|
| Distinct entity IDs | 2,720 (1,674 in Feb, 2,678 since Aug 1) |
| Distinct domains | 20; `sensor` alone 96.6% of rows |
| State length, average | 5.8 chars |
| Attributes length, average | 139.4 B |
| Attributes length, p50 / p90 / p99 | 143 / 159 / 191 B |
| Attributes length, max | 2,011 B |
| `unavailable` states | 1,030,089 (0.75%) |

State values are arbitrary strings without numeric coercion: `on`, `0.99`,
`unavailable`, `not_home`, and even a literal `""` (368,884 rows from
`sensor.motion_times`).

## Duplicates

A full-range scan for repeated `(timestamp, entity_id)` pairs found **zero**
duplicates in all 137,060,056 rows. QSS sends one row per TCP flush and retries
for up to 12.5 minutes, so its uncertain-delivery window is small. HASS QuestDB
Writer sends HTTP batches of up to 1,000 rows, which widens the uncertain
window; the `DEDUP UPSERT KEYS(last_updated, entity_id)` decision in
[ADR 0005](../decisions/0005-questdb-record-format.md) is therefore retained as
a cheap, hard guarantee rather than a response to measured duplicates.

## Size estimate

Data-level bytes per row (exact fixed widths plus SQL-measured string lengths;
excludes QuestDB 16 MiB column-file allocation slack):

| Format | Bytes per row | 137M rows | Per 30 active days | Per 30 wall days |
|---|---:|---:|---:|---:|
| QSS minimal | ~157 | ~21.5 GB | ~4.8 GB | ~3.3 GB |
| HASS QuestDB Writer (ADR 0005) | ~253 | ~34.7 GB | ~7.8 GB | ~5.3 GB |

The 96 B/row difference buys `domain`, `ingested_at`, `last_changed`,
`event_id`, and `context_id`, plus database-level deduplication. "Active days"
exclude the outage gaps; "wall days" use the full 2026-02-14 .. 2026-08-29
range.

## Implications for HASS QuestDB Writer

- `entity_id` cardinality of a few thousand confirms SYMBOL storage (4 B/row
  versus ~30 B/row for VARCHAR) and fast Grafana `DISTINCT`/`WHERE`/`LATEST ON`
  queries.
- The measured peak of ~61 events/s fits the provisional ingress queue
  (1,000 events ≈ 16 s of peak burst), the 100-event persist batch, and the
  100,000-row spool (≈ 2 hours at the median rate).
- Real payloads (p99 191 B, max 2,011 B) leave the 64 KiB per-event cap with
  ~340x margin; a 1,000-row delivery batch is ~160 KB, so the row bound binds
  before the byte bound.
- The 0.75% share of `unavailable` states justifies the skip-`unknown`,
  keep-`unavailable` listener policy.
- At ~253 B/row the ADR 0005 format adds roughly 61% to the archived data
  volume compared with the minimal QSS format.

## Queries used

```sql
-- metadata
select id, table_name, designatedTimestamp, partitionBy, walEnabled, dedup,
       table_row_count, table_min_timestamp, table_max_timestamp
from tables();

-- cardinality and lengths
select count_distinct(entity_id) from qss;
select round(avg(length(state)),2), round(avg(length(attributes)),2),
       max(length(attributes)) from qss;
select approx_percentile(length(attributes), 0.5),
       approx_percentile(length(attributes), 0.9),
       approx_percentile(length(attributes), 0.99),
       max(length(attributes)) from qss;

-- rates
select timestamp, count() from qss sample by 1d;
select timestamp, count() as c from qss
where timestamp > '2026-07-09T00:00:00Z' and timestamp < '2026-07-10T00:00:00Z'
sample by 1m;

-- shape
select split_part(entity_id, '.', 1) as domain, count()
from qss group by 1 order by 2 desc;
select state, count() from qss group by state order by 2 desc limit 15;

-- duplicates (full range)
select count() from (
    select timestamp, entity_id, count() as c
    from qss group by timestamp, entity_id
) where c > 1;
```

## Limitations

- Single production deployment; the exact Home Assistant configuration is not
  part of this record.
- Data was written by QSS (TCP, per-event flush), not by HASS QuestDB Writer;
  ingestion path and retry behavior differ.
- The instance is on the private LAN; do not reproduce against it from other
  networks, and do not run any write or DDL query against it.

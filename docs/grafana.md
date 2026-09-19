# Grafana panels

Date: 2026-09-19

Two worked examples of a Grafana time-series panel on the `hass` table: one
reading a value from `state`, one reading it from `attributes`. The query bodies
were run against QuestDB 10.0.1 with the entity ids of a live installation, and
they use only what the
[official QuestDB plugin](https://grafana.com/grafana/plugins/questdb-questdb-datasource/)
documents ([Grafana guide](https://questdb.com/docs/integrations/visualization/grafana/),
[JSON functions](https://questdb.com/docs/query/functions/json/)).

## What the table looks like

One row per state change:

| Column | Meaning |
|---|---|
| `last_updated` | the designated timestamp — **the time column of every panel** |
| `entity_id`, `domain` | SYMBOL columns, cheap to filter on |
| `state` | the state as text, exactly as Home Assistant reported it |
| `attributes` | the attribute set as a JSON document in a VARCHAR column |

There is no `timestamp` column: in this table the designated timestamp is
`last_updated`.

## Data source

Use the official QuestDB plugin (`questdb-questdb-datasource`) — the PostgreSQL
plugin works too but is configured differently, and the plugin's macros
(`$__timeFilter`, `$__sampleByInterval`) are what make the queries below
write-once. Point it at the QuestDB server (PGWire port **8812** by default) and
switch the panel's editor from Query Builder to **SQL**.

## Example 1 — a value that lives in `state`

A numeric sensor, plotted as it changed:

```sql
SELECT
  last_updated AS time,
  CAST(state AS DOUBLE) AS value
FROM hass
WHERE entity_id = 'sensor.example_temperature'
  AND $__timeFilter(last_updated)
  AND state NOT IN ('unavailable', 'unknown')
ORDER BY last_updated
```

What matters here:

- **`$__timeFilter(last_updated)`** expands to a `BETWEEN` on the dashboard's
  time range (`last_updated >= cast(… as timestamp) AND last_updated <= cast(…)`),
  which is what keeps the query bounded as the user zooms.
- **`CAST(state AS DOUBLE)`** is what turns text into a number. `state` is a
  VARCHAR column, so without the cast the panel shows strings: aggregates fail or
  sort alphabetically.
- **`state NOT IN ('unavailable', 'unknown')`** keeps the two non-numeric states
  out of the series. Without it the cast returns `NULL` for them (verified:
  `CAST('unavailable' AS DOUBLE)` is `NULL`), which a timeseries panel renders as
  a gap — correct for an outage, misleading for a value panel.
- **The first column is the timestamp, aliased `time`** because that is how the
  plugin expects a time series to arrive.

## Example 2 — a value that lives inside `attributes`

The same panel type for a number stored in the attribute JSON — here the
humidity of a weather entity:

```sql
SELECT
  last_updated AS time,
  CAST(json_extract(attributes, '$.humidity') AS DOUBLE) AS humidity
FROM hass
WHERE entity_id = 'weather.example_home'
  AND $__timeFilter(last_updated)
  AND json_extract(attributes, '$.humidity') IS NOT NULL
ORDER BY last_updated
```

What matters here:

- **`json_extract(attributes, '$.humidity')` returns a VARCHAR** unless it is cast
  immediately — the QuestDB documentation is explicit about this, and it is the
  single most common way this panel ends up plotting text. `CAST(… AS DOUBLE)`
  and `…::double` are equivalent (verified on the same rows: both returned
  `25.08`), so use whichever reads better in the query.
- **`IS NOT NULL`** drops the rows that do not carry this attribute at all —
  otherwise they arrive as `NULL` and pad the series with gaps. A missing key
  extracts to `NULL` rather than erroring, so the filter is the whole story.
- **Add `AND state <> 'unavailable'`** when the panel has to be honest about
  outages: an `unavailable` row often still carries the *previous* attribute
  block, so a series filtered only by `IS NOT NULL` can look continuous across a
  stretch where the entity was not reporting.
- Any JSON type converts, not just numbers: the same path syntax `$.field[0]`
  reaches arrays, and `::varchar`/`::boolean`/`::timestamp` are available for the
  other shapes (see the JSON reference above).

## Long ranges: let the plugin choose the interval

Plotting months of raw rows is slow and illegible. Aggregate them, and let the
plugin pick the bucket from the dashboard's zoom level:

```sql
SELECT
  last_updated AS time,
  avg(CAST(state AS DOUBLE)) AS avg_state
FROM hass
WHERE entity_id = 'sensor.example_temperature'
  AND $__timeFilter(last_updated)
  AND state NOT IN ('unavailable', 'unknown')
SAMPLE BY $__sampleByInterval
```

- **`$__sampleByInterval`** is the plugin's macro for `SAMPLE BY`: it emits an
  interval in QuestDB units (`s`, `T`, `h`, `d`) and follows the zoom level, so
  the same query is readable at any range. The effect is measurable: on a live
  4-hour window, 586 raw rows became 4 buckets with `1h` and 46 buckets with `5m`.
  Grafana's own `$__interval` is a Grafana duration string; the plugin documents
  `$__sampleByInterval` for this purpose, so prefer it.
- The aggregate has to be cast for the same reason as above; `avg(CAST(state AS
  DOUBLE))` with a plain `state` averages text.

## Pitfalls, in one list

| Symptom | Cause |
|---|---|
| The panel says "no data" although rows exist | the time column is not the designated timestamp (`last_updated`), or it is not the first column / not aliased `time` |
| The series is flat, empty or sorts oddly | a missing `CAST(… AS DOUBLE)` on `state` or on `json_extract(...)` |
| Gaps where the entity was offline should be values | `state NOT IN ('unavailable', 'unknown')` missing, or, for an attribute panel, `state <> 'unavailable'` missing |
| The query is slow on a wide range | no `$__timeFilter(...)` bound, or no `SAMPLE BY` for the long ranges |
| The dashboard refreshes slower than expected | Grafana caps the dashboard refresh interval by default (5 s); raise it in the dashboard settings, not in the query |

## Checking a query without Grafana

The panel is just SQL, so the body can be run in the
[Web Console](https://questdb.com/docs/getting-started/web-console/overview/) or
over HTTP — replace the macro with a literal range to get exactly what the plugin
sends:

```sql
SELECT last_updated AS time, CAST(state AS DOUBLE) AS value
FROM hass
WHERE entity_id = 'sensor.example_temperature'
  AND last_updated BETWEEN '2026-09-18T00:00:00Z' AND '2026-09-19T00:00:00Z'
  AND state NOT IN ('unavailable', 'unknown')
ORDER BY last_updated;
```

Sample dashboards built on these queries ship in the dev-stack repository (see
the README); this page documents the queries themselves, so they can be rebuilt
in any Grafana.

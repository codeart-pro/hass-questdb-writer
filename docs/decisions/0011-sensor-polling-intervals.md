# ADR 0011: Explicit polling intervals for the sensor entities

Status: accepted.

Date: 2026-09-12

## Context

Both sensor classes declared `should_poll = True`, but the platform module
defined no interval, so Home Assistant applied the sensor-domain default of
30 seconds: the sensor component declares
`SCAN_INTERVAL: Final = timedelta(seconds=30)` in
`homeassistant/components/sensor/__init__.py` and passes it to its
`EntityComponent`, while `EntityComponent.async_setup_entry` reads the platform
module with `scan_interval=getattr(platform, "SCAN_INTERVAL", None)` and falls
back to that component value when the platform defines none. Verified against HA
2025.1.0 (the minimum in `hacs.json`) and HA 2026.7.2 (the dev stack): both 30 s.

That inherited default caused two problems:

1. The quality-scale rule *appropriate-polling* (Bronze, "There are no
   exceptions to this rule") requires the interval to be an explicit decision
   rather than an inherited default.
2. The default was wrong for one entity. `QuestDbTableSizeSensor.async_update`
   runs `SELECT sum(diskSize) FROM table_partitions('<table>')` over HTTP, so
   it issued 2,880 queries per config entry per day for a value that only
   grows. The five health sensors read the in-memory runtime snapshot only
   (no I/O), where 30 s is cheap: `snapshot()` measured p50 4.4 µs, p99 ~11 µs
   over 20,000 calls (macOS arm64, Python 3.11.15).

A single module constant cannot serve both cadences: an entity platform has one
interval (`EntityPlatform.scan_interval_seconds`, one `_async_polling_timer`)
shared by all its entities, and per-entity intervals are not supported — an
entity can only opt out of the platform poll with `should_poll = False`.

## Decision

- `sensor.py` defines `SCAN_INTERVAL = timedelta(seconds=30)` for the five
  in-memory health sensors. The value is unchanged, but it is now explicit and
  justified: the health values are free to read, the README's watchdog
  automation needs to see a stuck writer within a minute, and a user can force
  a refresh with `homeassistant.update_entity`.
- `QuestDbTableSizeSensor` leaves platform polling (`should_poll = False`) and
  owns a slower timer: `TABLE_SIZE_SCAN_INTERVAL = timedelta(minutes=5)`,
  registered with `async_track_time_interval` in `async_added_to_hass` and
  cancelled in `async_will_remove_from_hass`. It measures once while being
  added — HA writes the state after `async_added_to_hass` returns
  (`Entity.add_to_platform_finish`) — and afterwards refreshes and writes its
  own state, which is the pattern the developer docs prescribe for a
  non-polling entity ("Push vs poll").
- Five minutes for the table size: the entity exists to plan retention and
  watch growth (README), its value is a sum over partitions that only grows,
  and 288 queries/day is a defensible bound. An explicit
  `homeassistant.update_entity` call still refreshes it immediately.

## Alternatives considered

- **One platform `SCAN_INTERVAL` for everything** (e.g. 5 min): rejected — it
  would slow the watchdog sensors tenfold to fix one entity.
- **`DataUpdateCoordinator` with `update_interval`**: the docs' first choice
  for polling an API, but rejected here: a coordinator for a single integer
  sensor adds wiring plus its own error and logging semantics, while
  `async_update` already classifies transport errors and drives availability.
- **Throttling inside `async_update`** (query only when
  `now - last >= 5 min`): rejected — the entity would keep waking on every
  platform tick, HA would still publish a state every 30 s, and the real query
  cadence would be invisible in the entity model.
- **Push from the writer worker**: rejected — the table size is not known to
  the spool or the transport, and turning the health sensors into push entities
  would write state on every delivery instead of every 30 s (more state machine
  writes, not fewer).

## Consequences

- QuestDB queries for the table-size sensor drop from 2,880/day to 288/day per
  config entry.
- The health sensors keep their 30 s behaviour, now stated in the code and in
  the README.
- The table-size entity updates from its own timer instead of the platform poll
  loop; it still reports `unavailable` on transport errors, and a refresh
  requested with `homeassistant.update_entity` still works.
- A failed query therefore leaves the entity `unavailable` until the next tick —
  up to 5 minutes instead of up to 30 s — and `homeassistant.update_entity`
  retries immediately. The failure stays silent in the log because
  `async_update` swallows `IlpTransportError` and only flips availability
  (pre-existing behaviour, now visible for longer). Measured on the dev stack:
  `12:49:09.466 unknown` → `12:54:09.472 unavailable` → `12:59:09.513 218.1`,
  i.e. exactly 300 s apart, with the value matching a direct
  `SELECT sum(diskSize) FROM table_partitions('hass')` (218,103,808 bytes).
- The timer is registered per entity instance and cancelled on removal, so
  unloading a config entry (or disabling the entity) leaves no scheduled work.
- `quality_scale.yaml` (tracked separately) can mark `appropriate-polling` as
  done with this ADR as the justification.

## Verification

- Unit tests in `tests/unit/test_sensor_platform.py`: the platform interval is
  exposed and equals 30 s, the health sensors poll, the table-size sensor does
  not poll, it registers exactly one timer with the 5-minute interval, its
  timer callback refreshes the value and writes the state, and removal cancels
  the timer.
- Container integration tests (`tests/integration`) still cover clean startup,
  reload and delivery with the sensors in place.

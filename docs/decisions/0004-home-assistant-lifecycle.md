# ADR 0004: Bind one writer to one Home Assistant config entry

Status: accepted for the `0.1.0-dev0` local runtime.

Date: 2026-08-29

## Context

The independent worker is useful only if Home Assistant owns it predictably.
Reload must not leave duplicate event listeners or abandoned threads, and the
event-loop callback must not perform disk or network I/O.

Home Assistant APIs were checked against the installed 2026.7.2 source rather
than assumed from earlier versions. That inspection found:

- config entries expose typed `runtime_data`;
- `async_add_executor_job` is the supported way to invoke blocking lifecycle
  work from the event loop;
- `async_track_state_change_event` indexes literal entity IDs;
- passing `MATCH_ALL` to that indexed helper does not subscribe to every state;
- `hass.bus.async_listen(EVENT_STATE_CHANGED, ...)` is required for the current
  all-entity development mode;
- the supported serializer is `homeassistant.helpers.json.json_dumps`, not the
  removed `homeassistant.util.json.json_dumps` symbol.

## Decision

One config entry owns one `HassQuestDbRuntime` through `entry.runtime_data`.

Setup order:

1. Construct an explicit `RuntimeConfiguration`.
2. Start the worker through `async_add_executor_job`.
3. Publish the runtime in `entry.runtime_data` only after startup succeeds.
4. Register exactly one state listener after the worker is ready.

Unload order:

1. Remove the listener and reject future state events.
2. Call the worker's bounded stop through `async_add_executor_job`.
3. Return `False` to Home Assistant if the worker does not join by its deadline.

The callback copies only immutable scalar state metadata, serializes attributes
with Home Assistant's JSON helper, constructs an `EventEnvelope`, and calls the
non-blocking `submit()`. It performs no SQLite or HTTP work. Repeated conversion
or submission errors are logged at power-of-two counts to prevent unbounded log
volume while keeping continued failure visible.

## Timestamp mapping

Home Assistant provides timezone-aware `datetime` values. Conversion uses
integer day/second/microsecond arithmetic instead of multiplying a floating
Unix timestamp, avoiding avoidable precision loss:

- event `time_fired` becomes the QuestDB designated timestamp;
- runtime wall time becomes `ingested_at`;
- state `last_changed` and `last_updated` remain separate timestamp fields;
- all three metadata fields are sent in QuestDB timestamp microseconds.

## Provisional local profile

These values enable local end-to-end testing and are explicitly not production
defaults:

| Setting | Provisional value | Evidence / limitation |
|---|---:|---|
| ingress queue | 1,000 events | burst guard; production burst not measured |
| serialized event | 64 KiB | explicit safety bound; attribute distribution unknown |
| SQLite transaction | 100 events | local benchmark reached near-plateau at 100 |
| HTTP batch rows | 1,000 | local HTTP benchmark benefited from 1,000 rows |
| HTTP batch bytes | 512 KiB | below standard QuestDB HTTP receive buffer of 1 MiB |
| flush latency | 1 second | matches official ILP client interval default |
| HTTP timeout | 10 seconds | matches official ILP client request default |
| retry | 1 to 60 seconds, ×2, 20% jitter | policy still needs outage testing |
| pending payload | 100,000 rows / 64 MiB | production rate and disk unknown |
| dead-letter payload | 1,000 rows / 16 MiB | operator workflow not implemented |
| shutdown | 15 seconds, no remote flush | exceeds HTTP timeout; favors local durability |

SQLite file, index, metadata, and WAL overhead are outside the payload limits.

## Consequences

Positive:

- config-entry reload has an explicit ownership boundary;
- no blocking storage or network call runs on the HA event loop;
- startup and unload failures are propagated instead of hidden;
- runtime and worker diagnostics can be read without crossing SQLite threads;
- unsupported state attributes reject one event without killing the listener.

Negative:

- all-entity mode receives every state-change event before future filters run;
- serialization still consumes event-loop CPU and must be load-tested with
  unusually large attributes;
- authentication, filters, options flow, and repair UI are not implemented;
- provisional limits need replacement or explicit user configuration before a
  production release.

## Verification completed

- worker starts before listener registration;
- listener is removed before worker stop;
- failed startup does not publish `runtime_data`;
- unload returns the real worker stop result;
- HA-specific sets and datetimes serialize through the HA helper;
- state removal, conversion failure, and submission rejection remain contained;
- exact integer timestamp conversion is tested;
- Home Assistant 2026.7.2 config check passes;
- the HA config-flow manager creates and automatically loads a real entry;
- `input_boolean.questdb_test` reaches a real QuestDB through the HA event bus;
- config-entry reload stops the old listener and worker before replacing them;
- one event before and one event after reload produce exactly two QuestDB rows;
- config-entry unload reaches `NOT_LOADED` without leaving the worker alive.

## Required verification

- add include/exclude filters before enabling production all-entity export;
- measure event-loop callback duration and production event rate;
- test HA stop/restart while QuestDB is unavailable;
- expose diagnostics and repairs for blocked, failed, full, and dead-letter
  states;
- design options migration before any provisional constant becomes supported.

## References

- QuestDB HTTP server defaults:
  <https://questdb.com/docs/configuration/http-server/>
- QuestDB ILP client parameters:
  <https://questdb.com/docs/connect/compatibility/ilp/overview/>

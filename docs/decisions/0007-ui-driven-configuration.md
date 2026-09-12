# ADR 0007: UI-driven configuration via config entry and options flow

Status: accepted.

Date: 2026-08-29

## Context

The integration was configured entirely from `PROVISIONAL_*` constants in
`const.py`: the config flow exposed only host, port, table, and TLS, while
every tuning knob (queue, spool, batch, retry, timeout limits) and the entity
filter were compile-time values. There was no way for an installed user to
authenticate to QuestDB, to select which entities are written, or to adjust
any limit without editing source code.

Home Assistant is UI-first for config-entry integrations: users install a
custom component through HACS and configure it through Settings. The config
flow already existed, so the question was how far to extend it.

## Decision

All configuration is UI-driven. The config entry owns the connection, the
options flow owns filters and tuning:

- **`entry.data` (setup step "user")**: `host`, `port`, `table`, `use_tls`,
  and optional `username`/`password` (HTTP Basic auth; QuestDB runs without
  authentication by default, so both fields default to empty and must be
  provided together). Changing these requires re-adding the entry.
- **`entry.options` (options flow, step "init")**: the include/exclude entity
  filter. The stored shape is the standard
  `{"include": {domains, entity_globs, entities}, "exclude": {...}}` dict
  validated by `INCLUDE_EXCLUDE_BASE_FILTER_SCHEMA` and applied at runtime
  with `convert_include_exclude_filter`. The Home Assistant filter semantics
  are used as-is: an entity include list alone acts as an allow-list;
  exclude rules remove; everything not matched stays included.
- **`entry.options` (options flow, step "advanced")**: all `PROVISIONAL_*`
  limits, reached through a "Show advanced settings" checkbox on the init
  step. Every value defaults to the matching `PROVISIONAL_*` constant, so
  entries created before an option existed keep working without a migration;
  `entry.version` stays 1.
- The frontend has no `entity_filter` selector in this HA version
  (verified against the bundled frontend), so the filter is built from
  standard selectors: entity multi-selects plus comma-separated text fields
  for domains and globs.
- The `show_advanced_options` flow property is deprecated (scheduled for
  removal in 2027.6), so the advanced step is reached through the flow's own
  boolean field instead.
- Saving options triggers a reload: `async_setup_entry` registers
  `entry.add_update_listener(async_update_options)`, and
  `async_update_options` calls `async_reload`. The runtime rebuilds the
  worker, spool, and filter from the new options.

## Consequences

Positive:

- users configure authentication, filters, and limits entirely through the
  UI, with translated strings (EN) and selector validation;
- secrets stay in the config entry storage, never in YAML or logs;
- the runtime remains unchanged: it still receives explicit configuration
  dataclasses, only the assembly point changed;
- cross-field validation lives in the flow (retry bounds, event size vs
  dead-letter capacity) and prevents invalid runtime profiles.

Negative:

- the options flow exposes experimental `PROVISIONAL_*` values as tunable
  fields; the advanced step description marks them as a development profile
  until production validation selects defaults;
- options changes always reload the entry, which briefly stops the listener
  and re-opens the spool.

## Notes from implementation

- `async_set_unique_id` requires a live `hass` (it queries in-progress
  flows); the config-flow unit tests mock it, the options-flow tests run
  against the real Home Assistant harness.
- A real race was found and fixed in the worker: `start()` waited for
  `_ready` and then required `WorkerState.RUNNING`, but the schema gate can
  legitimately move the worker to `RETRY_WAIT`/`BLOCKED` before the start
  caller reads the state when QuestDB is unreachable. `start()` now accepts
  every state of a live worker and only fails on `FAILED` or uninitialized
  states, preserving the durability promise that setup never depends on
  QuestDB availability.

## References

- ADR 0005 (record format) and ADR 0006 (dead-letter retention) define the
  runtime profile the options flow tunes.
- `homeassistant.helpers.entityfilter` (`INCLUDE_EXCLUDE_BASE_FILTER_SCHEMA`,
  `convert_include_exclude_filter`, `EntityFilter`) in HA 2026.7.2.

# Integration quality scale

Target: [Home Assistant integration quality scale — Gold](https://www.home-assistant.io/docs/quality_scale/).

Gold is the release bar for the first public version.

**Rule-by-rule status lives in
[`custom_components/hass_questdb_writer/quality_scale.yaml`](../custom_components/hass_questdb_writer/quality_scale.yaml).**
That file is the source of truth: one entry per rule of every tier, with
`done`, `todo` (plus the reason) or `exempt` (plus the reason). Rule IDs and
tiers come from the official rule list
([rules/](https://github.com/home-assistant/developers.home-assistant/tree/master/docs/core/integration-quality-scale/rules))
and the [checklist](https://developers.home-assistant.io/docs/core/integration-quality-scale/checklist).
This document holds the narrative: what the tiers mean for this project, what was
verified and how, and what is deliberately out of scope.

Corrected on 2026-09-12: the earlier version of this file claimed
"Bronze — baseline (done)" from the tier *characteristics* instead of the rule
list. The rule-by-rule pass found four open Bronze rules, all four of which were
closed the same day (`has-entity-name`, `test-before-setup`,
`docs-removal-instructions`, `common-modules`). The four open Silver rules
(`parallel-updates`, `log-when-unavailable`, `reauthentication-flow`,
`test-coverage`) were closed the same day as well.

## Current status (2026-09-12, rule-by-rule)

| Tier | done | open (todo) | exempt | Tier claimable |
|---|---:|---:|---:|---|
| Bronze | 15 | 0 | 5 | **yes** |
| Silver | 9 | 0 | 1 | **yes** |
| Gold | 10 | 5 | 6 | no |
| Platinum | 0 | 1 | 2 | not targeted |

Open rules, with the reason recorded in the YAML:

- **Gold**
  - `entity-category`, `entity-translations`, `icon-translations` — the six
    entities carry literal names and icons, have no translation keys and no
    `EntityCategory`.
  - `entity-disabled-by-default` — all entities are enabled; the decision which
    diagnostics to disable needs to be made against the watchdog requirement.
  - `docs-known-limitations` — limitations are documented but scattered, not
    collected in one place.
- **Platinum**
  - `strict-typing` — the code is annotated, but no strict mypy gate exists.

Exempt rules fall into three groups: the integration has no service actions,
conditions or triggers; it talks to no device (no discovery, no dynamic or stale
devices, nothing to inject a websession into); or the rule assumes core-only
mechanics (brands repository, core team ownership, PyPI dependency
transparency).

## Verified behaviour behind the `done` rules

- **Auth-failure visibility** (`repair-issues`): on `AuthenticationIlpError` the
  worker goes `BLOCKED` (snapshot `block_reason="auth"`) and the runtime raises a
  persistent ERROR repair issue that disappears once the worker recovers or the
  entry unloads; delivery resumes automatically after the options are corrected
  (reload).
- **Recovery without log spam** (`action-exceptions`, logging behaviour): retries
  log rate-limited warnings (powers of two: 1, 2, 4, … attempts) saying how long
  the backoff waits and that events stay in the spool; verified against a live
  QuestDB outage in the dev stack (spool grows, delivery resumes without loss or
  duplicates).
- **Polling** (`appropriate-polling`, ADR-0011): `sensor.py` declares
  `SCAN_INTERVAL = 30 s` for the five in-memory health sensors and the table-size
  sensor refreshes on its own 5-minute timer. Verified in a live HA 2026.7.2
  instance: health sensors update every 30.0 s (recorder timestamps) and the
  table-size entity at exactly 300 s intervals, with the value matching a direct
  `SELECT sum(diskSize) FROM table_partitions(...)`.
- **Diagnostics** (`diagnostics`): safe configuration (password redacted via
  `async_redact_data`), options, listener and worker snapshots (state, counters,
  ring of the last 10 delivery/schema errors), spool/dead-letter stats, table TTL
  and the QuestDB build — downloadable from the UI.
- **Test coverage** (`test-coverage`): unit and integration tests run inside the
  dev-stack Home Assistant container; the integration tests boot their own Home
  Assistant and write to a real QuestDB. Measured with `coverage 7.15`
  (`python3 -m coverage run --concurrency=thread --source=custom_components.hass_questdb_writer -m pytest tests/unit tests/integration`):
  **99.1% total**, every module above 95% (lowest config_flow.py 96.9%, spool.py
  98.8%, worker.py 99.1%). The uncovered lines that remain are defensive branches
  such as "worker task exited before stop was requested".
- **Reauthentication** (`reauthentication-flow`, ADR-0012): a rejected credential
  set now starts a Home Assistant reauth dialog from the runtime (once per
  auth-blocked outage) that validates and stores new credentials, reloads the
  entry and aborts with `reauth_successful`. Covered by config-flow unit tests,
  runtime tests and an end-to-end integration test that opens the flow on a real
  entry and completes it against real QuestDB.
- **Availability logging** (`log-when-unavailable`): the table-size sensor logs one
  warning when it goes unavailable (including the transport error) and one info
  line when QuestDB answers again, on transitions only.
- **Parallel updates** (`parallel-updates`): `PARALLEL_UPDATES = 1` in `sensor.py`,
  matching the single HTTP-querying entity.
- **Documentation** (`docs-*` rules marked done): README covers installation,
  setup, every option with defaults and ranges, the health sensors with a watchdog
  automation, outage behaviour, verified example queries (`LATEST ON`,
  `SAMPLE BY` + `CAST(state AS DOUBLE)`, volume via `table_partitions`), reading
  data inside HA with the SQL integration over PGWire, troubleshooting
  (plus `docs/troubleshooting.md`), the data model and the architecture.
- **Entity naming** (`has-entity-name`): both sensor classes set
  `_attr_has_entity_name = True` through the shared base in `entity.py`. A
  throwaway HA 2026.7.2 instance set up through the real config flow reports
  `has_entity_name=True` for all six entities and renders "HASS QuestDB Writer
  State", "HASS QuestDB Writer Table size", and so on; installs that already have
  the entities keep their entity IDs.
- **Setup retry** (`test-before-setup`): worker start failures
  (`WorkerStartError`, `WorkerStartTimeoutError`) are mapped to
  `ConfigEntryNotReady`, so Home Assistant retries the setup instead of leaving the
  entry in a setup error. The runtime stops the half-started service first, and any
  other exception stays fatal.
- **Shared entity module** (`common-modules`): the device registry entry, the
  unique id and the naming contract are defined once in `entity.py`. There is no
  coordinator because the integration pushes data out rather than fetching it.
- **Removal** (`docs-removal-instructions`): README "## Removal" lists the three
  steps and states what a removal leaves behind — the QuestDB table with its
  history and the SQLite spool under `.storage/hass_questdb_writer/`.

## Keeping this honest

- Change rule statuses in `quality_scale.yaml`, never in this prose. A rule that
  is absent from the YAML counts as `todo`.
- hassfest only validates `quality_scale.yaml` for integrations inside core, so
  here the file is a self-check: it must be complete and accurate before any core
  submission, and it should be reviewed whenever a release is cut.
- When a rule is closed, record the evidence (test name, ADR, or measured
  behaviour) in its `comment`, the same way the current `done` entries do.

## Release prerequisites (outside the tiers)

- [x] Attribute allow/deny filter with `*`/`?` wildcards, UI-only (ADR-0008).
- [ ] Production defaults replacing the `PROVISIONAL_*` values, after
      production-rate spool/ILP benchmarks and filesystem tests.
- [ ] Honest semantic version in `manifest.json` and release tags for HACS.
- [ ] `hacs.json` + `LICENSE` + README verified against the HACS checklist.

## Explicit non-goals

- Replacing Home Assistant Recorder.
- End-to-end exactly-once delivery (server-side dedup only).
- YAML-based configuration (config entry + options flow only, per ADR-0007).

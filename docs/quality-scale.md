# Quality scale target: Gold

Target: [Home Assistant integration quality scale — Gold](https://www.home-assistant.io/docs/quality_scale/).

Gold is the release bar for the first public version. Every tier below Gold
must be complete before publication (Bronze → Silver → Gold).

## Bronze — baseline (done)

- [x] Can be easily set up through the UI (config flow + options flow).
- [x] Source code adheres to basic coding standards (ruff-clean, HA-style).
- [x] Automated tests guard the integration (unit + integration, run inside
      the dev Home Assistant container).
- [x] Basic end-user documentation (README: install, setup, options).

## Silver

- [x] **Auth-failure visibility**: on `AuthenticationIlpError` the worker
      goes `BLOCKED` (snapshot `block_reason="auth"`) and the runtime
      raises a persistent ERROR repair issue («QuestDB rejected the
      credentials») that disappears once the worker recovers or the entry
      unloads; delivery resumes automatically after the options are
      corrected (reload).
- [ ] **Code owners**: non-empty `codeowners` in `manifest.json`
      (add the maintainer's GitHub handle at publication time).
- [x] **Recovery without log spam**: retries log rate-limited warnings
      (powers of two: 1, 2, 4, … attempts) saying how long the backoff
      waits and that events stay in the spool; verified against a live
      QuestDB outage in the dev stack (spool grows, delivery resumes
      without loss or duplicates).
- [x] **Troubleshooting documentation**: `docs/troubleshooting.md`
      (symptoms → causes → fixes: auth, unreachable host, schema mismatch,
      spool/dead-letter limits, TTL), linked from the README.

## Gold

- [x] **Diagnostics**: diagnostics flow (`diagnostics.py`) exposing safe
      configuration (password redacted via `async_redact_data`), options,
      listener + worker snapshot (state, counters, ring of the last 10
      delivery/schema errors), spool/dead-letter stats, table TTL and the
      QuestDB build — downloadable from the UI (⋯ → Download diagnostics).
- [x] Reconfiguration via the UI (options flow with reload on save).
- [x] **Full automated test coverage**: coverage audit run (unit +
      integration in the dev HA container): **95% total** — 6 modules at
      100% (attribute_filter, const, diagnostics, ilp, schema, transport),
      event/runtime 99%, config_flow 97%, worker 92%, spool 88%. Known
      gaps are deep edge branches (dead-letter eviction paths, WAL
      recovery, shutdown flush, one-shot stop-from-worker guard).
- [x] **End-user documentation**: README covers use cases, health sensors
      with a watchdog automation, and verified example queries
      (`LATEST ON`, `SAMPLE BY` + `CAST(state AS DOUBLE)`, volume via
      `table_partitions`), plus reading data inside HA with the SQL
      integration over PGWire (aliased aggregate columns, PGWire
      defaults, freezing-sensor caveat) and a note about the sample
      Grafana dashboards in the dev-stack repository.
- [x] **Examples**: sample Grafana dashboards (dev stack) and a watchdog
      automation in the README.

## Release prerequisites (outside the tiers)

- [x] Attribute allow/deny filter with `*`/`?` wildcards, UI-only
      (ADR-0008).
- [ ] Production defaults replacing the `PROVISIONAL_*` values, after
      production-rate spool/ILP benchmarks and filesystem tests.
- [ ] Honest semantic version in `manifest.json` (`0.1.0-dev0` →
      `0.1.0`) and release tags for HACS.
- [ ] `hacs.json` + `LICENSE` + README verified against the HACS
      checklist.

## Explicit non-goals

- Replacing Home Assistant Recorder.
- End-to-end exactly-once delivery (server-side dedup only).
- YAML-based configuration (config entry + options flow only, per ADR-0007).

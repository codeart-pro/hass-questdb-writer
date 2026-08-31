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

- [ ] **Diagnostics**: diagnostics flow exposing safe configuration
      (secrets redacted), worker snapshot (state, counters), spool stats,
      and the most recent errors — downloadable from the UI.
- [x] Reconfiguration via the UI (options flow with reload on save).
- [ ] **Full automated test coverage**: run a coverage audit; close the gaps
      (config flow branches, worker state transitions, transport error
      classes, schema validation paths, diagnostics).
- [ ] **End-user documentation**: README expanded with use cases, example
      QuestDB queries (`SAMPLE BY`, `LATEST ON`, casts), example
      automations, and links to the sample Grafana dashboards (from the
      devstack repo).
- [ ] **Examples**: usable Grafana dashboards and one or two example
      automations published as part of the docs.

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

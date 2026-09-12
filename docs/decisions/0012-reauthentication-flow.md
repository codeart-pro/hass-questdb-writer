# ADR 0012: Reauthentication flow for rejected credentials

Status: accepted.

Date: 2026-09-12

## Context

ADR-0010 made a credential rejection visible: the worker goes `BLOCKED` with
`block_reason="auth"` and the runtime raises a persistent, non-fixable repair
issue. Fixing it meant opening the entry and using **Reconfigure**.

The Silver rule `reauthentication-flow` expects the repair to be offered directly:
"To avoid that the user has to remove the configuration entry and re-add it, we
start a reauthentication flow. During this flow, the user can provide the new
credentials to use from now on." The rule exempts only integrations without any
form of authentication, and this one supports HTTP Basic credentials.

## Decision

- `HassQuestDbWriterConfigFlow` implements `async_step_reauth` and
  `async_step_reauth_confirm`. They share the connection form and its validation
  with the user and reconfigure steps through `_async_connection_step`: the form is
  prefilled from the entry with an empty password, empty credentials keep the
  stored ones, and a successful run ends with
  `async_update_reload_and_abort(entry, data=..., reason="reauth_successful")`.
- The runtime takes an optional `request_reauth` callback and calls it **once per
  auth-blocked outage** (the guard resets when the worker leaves the blocked
  state), so the 30 s monitor loop cannot queue a second flow. `__init__.py` wires
  it to `entry.async_start_reauth(hass)`.
- The ADR-0010 repair issue stays: it is the persistent, visible record of the
  outage, while the reauth flow is the actionable path.

## Why not raise ConfigEntryAuthFailed

That is the coordinator pattern: raise during a data update and Home Assistant
starts the reauth flow itself. This integration writes data out and learns about
the 401 inside a background worker thread, long after setup finished, so the
runtime has to ask for the flow explicitly.

## Alternatives considered

- **Fixable repair issue with `async_create_fix_flow`**: rejected — the repairs
  framework would need its own connection form and validation, duplicating what the
  reauth flow already does with the config flow's `_test_connection`.
- **Reconfigure only** (status quo of ADR-0010): rejected — the rule exists because
  users do not find Reconfigure when their password changed.
- **Starting the flow from every monitor tick** while blocked: rejected — Home
  Assistant already skips a request while a flow runs, but the guard keeps the
  intent explicit and testable.

## Consequences

- A rejected credential set now surfaces as a dialog that asks for the new password
  and reloads the entry; the user no longer has to know that Reconfigure exists.
  Delivery resumes after the credentials are corrected, as before.
- The reauth flow does not touch `unique_id` (it encodes host, port and table):
  it is meant to fix credentials only. Changing the destination remains the
  Reconfigure flow's job.
- Home Assistant ignores the request when a reauth or reconfigure flow for the
  entry is already running (`ConfigEntry.async_start_reauth`).

## Verification

- Config-flow unit tests: the reauth step prefills the stored connection with an
  empty password, an empty password keeps the stored secret, wrong credentials
  return `invalid_auth`, and a successful run updates the entry with the
  `reauth_successful` reason.
- Runtime unit tests: reauth is requested once per auth-blocked outage, the guard
  resets after recovery, and nothing happens without a callback or an entry id.
- Not covered end to end: the dev QuestDB runs without authentication, so a real
  401 at delivery time cannot be produced in the local stack. The trigger path is
  covered by unit tests, the flow by the config-flow tests, and the wiring from
  `async_setup_entry` by the existing setup tests.

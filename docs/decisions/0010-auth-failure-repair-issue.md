# ADR 0010: Repair issue on QuestDB credential rejection

Status: accepted.

Partially superseded by [ADR 0012](0012-reauthentication-flow.md): a rejected
credential set now also starts a reauthentication flow. The repair issue described
here stays as the persistent, visible record of the outage.

Date: 2026-08-29

## Context

Silver-tier quality scale requires that authentication failures are
visible and recoverable. The worker already transitions to `BLOCKED` on
`AuthenticationIlpError`, but nothing told the user why, and the failure
looked like a silent outage.

## Decision

- The worker snapshot gains `block_reason` (`"auth"` for credential
  rejections, `None` otherwise), set by `_set_delivery_state`.
- The runtime monitors the worker state once per 30 s and reflects it in
  the HA issue registry:
  - `BLOCKED` + `block_reason == "auth"` →
    `ir.async_create_issue(... "auth_failed", is_persistent=True,
    severity=ERROR, translation_placeholders={host})`;
  - any other state → `ir.async_delete_issue(...)`.
- The issue is also synced once at startup and deleted on entry unload.
- The monitor runs as a **background task**
  (`hass.async_create_background_task`).

## Why a background task, not a regular hass task

`HomeAssistant.async_block_till_done()` waits for every task in
`hass._tasks`. A long-lived `hass.async_create_task` loop therefore makes
every `async_block_till_done()` (including the E2E tests and shutdown
stages) hang forever. Background tasks are excluded from that wait and
are auto-cancelled on shutdown. Verified by an E2E hang that disappeared
when switching.

## Consequences

- Credential problems are surfaced in the HA UI as a repair issue and
  disappear automatically once the credentials are fixed (reload) or the
  entry is removed.
- `is_fixable=False`: fixing means editing the entry (Reconfigure), which
  is outside the repairs framework.

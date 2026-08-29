# ADR 0008: Attribute allow/deny filter with wildcards

Status: accepted.

Date: 2026-08-29

## Context

ADR-0005 stores `attributes` as a VARCHAR holding the full Home Assistant
JSON serialization and explicitly left an *attribute allow-list* as a future
option. Real attribute payloads are noisy: Zigbee/device diagnostics add
`rssi`, `linkquality`, battery reports and similar keys that are useless for
time-series analysis and inflate every row. Users also may want to drop
sensitive keys before they leave the machine.

Two design questions were settled first:

- **UI vs YAML.** The Home Assistant quality scale treats YAML-only
  configuration as the *Legacy* tier, and the official guidance for
  config-entry integrations is UI-first. ADR-0007 already commits the
  integration to UI-driven configuration. A custom YAML file would create a
  second configuration channel with priority rules, its own validation and
  reload semantics, for no tier credit. The filter is therefore configured
  in the options flow, like the entity filter.
- **Allow-list vs deny-list vs wildcards.** Doing this in one pass (before
  the first release) costs roughly the same as a plain allow-list (the
  matcher is small; the extra cost is the test matrix). A deny list is
  genuinely useful for stripping noisy or sensitive keys without
  enumerating everything else, and wildcards make both lists practical
  (`update.*`, `rssi`).

## Decision

Add an attribute filter to the options flow, applied in the listener before
the attributes are serialized:

- **Allow list** (`attribute_allowlist`): comma-separated patterns. Empty
  means "write everything"; non-empty is a strict allow-list — only names
  matching at least one pattern are written.
- **Deny list** (`attribute_denylist`): comma-separated patterns. Matching
  names are removed; **deny always wins** over allow. This mirrors the
  entity include/exclude semantics users already know.
- **Wildcards**: `*` and `?`, matched with `fnmatchcase` (case-sensitive,
  like HA attribute names). No regular expressions.
- Both lists are stored in `entry.options`; changing them reloads the entry
  through the existing options-flow reload path. No migration is needed:
  entries without the keys fall back to empty lists (current behaviour).
- The event payload format is unchanged (`EVENT_PAYLOAD_VERSION` stays 2);
  only the attribute payload is filtered before `json_dumps`.

The runtime keeps a `attribute_entries_removed` counter in the snapshot so
the filter's effect is observable.

## Consequences

- Attribute payloads shrink (less spool/QuestDB space, `max_event_bytes` is
  easier to satisfy), at the cost of losing unfiltered attribute history:
  the filter is destructive and cannot be undone from QuestDB data.
- `friendly_name`/`unit_of_measurement` are regular attributes: users who
  want them in Grafana legend must include them explicitly.
- `fnmatchcase` interprets `[`/`]` as character classes; attribute names
  containing brackets are impractical to filter (accepted, consistent with
  the HA entity-filter behaviour).
- The default (both lists empty) preserves the exact ADR-0005 behaviour.

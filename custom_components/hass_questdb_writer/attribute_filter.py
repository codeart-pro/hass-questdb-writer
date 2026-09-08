"""Attribute allow/deny filter with fnmatch wildcards."""

from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatchcase


@dataclass(frozen=True, slots=True)
class AttributeFilter:
    """Decide which attribute names are written to the event payload.

    Semantics mirror the entity include/exclude filter (ADR-0008):

    - an empty allow list lets every name pass;
    - a non-empty allow list is a strict allow-list: only names matching
      at least one pattern pass;
    - names matching any deny pattern are removed, and deny always wins;
    - patterns support fnmatch wildcards ``*`` and ``?`` and are matched
      case-sensitively (``fnmatchcase``), like Home Assistant attribute
      names.
    """

    allow: tuple[str, ...] = ()
    deny: tuple[str, ...] = ()

    def __call__(self, name: str) -> bool:
        """Return True when the attribute ``name`` should be written."""
        if self.deny and any(fnmatchcase(name, pattern) for pattern in self.deny):
            return False
        if not self.allow:
            return True
        return any(fnmatchcase(name, pattern) for pattern in self.allow)


def parse_attribute_patterns(value: str | None) -> tuple[str, ...]:
    """Parse a comma-separated attribute pattern list from the UI.

    Blank entries and surrounding whitespace are removed; duplicates are
    dropped while preserving first-seen order.
    """
    if not value:
        return ()
    seen: dict[str, None] = {}
    for part in value.split(","):
        pattern = part.strip()
        if pattern:
            seen.setdefault(pattern, None)
    return tuple(seen)

"""Formatting helpers layered over the span-anchored marks in :mod:`rga`.

Marks live in the RGA itself because resolving them needs document order. What
lives here is the vocabulary around them: which keys exist, which are toggles
versus values, and how a set of marks turns into the inline syntax used by the
Markdown and HTML exporters.
"""

from __future__ import annotations

from collections.abc import Mapping

from syncpad.crdt.ops import MARK_KEYS, MarkOp
from syncpad.crdt.rga import Span

__all__ = ["MARK_KEYS", "TOGGLE_KEYS", "VALUE_KEYS", "MarkOp", "Span", "is_toggle", "marks_equal"]

#: Marks that are simply on or off.
TOGGLE_KEYS = frozenset({"bold", "italic", "code", "strike"})

#: Marks carrying a payload — a URL, a heading level.
VALUE_KEYS = frozenset({"link", "heading"})


def is_toggle(key: str) -> bool:
    return key in TOGGLE_KEYS


def marks_equal(a: Mapping[str, str | bool], b: Mapping[str, str | bool]) -> bool:
    """Compare two mark sets, ignoring keys explicitly set to a falsey toggle."""
    return {k: v for k, v in a.items() if v is not False} == {
        k: v for k, v in b.items() if v is not False
    }

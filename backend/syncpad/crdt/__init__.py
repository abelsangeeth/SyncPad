"""The CRDT layer: pure, synchronous, and free of any I/O or framework import.

Everything above this package — storage, the WebSocket gateway, the REST API and
the browser client — depends on these names. They are the project's most stable
interface, so they are re-exported here rather than reached for by module path.
"""

from __future__ import annotations

from syncpad.crdt.compaction import compact_ops, compaction_ratio, gc_tombstones
from syncpad.crdt.ids import (
    LamportClock,
    OpId,
    VersionVector,
    new_site_id,
    vv_covers,
    vv_merge,
    vv_min,
)
from syncpad.crdt.marks import TOGGLE_KEYS, VALUE_KEYS, is_toggle
from syncpad.crdt.ops import (
    MARK_KEYS,
    MAX_INSERT_RUN,
    DeleteOp,
    InsertOp,
    MarkOp,
    Op,
    OpDecodeError,
    op_from_dict,
    op_to_dict,
    ops_from_dicts,
    ops_to_dicts,
)
from syncpad.crdt.rga import RGA, SNAPSHOT_FORMAT, Element, Span

__all__ = [
    "MARK_KEYS",
    "MAX_INSERT_RUN",
    "RGA",
    "SNAPSHOT_FORMAT",
    "TOGGLE_KEYS",
    "VALUE_KEYS",
    "DeleteOp",
    "Element",
    "InsertOp",
    "LamportClock",
    "MarkOp",
    "Op",
    "OpDecodeError",
    "OpId",
    "Span",
    "VersionVector",
    "compact_ops",
    "compaction_ratio",
    "gc_tombstones",
    "is_toggle",
    "new_site_id",
    "op_from_dict",
    "op_to_dict",
    "ops_from_dicts",
    "ops_to_dicts",
    "vv_covers",
    "vv_merge",
    "vv_min",
]

"""The three operations a SyncPad document is made of, and their wire codec.

Operations are immutable value objects. They are the *only* thing that travels
between replicas — the document state itself is never shipped except as a
snapshot optimisation — so their encoding is a compatibility surface. Encoded
keys are deliberately terse (``k``, ``tx``, ``tg``) because a busy document with
fifty editors sends thousands of these per second.

Every operation is idempotent by construction: applying it twice is identical to
applying it once, because it names *what* it affects by id rather than *where* it
applies by index. That is what lets the gateway be careless about duplicate
delivery, and it is why the Redis fan-out needs no exactly-once guarantee.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, TypeAlias

from syncpad.crdt.ids import OpId

__all__ = [
    "MARK_KEYS",
    "MAX_INSERT_RUN",
    "DeleteOp",
    "InsertOp",
    "MarkOp",
    "Op",
    "OpDecodeError",
    "op_from_dict",
    "op_to_dict",
    "ops_from_dicts",
    "ops_to_dicts",
]

#: Longest run of text a single InsertOp may carry. Bounds both frame size and
#: the work one apply() can do, which matters for the per-connection rate limit.
MAX_INSERT_RUN = 512

#: Formatting keys the editor understands. Unknown keys are rejected at the wire
#: boundary so a buggy client cannot poison a document with unrenderable marks.
MARK_KEYS = frozenset({"bold", "italic", "code", "strike", "link", "heading"})


class OpDecodeError(ValueError):
    """Raised when an encoded operation is malformed or hostile.

    Always raised from the wire boundary rather than letting a ``KeyError`` or
    ``TypeError`` escape, so the gateway can answer with a clean ``bad_frame``
    instead of a 500.
    """


@dataclass(frozen=True, slots=True)
class InsertOp:
    """Insert ``text`` immediately after the element ``after``.

    One op carries a whole run: character ``i`` of ``text`` takes the id
    ``id.offset(i)``, and its left origin is the preceding character of the same
    run. Typing a word is therefore one op rather than five, which is the single
    biggest constant-factor win in the whole pipeline.

    ``after is None`` means "at the very start of the document".
    """

    kind: Literal["ins"]
    id: OpId
    after: OpId | None
    text: str


@dataclass(frozen=True, slots=True)
class DeleteOp:
    """Tombstone every element named in ``targets``.

    Deletion never removes anything: it flips a bit. A concurrent insert that
    used a deleted character as its left origin still has somewhere to attach,
    which is exactly why this converges where an index-based delete would not.
    Physical removal is a separate, quorum-gated step — see
    :func:`syncpad.crdt.compaction.gc_tombstones`.
    """

    kind: Literal["del"]
    id: OpId
    targets: tuple[OpId, ...]


@dataclass(frozen=True, slots=True)
class MarkOp:
    """Apply (or with ``value is None`` remove) a formatting mark over a span.

    The span is anchored to character *ids*, not offsets, so it keeps meaning as
    the text around and inside it changes. With ``expand`` set, text typed inside
    the span inherits the mark — which is what makes typing in the middle of a
    bold word stay bold without the client sending a second op.
    """

    kind: Literal["mark"]
    id: OpId
    start: OpId
    end: OpId
    key: str
    value: str | bool | None
    expand: bool = True


Op: TypeAlias = InsertOp | DeleteOp | MarkOp


def op_to_dict(op: Op) -> dict[str, object]:
    """Encode an operation to its compact wire form."""
    if isinstance(op, InsertOp):
        return {
            "k": "ins",
            "id": op.id.encode(),
            "after": op.after.encode() if op.after is not None else None,
            "tx": op.text,
        }
    if isinstance(op, DeleteOp):
        return {
            "k": "del",
            "id": op.id.encode(),
            "tg": [t.encode() for t in op.targets],
        }
    if isinstance(op, MarkOp):
        return {
            "k": "mark",
            "id": op.id.encode(),
            "s": op.start.encode(),
            "e": op.end.encode(),
            "key": op.key,
            "v": op.value,
            "x": op.expand,
        }
    raise TypeError(f"not an operation: {type(op).__name__}")


def _req_id(d: Mapping[str, object], key: str) -> OpId:
    raw = d.get(key)
    if not isinstance(raw, str):
        raise OpDecodeError(f"field {key!r} must be an OpId string")
    try:
        return OpId.decode(raw)
    except ValueError as exc:
        raise OpDecodeError(str(exc)) from exc


def op_from_dict(d: Mapping[str, object]) -> Op:
    """Decode an operation, rejecting anything malformed or oversized.

    This is a trust boundary: ``d`` came off a WebSocket frame written by a
    client we do not control. Every field is validated, and the only exception
    type that escapes is :class:`OpDecodeError`.
    """
    if not isinstance(d, Mapping):
        raise OpDecodeError("operation must be an object")
    kind = d.get("k")

    if kind == "ins":
        text = d.get("tx")
        if not isinstance(text, str):
            raise OpDecodeError("insert 'tx' must be a string")
        if not text:
            raise OpDecodeError("insert 'tx' must be non-empty")
        if len(text) > MAX_INSERT_RUN:
            raise OpDecodeError(f"insert run of {len(text)} exceeds {MAX_INSERT_RUN}")
        after_raw = d.get("after")
        if after_raw is None:
            after: OpId | None = None
        elif isinstance(after_raw, str):
            try:
                after = OpId.decode(after_raw)
            except ValueError as exc:
                raise OpDecodeError(str(exc)) from exc
        else:
            raise OpDecodeError("insert 'after' must be an OpId string or null")
        return InsertOp(kind="ins", id=_req_id(d, "id"), after=after, text=text)

    if kind == "del":
        raw_targets = d.get("tg")
        if not isinstance(raw_targets, (list, tuple)):
            raise OpDecodeError("delete 'tg' must be a list")
        if not raw_targets:
            raise OpDecodeError("delete 'tg' must be non-empty")
        if len(raw_targets) > MAX_INSERT_RUN:
            raise OpDecodeError(f"delete of {len(raw_targets)} exceeds {MAX_INSERT_RUN}")
        targets: list[OpId] = []
        for t in raw_targets:
            if not isinstance(t, str):
                raise OpDecodeError("delete targets must be OpId strings")
            try:
                targets.append(OpId.decode(t))
            except ValueError as exc:
                raise OpDecodeError(str(exc)) from exc
        return DeleteOp(kind="del", id=_req_id(d, "id"), targets=tuple(targets))

    if kind == "mark":
        key = d.get("key")
        if not isinstance(key, str) or key not in MARK_KEYS:
            raise OpDecodeError(f"mark 'key' must be one of {sorted(MARK_KEYS)}")
        value = d.get("v")
        if value is not None and not isinstance(value, (str, bool)):
            raise OpDecodeError("mark 'v' must be a string, bool, or null")
        if isinstance(value, str) and len(value) > 2048:
            raise OpDecodeError("mark 'v' string too long")
        expand = d.get("x", True)
        if not isinstance(expand, bool):
            raise OpDecodeError("mark 'x' must be a bool")
        return MarkOp(
            kind="mark",
            id=_req_id(d, "id"),
            start=_req_id(d, "s"),
            end=_req_id(d, "e"),
            key=key,
            value=value,
            expand=expand,
        )

    raise OpDecodeError(f"unknown operation kind {kind!r}")


def ops_to_dicts(ops: list[Op] | tuple[Op, ...]) -> list[dict[str, object]]:
    return [op_to_dict(o) for o in ops]


def ops_from_dicts(raw: object) -> list[Op]:
    """Decode a list of operations, as received in one wire frame."""
    if not isinstance(raw, (list, tuple)):
        raise OpDecodeError("'ops' must be a list")
    return [op_from_dict(d) for d in raw]  # type: ignore[arg-type]

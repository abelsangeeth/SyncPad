"""The Replicated Growable Array — SyncPad's conflict resolution, in one class.

Fifty people typing into one paragraph is not a locking problem, it is an
ordering problem. Operational transformation solves it by rewriting operations
against each other, which requires a central server to fix an order and gets
combinatorially nasty at three or more concurrent sites. An RGA solves it by
never needing an order in the first place: every character carries an immutable
identity, and the rule for where a character belongs depends only on identities
the replica already holds.

The integration rule is three lines long:

    to insert element ``e`` after origin ``p``,
    scan right from ``p``, skipping every element whose id sorts above ``e``'s,
    and place ``e`` before the first element whose id sorts below.

It converges because of a single invariant maintained in :mod:`syncpad.crdt.ids`:
a character's id always sorts above its left origin's. So if the scan meets an
element ``x`` with ``x.id > e.id``, every character ``x`` anchors — its whole
subtree — also sorts above ``e`` and must also be skipped; and the first element
sorting below ``e`` cannot belong to that subtree. The scan therefore steps over
whole subtrees atomically without ever having to identify them, and every
replica walks the same elements to the same conclusion regardless of the order
the operations arrived in.

Deletion never removes: it tombstones. A concurrent insert whose left origin was
just deleted elsewhere still finds its anchor, which is precisely the case an
index-based delete gets wrong. Physical removal happens only under a quorum
guarantee — see :func:`syncpad.crdt.compaction.gc_tombstones`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Final

import orjson

from syncpad.crdt._order import OrderTreap, TreapNode
from syncpad.crdt.ids import LamportClock, OpId, VersionVector, new_site_id
from syncpad.crdt.ops import (
    MAX_INSERT_RUN,
    DeleteOp,
    InsertOp,
    MarkOp,
    Op,
    op_from_dict,
    op_to_dict,
)

__all__ = ["RGA", "SNAPSHOT_FORMAT", "Element", "Span"]

#: Bumped whenever the snapshot envelope changes shape. ``load`` refuses blobs
#: from the future rather than silently misreading them.
SNAPSHOT_FORMAT: Final = 1

_NO_MARKS: Final[dict[str, str | bool]] = {}


@dataclass(slots=True)
class Element:
    """One character, and the identity that outlives it.

    ``parent`` is the left origin — the element this one was typed after. It is
    kept for the lifetime of the document because it is the anchor a late or
    concurrent insert will search for.
    """

    id: OpId
    ch: str
    parent: OpId | None
    deleted: bool = False


@dataclass(frozen=True, slots=True)
class Span:
    """A maximal run of text sharing identical formatting."""

    text: str
    marks: Mapping[str, str | bool]


class RGA:
    """A replicated sequence of characters with span-anchored formatting.

    Not thread-safe, and deliberately so: one document lives on one asyncio event
    loop inside one :class:`~syncpad.services.rooms.Room`, and concurrency
    between replicas is handled by the CRDT rather than by a mutex.
    """

    __slots__ = (
        "_clock",
        "_marks",
        "_nodes",
        "_pending_deletes",
        "_pending_inserts",
        "_tree",
        "_vv",
    )

    def __init__(self, site: str | None = None) -> None:
        self._clock = LamportClock(site or new_site_id())
        self._tree: OrderTreap[Element] = OrderTreap()
        self._nodes: dict[OpId, TreapNode[Element]] = {}
        # Deletes that named a character this replica has not received yet. The
        # op log is causally unordered, so this is routine, not an error.
        self._pending_deletes: set[OpId] = set()
        # Inserts whose left origin is still missing, keyed by the origin awaited.
        self._pending_inserts: dict[OpId, list[InsertOp]] = {}
        self._marks: dict[OpId, MarkOp] = {}
        self._vv: VersionVector = {}

    # ------------------------------------------------------------------ meta
    @property
    def site(self) -> str:
        return self._clock.site

    @property
    def clock(self) -> LamportClock:
        return self._clock

    @property
    def elements(self) -> int:
        """Total characters ever inserted and not yet garbage collected."""
        return len(self._tree)

    @property
    def tombstones(self) -> int:
        return len(self._tree) - self._tree.visible_count

    @property
    def pending(self) -> int:
        """Operations buffered awaiting causal dependencies."""
        return sum(len(v) for v in self._pending_inserts.values())

    def __len__(self) -> int:
        return self._tree.visible_count

    def version_vector(self) -> VersionVector:
        return dict(self._vv)

    # --------------------------------------------------------- local editing
    def local_insert(self, index: int, text: str) -> list[Op]:
        """Insert ``text`` at visible ``index``, returning the ops to broadcast."""
        if not text:
            return []
        visible = self._tree.visible_count
        if index < 0 or index > visible:
            raise IndexError(f"insert index {index} out of range 0..{visible}")

        after: OpId | None = None
        if index > 0:
            node = self._tree.select_visible(index - 1)
            # select_visible cannot return None here: index-1 < visible.
            assert node is not None
            after = node.value.id

        ops: list[Op] = []
        for chunk in _chunks(text, MAX_INSERT_RUN):
            op = InsertOp(kind="ins", id=self._clock.tick_run(len(chunk)), after=after, text=chunk)
            self.apply(op)
            ops.append(op)
            after = op.id.offset(len(chunk) - 1)
        return ops

    def local_delete(self, index: int, length: int) -> list[Op]:
        """Tombstone ``length`` visible characters from ``index``."""
        if length <= 0:
            return []
        visible = self._tree.visible_count
        if index < 0 or index + length > visible:
            raise IndexError(f"delete range {index}..{index + length} out of range 0..{visible}")

        targets: list[OpId] = []
        node = self._tree.select_visible(index)
        while node is not None and len(targets) < length:
            if not node.value.deleted:
                targets.append(node.value.id)
            node = self._tree.successor(node)
        if not targets:
            return []

        ops: list[Op] = []
        for chunk in _chunks(targets, MAX_INSERT_RUN):
            op = DeleteOp(kind="del", id=self._clock.tick(), targets=tuple(chunk))
            self.apply(op)
            ops.append(op)
        return ops

    def local_mark(
        self,
        index: int,
        length: int,
        key: str,
        value: str | bool | None,
        expand: bool = True,
    ) -> list[Op]:
        """Format ``length`` visible characters from ``index``.

        ``value=None`` removes the mark. Marking an empty range is a no-op rather
        than an error: the editor calls this straight from a toolbar button, and
        a click with no selection should do nothing rather than raise.
        """
        if length <= 0:
            return []
        visible = self._tree.visible_count
        if index < 0 or index + length > visible:
            raise IndexError(f"mark range {index}..{index + length} out of range 0..{visible}")

        start_node = self._tree.select_visible(index)
        end_node = self._tree.select_visible(index + length - 1)
        if start_node is None or end_node is None:
            return []

        op = MarkOp(
            kind="mark",
            id=self._clock.tick(),
            start=start_node.value.id,
            end=end_node.value.id,
            key=key,
            value=value,
            expand=expand,
        )
        self.apply(op)
        return [op]

    # -------------------------------------------------------- remote editing
    def apply(self, op: Op) -> bool:
        """Apply a remote (or freshly generated local) operation.

        Returns True when the document changed. Returns False for a duplicate, and
        for an operation buffered awaiting a causal dependency — in both cases the
        caller has nothing new to render. Idempotent and commutative: the whole
        design rests on that, so it is asserted by property tests rather than
        assumed.
        """
        if isinstance(op, InsertOp):
            return self._apply_insert(op)
        if isinstance(op, DeleteOp):
            return self._apply_delete(op)
        if isinstance(op, MarkOp):
            return self._apply_mark(op)
        raise TypeError(f"not an operation: {type(op).__name__}")

    def apply_many(self, ops: Iterable[Op]) -> int:
        """Apply a batch, returning how many actually changed the document."""
        return sum(1 for op in ops if self.apply(op))

    def _observe(self, oid: OpId, span: int = 1) -> None:
        """Absorb a remote id into the clock and the version vector."""
        last = oid.counter + span - 1
        self._clock.observe(OpId(oid.site, last))
        if last > self._vv.get(oid.site, 0):
            self._vv[oid.site] = last

    def _apply_insert(self, op: InsertOp) -> bool:
        self._observe(op.id, len(op.text))

        if op.id in self._nodes:
            # Already integrated. A partially applied run is impossible: runs are
            # integrated atomically below.
            return False

        if op.after is not None and op.after not in self._nodes:
            waiters = self._pending_inserts.setdefault(op.after, [])
            if any(w.id == op.id for w in waiters):
                return False
            waiters.append(op)
            return False

        self._integrate_run(op)
        self._drain(op)
        return True

    def _integrate_run(self, op: InsertOp) -> None:
        """Integrate every character of a run, each anchored to the last."""
        parent_id = op.after
        parent_node = self._nodes[op.after] if op.after is not None else None
        for i, ch in enumerate(op.text):
            eid = op.id.offset(i)
            existing = self._nodes.get(eid)
            if existing is not None:
                parent_id, parent_node = eid, existing
                continue
            parent_node = self._integrate_one(eid, ch, parent_id, parent_node)
            parent_id = eid

    def _integrate_one(
        self,
        eid: OpId,
        ch: str,
        parent_id: OpId | None,
        parent_node: TreapNode[Element] | None,
    ) -> TreapNode[Element]:
        """The RGA integration rule. See this module's docstring for why it works."""
        ref = parent_node
        cur = self._tree.first() if parent_node is None else self._tree.successor(parent_node)
        while cur is not None and cur.value.id > eid:
            ref = cur
            cur = self._tree.successor(cur)

        deleted = eid in self._pending_deletes
        element = Element(id=eid, ch=ch, parent=parent_id, deleted=deleted)
        node = self._tree.insert_after(ref, element, site=eid.site, counter=eid.counter)
        self._nodes[eid] = node
        if deleted:
            self._tree.set_visible(node, False)
            self._pending_deletes.discard(eid)
        return node

    def _drain(self, op: InsertOp) -> None:
        """Integrate inserts that were waiting on characters this run supplied.

        Iterative rather than recursive: a long causal chain delivered backwards
        would otherwise overflow the stack, and a hostile client can construct
        exactly that.
        """
        frontier = [op.id.offset(i) for i in range(len(op.text))]
        while frontier:
            anchor = frontier.pop()
            waiters = self._pending_inserts.pop(anchor, None)
            if not waiters:
                continue
            for waiting in waiters:
                if waiting.id in self._nodes:
                    continue
                self._integrate_run(waiting)
                frontier.extend(waiting.id.offset(i) for i in range(len(waiting.text)))

    def _apply_delete(self, op: DeleteOp) -> bool:
        self._observe(op.id)
        changed = False
        for target in op.targets:
            node = self._nodes.get(target)
            if node is None:
                if target not in self._pending_deletes:
                    self._pending_deletes.add(target)
                    changed = True
                continue
            if not node.value.deleted:
                node.value.deleted = True
                self._tree.set_visible(node, False)
                changed = True
        return changed

    def _apply_mark(self, op: MarkOp) -> bool:
        self._observe(op.id)
        if op.id in self._marks:
            return False
        self._marks[op.id] = op
        return True

    # ------------------------------------------------------------- rendering
    def text(self) -> str:
        return "".join(n.value.ch for n in self._tree if n.value.deleted is False)

    def spans(self) -> list[Span]:
        """Visible text split into maximal runs of identical formatting.

        A sweep line over mark-interval events, so the cost is
        ``O(m log m + n)`` rather than the ``O(n·m)`` of asking each character
        which marks contain it.
        """
        events = self._mark_events()
        out: list[Span] = []
        active: dict[str, dict[OpId, MarkOp]] = {}
        nonexpand = 0
        ev_i = 0
        n_events = len(events)

        buf: list[str] = []
        cur_marks: Mapping[str, str | bool] = _NO_MARKS
        started = False
        dirty = True

        for abs_idx, node in enumerate(self._tree):
            while ev_i < n_events and events[ev_i][0] <= abs_idx:
                _, delta, mark = events[ev_i]
                ev_i += 1
                bucket = active.setdefault(mark.key, {})
                if delta > 0:
                    bucket[mark.id] = mark
                    if not mark.expand:
                        nonexpand += 1
                else:
                    if bucket.pop(mark.id, None) is not None and not mark.expand:
                        nonexpand -= 1
                    if not bucket:
                        del active[mark.key]
                dirty = True

            element = node.value
            if element.deleted:
                continue

            if not active:
                marks: Mapping[str, str | bool] = _NO_MARKS
            elif dirty or nonexpand:
                marks = self._resolve(active, element)
            else:
                marks = cur_marks
            dirty = False

            if not started:
                cur_marks = marks
                started = True
            elif marks != cur_marks:
                out.append(Span("".join(buf), cur_marks))
                buf = []
                cur_marks = marks
            buf.append(element.ch)

        if buf:
            out.append(Span("".join(buf), cur_marks))
        return out

    def _mark_events(self) -> list[tuple[int, int, MarkOp]]:
        """Interval open/close events in absolute (tombstone-inclusive) order."""
        events: list[tuple[int, int, MarkOp]] = []
        for mark in self._marks.values():
            start = self._nodes.get(mark.start)
            end = self._nodes.get(mark.end)
            if start is None or end is None:
                # Anchors not delivered yet; the mark activates when they arrive.
                continue
            lo = self._tree.rank(start)
            hi = self._tree.rank(end)
            if lo > hi:
                lo, hi = hi, lo
            events.append((lo, 1, mark))
            events.append((hi + 1, -1, mark))
        events.sort(key=lambda e: (e[0], e[1]))
        return events

    @staticmethod
    def _resolve(
        active: dict[str, dict[OpId, MarkOp]], element: Element
    ) -> Mapping[str, str | bool]:
        """Winning value per mark key for one character: greatest op id wins."""
        resolved: dict[str, str | bool] = {}
        for key, bucket in active.items():
            best: MarkOp | None = None
            for mark in bucket.values():
                # A non-expanding mark covers only text that already existed, in
                # id order, when the mark was made. Concurrently typed text sorts
                # above the mark and is therefore excluded.
                if not mark.expand and not element.id < mark.id:
                    continue
                if best is None or mark.id > best.id:
                    best = mark
            if best is not None and best.value is not None:
                resolved[key] = best.value
        return resolved

    def marks_at(self, index: int) -> dict[str, str | bool]:
        """Formatting in effect at visible ``index``."""
        node = self._tree.select_visible(index)
        if node is None:
            return {}
        pos = self._tree.rank(node)
        active: dict[str, dict[OpId, MarkOp]] = {}
        for mark in self._marks.values():
            start = self._nodes.get(mark.start)
            end = self._nodes.get(mark.end)
            if start is None or end is None:
                continue
            lo = self._tree.rank(start)
            hi = self._tree.rank(end)
            if lo > hi:
                lo, hi = hi, lo
            if lo <= pos <= hi:
                active.setdefault(mark.key, {})[mark.id] = mark
        return dict(self._resolve(active, node.value))

    # ------------------------------------------------------------- addressing
    def index_of(self, oid: OpId) -> int | None:
        """Visible index of a character, or None if unknown or tombstoned."""
        node = self._nodes.get(oid)
        if node is None or node.value.deleted:
            return None
        return self._tree.visible_rank(node)

    def anchor_index(self, oid: OpId) -> int | None:
        """Where a character is, or where it *would* be had it survived.

        Cursors and comment anchors point at characters that may since have been
        deleted by someone else. Returning None there would drop the cursor to
        the top of the document; returning the tombstone's position keeps it
        where the user left it.
        """
        node = self._nodes.get(oid)
        if node is None:
            return None
        return self._tree.visible_rank(node)

    def id_at(self, index: int) -> OpId | None:
        node = self._tree.select_visible(index)
        return node.value.id if node is not None else None

    def contains(self, oid: OpId) -> bool:
        return oid in self._nodes

    def is_deleted(self, oid: OpId) -> bool:
        node = self._nodes.get(oid)
        return node is not None and node.value.deleted

    def text_between(self, start: OpId, end: OpId) -> str:
        """Visible text spanned by two anchors, inclusive; "" if unresolvable.

        Used to re-quote a comment after the text around it has moved.
        """
        s = self._nodes.get(start)
        e = self._nodes.get(end)
        if s is None or e is None:
            return ""
        lo, hi = self._tree.rank(s), self._tree.rank(e)
        if lo > hi:
            lo, hi = hi, lo
        out: list[str] = []
        node = self._tree.node_at(lo)
        idx = lo
        while node is not None and idx <= hi:
            if not node.value.deleted:
                out.append(node.value.ch)
            node = self._tree.successor(node)
            idx += 1
        return "".join(out)

    # ----------------------------------------------------------- persistence
    def snapshot(self) -> bytes:
        """Serialise the full replica state, including causal buffers.

        Everything needed to resume as this exact replica: the sequence, the
        tombstones (including deletes for characters not yet seen), the marks,
        the version vector, and the unresolved insert buffer. Dropping any of
        them would make a snapshot-restored replica diverge from one that had
        been running all along — which is the bug this design most wants to
        avoid, because it would surface only under load.
        """
        elements = [
            [
                n.value.id.site,
                n.value.id.counter,
                n.value.ch,
                n.value.parent.encode() if n.value.parent is not None else None,
                n.value.deleted,
            ]
            for n in self._tree
        ]
        payload = {
            "v": SNAPSHOT_FORMAT,
            "elements": elements,
            "marks": [op_to_dict(m) for m in self._marks.values()],
            "pending_deletes": [o.encode() for o in sorted(self._pending_deletes)],
            "pending_inserts": [
                op_to_dict(o) for ops in self._pending_inserts.values() for o in ops
            ],
            "vv": self._vv,
        }
        return orjson.dumps(payload)

    @classmethod
    def load(cls, site: str | None, blob: bytes) -> RGA:
        """Rebuild a replica from a snapshot, adopting ``site`` as its identity."""
        data = orjson.loads(blob)
        version = data.get("v")
        if version != SNAPSHOT_FORMAT:
            raise ValueError(f"unsupported snapshot format {version!r}")

        rga = cls(site)
        nodes: list[TreapNode[Element]] = []
        for site_id, counter, ch, parent, deleted in data["elements"]:
            oid = OpId(site_id, counter)
            element = Element(
                id=oid,
                ch=ch,
                parent=OpId.decode(parent) if parent is not None else None,
                deleted=bool(deleted),
            )
            node: TreapNode[Element] = TreapNode(element, 0)
            node.visible = not element.deleted
            node.vis = 0 if element.deleted else 1
            rga._nodes[oid] = node
            nodes.append(node)
        # Bulk-balance rather than inserting one by one: O(n) instead of
        # O(n log n), which matters because this runs on every cold load.
        rga._tree.rebuild(nodes)

        for raw in data["marks"]:
            mark = op_from_dict(raw)
            assert isinstance(mark, MarkOp)
            rga._marks[mark.id] = mark
        rga._pending_deletes = {OpId.decode(s) for s in data.get("pending_deletes", ())}
        for raw in data.get("pending_inserts", ()):
            op = op_from_dict(raw)
            assert isinstance(op, InsertOp)
            if op.after is not None:
                rga._pending_inserts.setdefault(op.after, []).append(op)
        rga._vv = {str(k): int(v) for k, v in data.get("vv", {}).items()}

        # The clock must outrank everything in the snapshot, or this replica will
        # mint ids that sort below existing text and insert in the wrong place.
        highest = max(rga._vv.values(), default=0)
        for oid in rga._nodes:
            highest = max(highest, oid.counter)
        rga._clock = LamportClock(rga._clock.site, highest)
        return rga

    # ---------------------------------------------------------- internal use
    def _iter_nodes(self) -> Iterable[TreapNode[Element]]:
        return iter(self._tree)

    def _drop_nodes(self, doomed: set[OpId]) -> None:
        """Physically remove elements. Only :mod:`compaction` may call this."""
        survivors = [n for n in self._tree if n.value.id not in doomed]
        for oid in doomed:
            self._nodes.pop(oid, None)
        self._tree.rebuild(survivors)

    def check_invariants(self) -> None:
        """Assert internal consistency. Test-only; O(n)."""
        self._tree.check()
        assert len(self._nodes) == len(self._tree), "node map / tree size mismatch"
        for node in self._tree:
            assert self._nodes[node.value.id] is node, "node map points elsewhere"
            assert node.visible != node.value.deleted, "visibility out of sync"
        seen: set[OpId] = set()
        for node in self._tree:
            element = node.value
            if element.parent is not None and element.parent in self._nodes:
                assert element.parent in seen, "element precedes its own left origin"
                assert element.id > element.parent, "child id does not outrank parent"
            seen.add(element.id)


def _chunks(seq: str | list[OpId], size: int):
    """Split a run into wire-sized pieces."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]

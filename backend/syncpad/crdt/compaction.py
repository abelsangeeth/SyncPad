"""Two ways to stop a long-lived document from paying for its whole history.

An append-only op log is the right durability story and the wrong read story:
a document edited for a year replays a year of keystrokes on every cold load.
Snapshots (in :mod:`syncpad.storage.snapshots`) bound the *replay*; this module
bounds the *state* and the *log*.

:func:`compact_ops` rewrites a batch of operations into a smaller batch with an
identical effect — the typing of a word arriving as forty ops leaves as one.
:func:`gc_tombstones` physically deletes tombstones, which is only safe once
every live replica has acknowledged them, because a tombstone is the anchor a
concurrent insert may still be looking for.
"""

from __future__ import annotations

from collections.abc import Sequence

from syncpad.crdt.ids import OpId, VersionVector, vv_covers
from syncpad.crdt.ops import MAX_INSERT_RUN, DeleteOp, InsertOp, MarkOp, Op
from syncpad.crdt.rga import RGA

__all__ = ["compact_ops", "compaction_ratio", "gc_tombstones"]


def compact_ops(ops: Sequence[Op]) -> list[Op]:
    """Rewrite ``ops`` into a smaller sequence with exactly the same effect.

    Three reductions, all of them effect-preserving:

    * **Insert runs** — consecutive inserts from one site where each anchors to
      the end of the previous one are a single contiguous run of ids, so they
      merge into one op. This is the common case: it is what typing *is*.
    * **Deletes** — deletes from one site coalesce into one op per site, since
      a delete is an unordered set of tombstones and applying it twice or in
      pieces makes no difference.
    * **Marks** — a mark op is dropped when a later op on the same span and key
      overrides it completely, because only the greatest id can ever win.

    Relative order between sites is preserved, and no op is ever moved before an
    op it causally depends on: merging only ever joins ops that are already
    adjacent within their own site's subsequence.
    """
    if len(ops) <= 1:
        return list(ops)

    # --- pass 1: drop fully superseded marks ----------------------------
    best_mark: dict[tuple[str, str, str, bool], OpId] = {}
    for op in ops:
        if isinstance(op, MarkOp):
            key = (op.start.encode(), op.end.encode(), op.key, op.expand)
            current = best_mark.get(key)
            if current is None or op.id > current:
                best_mark[key] = op.id

    # --- pass 2: merge inserts, coalesce deletes -------------------------
    out: list[Op] = []
    delete_slots: dict[str, int] = {}
    delete_targets: dict[str, list[OpId]] = {}

    for op in ops:
        if isinstance(op, InsertOp):
            merged = False
            if out:
                prev = out[-1]
                if (
                    isinstance(prev, InsertOp)
                    and prev.id.site == op.id.site
                    and prev.id.counter + len(prev.text) == op.id.counter
                    and op.after == prev.id.offset(len(prev.text) - 1)
                    and len(prev.text) + len(op.text) <= MAX_INSERT_RUN
                ):
                    out[-1] = InsertOp(
                        kind="ins", id=prev.id, after=prev.after, text=prev.text + op.text
                    )
                    merged = True
            if not merged:
                out.append(op)

        elif isinstance(op, DeleteOp):
            site = op.id.site
            slot = delete_slots.get(site)
            if slot is None:
                delete_slots[site] = len(out)
                delete_targets[site] = list(op.targets)
                out.append(op)
            else:
                targets = delete_targets[site]
                if len(targets) + len(op.targets) <= MAX_INSERT_RUN:
                    targets.extend(op.targets)
                    head = out[slot]
                    assert isinstance(head, DeleteOp)
                    # Keep the lowest id so the op still sorts where it did.
                    out[slot] = DeleteOp(kind="del", id=head.id, targets=tuple(targets))
                else:
                    delete_slots[site] = len(out)
                    delete_targets[site] = list(op.targets)
                    out.append(op)

        else:
            assert isinstance(op, MarkOp)
            key = (op.start.encode(), op.end.encode(), op.key, op.expand)
            if best_mark.get(key) == op.id:
                out.append(op)

    return _dedupe_delete_targets(out)


def _dedupe_delete_targets(ops: list[Op]) -> list[Op]:
    """Drop repeated tombstone targets left behind by coalescing."""
    result: list[Op] = []
    for op in ops:
        if isinstance(op, DeleteOp):
            seen: set[OpId] = set()
            unique = tuple(t for t in op.targets if not (t in seen or seen.add(t)))
            result.append(
                op
                if len(unique) == len(op.targets)
                else DeleteOp(kind="del", id=op.id, targets=unique)
            )
        else:
            result.append(op)
    return result


def gc_tombstones(rga: RGA, stable: VersionVector) -> int:
    """Physically remove tombstones every replica in ``stable`` has observed.

    ``stable`` must be the *pointwise minimum* version vector across every live
    replica — see :func:`syncpad.crdt.ids.vv_min`. Passing anything larger is a
    correctness bug, not a tuning choice: dropping a tombstone a replica has not
    seen destroys the anchor its in-flight insert is searching for, and that text
    would silently land in the wrong place or vanish.

    A tombstone is only dropped when the characters on *both* sides of it are
    also stable, so no surviving element is left pointing at a parent that no
    longer exists.

    Returns the number of characters reclaimed. ``rga.text()`` is unchanged by
    construction — only invisible elements are eligible.
    """
    doomed: set[OpId] = set()
    for node in rga._iter_nodes():
        element = node.value
        if not element.deleted:
            continue
        if not vv_covers(stable, element.id):
            continue
        doomed.add(element.id)

    if not doomed:
        return 0

    # A surviving element must keep a resolvable left origin, so never drop a
    # tombstone that a survivor (or a retained tombstone) still anchors to.
    anchored: set[OpId] = set()
    for node in rga._iter_nodes():
        parent = node.value.parent
        if parent is not None and node.value.id not in doomed:
            anchored.add(parent)
    doomed -= anchored

    # Likewise for marks: an anchor that disappears takes its formatting with it.
    for mark in rga._marks.values():
        doomed.discard(mark.start)
        doomed.discard(mark.end)

    if not doomed:
        return 0
    rga._drop_nodes(doomed)
    return len(doomed)


def compaction_ratio(before: Sequence[Op], after: Sequence[Op]) -> float:
    """Fraction of operations eliminated, for metrics and the bench report."""
    if not before:
        return 0.0
    return 1.0 - (len(after) / len(before))

"""Compaction must be invisible: smaller logs, byte-identical documents.

Both functions here trade history for space, so both are tested the same way —
apply the original operations to one replica and the compacted ones to another,
then assert the two replicas are indistinguishable. Anything less would let a
space optimisation quietly corrupt documents that are only read months later.
"""

from __future__ import annotations

import random

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from syncpad.crdt import (
    RGA,
    DeleteOp,
    InsertOp,
    Op,
    OpId,
    compact_ops,
    compaction_ratio,
    gc_tombstones,
    vv_min,
)


def _spans_key(doc: RGA) -> tuple:
    return tuple((s.text, tuple(sorted(s.marks.items()))) for s in doc.spans())


def _apply_fresh(ops: list[Op], site: str = "fresh") -> RGA:
    doc = RGA(site)
    for _ in range(3):  # let causal buffers drain
        doc.apply_many(ops)
    return doc


# --------------------------------------------------------------- compact_ops


def test_typing_collapses_to_a_single_op() -> None:
    doc = RGA("a")
    ops: list[Op] = []
    for ch in "hello":
        ops.extend(doc.local_insert(len(doc), ch))
    assert len(ops) == 5

    compacted = compact_ops(ops)
    assert len(compacted) == 1
    assert isinstance(compacted[0], InsertOp)
    assert compacted[0].text == "hello"
    assert _apply_fresh(compacted).text() == "hello"


def test_compaction_is_effect_preserving_on_mixed_edits() -> None:
    doc = RGA("a")
    ops: list[Op] = []
    for ch in "hello world":
        ops.extend(doc.local_insert(len(doc), ch))
    ops.extend(doc.local_mark(0, 5, "bold", True))
    ops.extend(doc.local_delete(5, 1))
    for ch in "!!":
        ops.extend(doc.local_insert(len(doc), ch))

    compacted = compact_ops(ops)
    assert len(compacted) < len(ops)

    raw_replica, compact_replica = _apply_fresh(ops, "raw"), _apply_fresh(compacted, "small")
    assert raw_replica.text() == compact_replica.text() == doc.text()
    assert _spans_key(raw_replica) == _spans_key(compact_replica) == _spans_key(doc)


def test_deletes_coalesce_per_site() -> None:
    doc = RGA("a")
    doc.local_insert(0, "abcdefgh")
    ops: list[Op] = []
    for _ in range(4):
        ops.extend(doc.local_delete(0, 1))

    compacted = compact_ops(ops)
    deletes = [o for o in compacted if isinstance(o, DeleteOp)]
    assert len(deletes) == 1
    assert len(deletes[0].targets) == 4


def test_superseded_marks_are_dropped() -> None:
    doc = RGA("a")
    setup = doc.local_insert(0, "abcdef")
    ops: list[Op] = []
    ops.extend(doc.local_mark(0, 6, "bold", True))
    ops.extend(doc.local_mark(0, 6, "bold", None))
    ops.extend(doc.local_mark(0, 6, "bold", True))

    compacted = compact_ops(ops)
    assert len(compacted) == 1, "only the winning mark can ever be observable"
    assert _spans_key(_apply_fresh(setup + compacted)) == _spans_key(doc)


def test_interleaved_sites_do_not_merge() -> None:
    """Merging across sites would fabricate ids that no site ever issued."""
    alice, bob = RGA("alice"), RGA("bob")
    a1 = alice.local_insert(0, "a")
    bob.apply_many(a1)
    b1 = bob.local_insert(1, "b")
    alice.apply_many(b1)
    a2 = alice.local_insert(2, "c")

    compacted = compact_ops([*a1, *b1, *a2])
    assert len(compacted) == 3, "ops from different sites must stay separate"
    assert _apply_fresh(compacted).text() == "abc"


def test_run_merging_respects_the_wire_size_cap() -> None:
    from syncpad.crdt import MAX_INSERT_RUN

    doc = RGA("a")
    ops: list[Op] = []
    for _ in range(MAX_INSERT_RUN + 50):
        ops.extend(doc.local_insert(len(doc), "x"))

    compacted = compact_ops(ops)
    assert all(not isinstance(o, InsertOp) or len(o.text) <= MAX_INSERT_RUN for o in compacted), (
        "a merged run must still fit in one frame"
    )
    assert _apply_fresh(compacted).text() == doc.text()


def test_compaction_handles_degenerate_input() -> None:
    assert compact_ops([]) == []
    doc = RGA("a")
    single = doc.local_insert(0, "x")
    assert compact_ops(single) == single


def test_duplicate_delete_targets_are_deduplicated() -> None:
    doc = RGA("a")
    doc.local_insert(0, "abc")
    target = doc.id_at(0)
    assert target is not None
    ops: list[Op] = [
        DeleteOp(kind="del", id=OpId("a", 100), targets=(target,)),
        DeleteOp(kind="del", id=OpId("a", 101), targets=(target,)),
    ]
    compacted = compact_ops(ops)
    assert len(compacted) == 1
    assert isinstance(compacted[0], DeleteOp)
    assert compacted[0].targets == (target,)


def test_compaction_ratio_reports_savings() -> None:
    assert compaction_ratio([], []) == 0.0
    doc = RGA("a")
    ops: list[Op] = []
    for ch in "abcdefghij":
        ops.extend(doc.local_insert(len(doc), ch))
    assert compaction_ratio(ops, compact_ops(ops)) == 0.9


@given(
    st.lists(
        st.tuples(
            st.sampled_from(("ins", "del", "mark")),
            st.integers(min_value=0, max_value=9999),
            st.integers(min_value=1, max_value=4),
            st.text(alphabet="abc ", min_size=1, max_size=3),
            st.sampled_from(("bold", "italic")),
            st.sampled_from((True, None)),
        ),
        min_size=1,
        max_size=30,
    )
)
@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_compaction_never_changes_the_document(script: list[tuple]) -> None:
    doc = RGA("author")
    ops: list[Op] = []
    for kind, pos, length, text, key, value in script:
        visible = len(doc)
        if kind == "ins" or visible == 0:
            ops.extend(doc.local_insert(pos % (visible + 1), text))
        elif kind == "del":
            start = pos % visible
            ops.extend(doc.local_delete(start, min(length, visible - start)))
        else:
            start = pos % visible
            ops.extend(doc.local_mark(start, min(length, visible - start), key, value))

    compacted = compact_ops(ops)
    assert len(compacted) <= len(ops)

    raw, small = _apply_fresh(ops, "raw"), _apply_fresh(compacted, "small")
    assert raw.text() == small.text() == doc.text()
    assert _spans_key(raw) == _spans_key(small) == _spans_key(doc)


# ------------------------------------------------------------ gc_tombstones


def test_gc_reclaims_stable_tombstones_without_changing_text() -> None:
    doc = RGA("a")
    doc.local_insert(0, "hello cruel world")
    doc.local_delete(5, 6)  # drop " cruel"
    before_text = doc.text()
    assert doc.tombstones == 6

    reclaimed = gc_tombstones(doc, doc.version_vector())
    assert reclaimed > 0
    assert doc.text() == before_text
    assert doc.tombstones < 6
    doc.check_invariants()


def test_gc_refuses_tombstones_a_peer_has_not_seen() -> None:
    """The safety property: an unstable tombstone is an anchor still in use."""
    doc = RGA("a")
    doc.local_insert(0, "abcdef")
    doc.local_delete(2, 2)
    assert doc.tombstones == 2

    # A peer that has seen nothing at all.
    stable = vv_min([doc.version_vector(), {}])
    assert gc_tombstones(doc, stable) == 0
    assert doc.tombstones == 2, "tombstones must survive until every replica agrees"


def test_gc_keeps_anchors_that_survivors_depend_on() -> None:
    doc = RGA("a")
    doc.local_insert(0, "abc")
    doc.local_delete(1, 1)  # tombstone 'b', which 'c' is anchored to
    doc.local_insert(2, "d")

    gc_tombstones(doc, doc.version_vector())
    assert doc.text() == "acd"
    doc.check_invariants()


def test_gc_keeps_anchors_that_marks_depend_on() -> None:
    doc = RGA("a")
    doc.local_insert(0, "hello world")
    doc.local_mark(0, 5, "bold", True)
    doc.local_delete(0, 5)  # the mark's anchors are now tombstones

    before = _spans_key(doc)
    gc_tombstones(doc, doc.version_vector())
    assert _spans_key(doc) == before, "dropping a mark anchor would lose formatting"
    doc.check_invariants()


def test_gc_replica_still_converges_with_a_non_gc_replica() -> None:
    """The point of the whole exercise: GC is a local optimisation only."""
    author = RGA("author")
    setup = author.local_insert(0, "the quick brown fox")
    removed = author.local_delete(4, 6)

    collected, intact = RGA("collected"), RGA("intact")
    for replica in (collected, intact):
        replica.apply_many([*setup, *removed])

    gc_tombstones(collected, collected.version_vector())
    later = author.local_insert(len(author), " jumps")
    collected.apply_many(later)
    intact.apply_many(later)

    assert collected.text() == intact.text() == author.text()
    collected.check_invariants()


def test_gc_on_a_document_with_nothing_to_collect() -> None:
    doc = RGA("a")
    doc.local_insert(0, "intact")
    assert gc_tombstones(doc, doc.version_vector()) == 0
    assert doc.text() == "intact"


def test_gc_then_snapshot_round_trips() -> None:
    doc = RGA("a")
    rnd = random.Random(5)
    doc.local_insert(0, "lorem ipsum dolor sit amet " * 10)
    for _ in range(40):
        doc.local_delete(rnd.randrange(len(doc) - 1), 1)

    gc_tombstones(doc, doc.version_vector())
    restored = RGA.load("b", doc.snapshot())
    assert restored.text() == doc.text()
    assert restored.elements == doc.elements
    restored.check_invariants()

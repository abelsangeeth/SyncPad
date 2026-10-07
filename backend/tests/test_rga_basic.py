"""Sequence mechanics: does the RGA behave like the text editor people expect?

The interesting tests live in test_rga_convergence.py. These are the ones that
catch the dull, expensive bugs — an off-by-one in index math, a stale aggregate
in the treap — by checking every index function against a naive list that is too
slow to ship but too simple to be wrong.
"""

from __future__ import annotations

import random
import time

import pytest

from syncpad.crdt import MAX_INSERT_RUN, RGA, DeleteOp, InsertOp, OpId


class Oracle:
    """A deliberately naive replica: a Python list and linear scans."""

    def __init__(self) -> None:
        self.items: list[tuple[OpId, str, bool]] = []

    def sync(self, rga: RGA) -> None:
        self.items = [(n.value.id, n.value.ch, n.value.deleted) for n in rga._iter_nodes()]

    @property
    def visible(self) -> list[tuple[OpId, str, bool]]:
        return [x for x in self.items if not x[2]]

    def text(self) -> str:
        return "".join(c for _, c, _ in self.visible)

    def index_of(self, oid: OpId) -> int | None:
        for i, (eid, _, _deleted) in enumerate(self.visible):
            if eid == oid:
                return i
        return None


def test_empty_document() -> None:
    doc = RGA("a")
    assert doc.text() == ""
    assert len(doc) == 0
    assert doc.elements == 0
    assert doc.tombstones == 0
    assert doc.spans() == []
    assert doc.id_at(0) is None
    assert doc.marks_at(0) == {}


def test_insert_and_read() -> None:
    doc = RGA("a")
    doc.local_insert(0, "world")
    doc.local_insert(0, "hello ")
    doc.local_insert(len(doc), "!")
    assert doc.text() == "hello world!"
    assert len(doc) == 12


def test_insert_returns_broadcastable_ops() -> None:
    doc = RGA("a")
    ops = doc.local_insert(0, "hi")
    assert len(ops) == 1
    op = ops[0]
    assert isinstance(op, InsertOp)
    assert op.text == "hi"
    assert op.after is None


def test_long_insert_splits_into_wire_sized_runs() -> None:
    doc = RGA("a")
    text = "z" * (MAX_INSERT_RUN * 2 + 7)
    ops = doc.local_insert(0, text)
    assert len(ops) == 3
    assert all(isinstance(o, InsertOp) and len(o.text) <= MAX_INSERT_RUN for o in ops)
    assert doc.text() == text
    # The chunks must chain, or the second run would not follow the first.
    replica = RGA("b")
    replica.apply_many(ops)
    assert replica.text() == text


def test_delete_tombstones_rather_than_removing() -> None:
    doc = RGA("a")
    doc.local_insert(0, "abcdef")
    doc.local_delete(2, 2)
    assert doc.text() == "abef"
    assert doc.elements == 6, "tombstones must remain addressable"
    assert doc.tombstones == 2


def test_delete_whole_document_then_retype() -> None:
    doc = RGA("a")
    doc.local_insert(0, "gone")
    doc.local_delete(0, 4)
    assert doc.text() == ""
    assert len(doc) == 0
    doc.local_insert(0, "back")
    assert doc.text() == "back"


def test_index_of_tombstoned_is_none_but_anchor_survives() -> None:
    doc = RGA("a")
    doc.local_insert(0, "abc")
    target = doc.id_at(1)
    assert target is not None
    doc.local_delete(1, 1)
    assert doc.index_of(target) is None
    # A cursor parked on deleted text still resolves somewhere sensible.
    assert doc.anchor_index(target) == 1


def test_out_of_range_edits_raise() -> None:
    doc = RGA("a")
    doc.local_insert(0, "abc")
    with pytest.raises(IndexError):
        doc.local_insert(9, "x")
    with pytest.raises(IndexError):
        doc.local_insert(-1, "x")
    with pytest.raises(IndexError):
        doc.local_delete(2, 5)


def test_degenerate_edits_are_noops_not_errors() -> None:
    doc = RGA("a")
    doc.local_insert(0, "abc")
    assert doc.local_insert(1, "") == []
    assert doc.local_delete(1, 0) == []
    assert doc.local_mark(1, 0, "bold", True) == []
    assert doc.text() == "abc"


def test_apply_is_idempotent() -> None:
    doc = RGA("a")
    ops = doc.local_insert(0, "hello")
    ops += doc.local_delete(1, 1)
    replica = RGA("b")
    assert replica.apply_many(ops) == 2
    assert replica.apply_many(ops) == 0, "second application must change nothing"
    assert replica.apply_many(ops) == 0
    assert replica.text() == doc.text() == "hllo"


def test_delete_arriving_before_its_target() -> None:
    """Causally backwards delivery must still converge, not drop the delete."""
    source = RGA("a")
    insert_ops = source.local_insert(0, "abcd")
    delete_ops = source.local_delete(1, 2)

    replica = RGA("b")
    replica.apply_many(delete_ops)  # delete first
    assert replica.text() == ""
    replica.apply_many(insert_ops)
    assert replica.text() == "ad", "the early delete must still take effect"
    replica.check_invariants()


def test_insert_arriving_before_its_left_origin() -> None:
    source = RGA("a")
    first = source.local_insert(0, "abc")
    second = source.local_insert(3, "def")

    replica = RGA("b")
    assert replica.apply_many(second) == 0, "buffered, not applied"
    assert replica.text() == ""
    assert replica.pending == 1
    replica.apply_many(first)
    assert replica.text() == "abcdef", "buffered op must drain once its anchor lands"
    assert replica.pending == 0
    replica.check_invariants()


def test_long_backwards_causal_chain_drains_iteratively() -> None:
    """A reversed chain must not recurse — a client could send 10k of these."""
    source = RGA("a")
    batches = [source.local_insert(len(source), "x") for _ in range(4000)]

    replica = RGA("b")
    for ops in reversed(batches[1:]):
        replica.apply_many(ops)
    assert replica.pending == 3999
    replica.apply_many(batches[0])
    assert replica.text() == source.text()
    assert replica.pending == 0


def test_insert_into_region_deleted_concurrently() -> None:
    alice = RGA("alice")
    setup = alice.local_insert(0, "abcdef")
    bob = RGA("bob")
    bob.apply_many(setup)

    typed = alice.local_insert(3, "XY")  # types inside...
    removed = bob.local_delete(2, 2)  # ...while bob deletes around it

    alice.apply_many(removed)
    bob.apply_many(typed)
    assert alice.text() == bob.text()
    assert "XY" in alice.text(), "text typed into a deleted region must survive"


def test_index_functions_match_a_naive_oracle() -> None:
    rnd = random.Random(11)
    doc = RGA("oracle")
    oracle = Oracle()

    for step in range(900):
        visible = len(doc)
        if rnd.random() < 0.7 or visible == 0:
            chunk = "".join(rnd.choice("xyz") for _ in range(rnd.randint(1, 4)))
            doc.local_insert(rnd.randint(0, visible), chunk)
        else:
            start = rnd.randrange(visible)
            doc.local_delete(start, rnd.randint(1, min(3, visible - start)))

        if step % 60:
            continue
        oracle.sync(doc)
        assert doc.text() == oracle.text()
        assert len(doc) == len(oracle.visible)
        for i, (eid, _, _) in enumerate(oracle.visible):
            assert doc.id_at(i) == eid
            assert doc.index_of(eid) == i
        for eid, _, is_deleted in oracle.items:
            assert (doc.index_of(eid) is None) is is_deleted
        doc.check_invariants()


def test_index_math_is_not_quadratic() -> None:
    """A guard against an accidental O(n^2): the treap must stay sub-linear.

    The bound is deliberately loose. It is not a benchmark — it is a tripwire for
    the day someone replaces the treap with a list and every index lookup starts
    scanning the document.
    """
    doc = RGA("perf")
    rnd = random.Random(3)

    start = time.perf_counter()
    doc.local_insert(0, "seed text ")
    for _ in range(400):
        doc.local_insert(rnd.randrange(len(doc)), "lorem ipsum dolor sit amet consectetur ")
    for _ in range(4000):
        doc.local_delete(rnd.randrange(len(doc) - 1), 1)
    elapsed = time.perf_counter() - start

    assert doc.elements > 15_000
    assert elapsed < 20.0, f"15k+ element edit workload took {elapsed:.1f}s"


def test_snapshot_round_trip_is_exact() -> None:
    doc = RGA("a")
    doc.local_insert(0, "hello world")
    doc.local_delete(5, 1)
    doc.local_mark(0, 5, "bold", True)
    doc.apply(DeleteOp(kind="del", id=OpId("ghost", 99), targets=(OpId("ghost", 1),)))

    blob = doc.snapshot()
    restored = RGA.load("b", blob)

    assert restored.text() == doc.text()
    assert restored.spans() == doc.spans()
    assert restored.version_vector() == doc.version_vector()
    assert restored.tombstones == doc.tombstones
    assert restored.elements == doc.elements
    restored.check_invariants()


def test_snapshot_preserves_pending_deletes_for_unseen_text() -> None:
    """A delete for a character we have not seen must survive serialisation."""
    source = RGA("a")
    inserts = source.local_insert(0, "abc")
    deletes = source.local_delete(1, 1)

    replica = RGA("b")
    replica.apply_many(deletes)
    restored = RGA.load("c", replica.snapshot())
    restored.apply_many(inserts)
    assert restored.text() == "ac", "pending delete was lost across the snapshot"


def test_snapshot_rejects_a_format_from_the_future() -> None:
    doc = RGA("a")
    doc.local_insert(0, "x")
    import orjson

    payload = orjson.loads(doc.snapshot())
    payload["v"] = 999
    with pytest.raises(ValueError, match="unsupported snapshot format"):
        RGA.load("b", orjson.dumps(payload))


def test_restored_replica_keeps_editing_in_the_right_place() -> None:
    """The restored clock must outrank the snapshot, or new text sorts wrongly."""
    alice = RGA("alice")
    ops = alice.local_insert(0, "abc")
    bob = RGA.load("bob", alice.snapshot())

    bob_ops = bob.local_insert(3, "Z")
    alice.apply_many(bob_ops)
    alice.apply_many(ops)
    assert alice.text() == bob.text() == "abcZ"


def test_unicode_indexing_is_consistent() -> None:
    doc = RGA("a")
    text = "héllo 🌍 wörld 👨‍👩‍👧 end"
    doc.local_insert(0, text)
    assert doc.text() == text
    assert len(doc) == len(text)
    for i, _ch in enumerate(text):
        oid = doc.id_at(i)
        assert oid is not None
        assert doc.index_of(oid) == i
    assert "".join(s.text for s in doc.spans()) == text


def test_text_between_quotes_a_range() -> None:
    doc = RGA("a")
    doc.local_insert(0, "the quick brown fox")
    start, end = doc.id_at(4), doc.id_at(8)
    assert start is not None and end is not None
    assert doc.text_between(start, end) == "quick"
    doc.local_insert(0, "OH! ")
    assert doc.text_between(start, end) == "quick", "anchors survive edits before them"


def test_text_between_is_empty_for_unknown_anchors() -> None:
    doc = RGA("a")
    doc.local_insert(0, "abc")
    assert doc.text_between(OpId("nobody", 1), OpId("nobody", 2)) == ""


def test_concurrent_inserts_at_the_same_position_agree() -> None:
    base = RGA("base")
    setup = base.local_insert(0, "AB")

    sites = []
    for name in ("s1", "s2", "s3"):
        replica = RGA(name)
        replica.apply_many(setup)
        sites.append(replica)

    concurrent = [r.local_insert(1, name) for r, name in zip(sites, ("x", "y", "z"), strict=True)]
    for replica in sites:
        for ops in concurrent:
            replica.apply_many(ops)

    texts = {r.text() for r in sites}
    assert len(texts) == 1, f"tie-break diverged: {texts}"
    assert set(texts.pop()) == set("ABxyz")

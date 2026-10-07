"""Span-anchored formatting: the part of rich text that usually goes wrong.

The naive design — store a mark on each character it covers — breaks the moment
two people edit at once. Bold "hello", have someone else insert "y" in the
middle concurrently, and the mark has no opinion about the new character; worse,
if the mark was expressed as offsets, it now covers the wrong text entirely.

SyncPad anchors marks to character *ids* and resolves coverage at render time,
so a span keeps its meaning while the text inside it changes. These tests pin
that behaviour, including the cases where "obviously correct" is ambiguous.
"""

from __future__ import annotations

from syncpad.crdt import RGA, MarkOp, OpId


def marks_of(doc: RGA) -> list[tuple[str, dict]]:
    return [(s.text, dict(s.marks)) for s in doc.spans()]


def test_mark_applies_to_a_range() -> None:
    doc = RGA("a")
    doc.local_insert(0, "hello world")
    doc.local_mark(0, 5, "bold", True)
    assert marks_of(doc) == [("hello", {"bold": True}), (" world", {})]


def test_mark_removal_by_none_value() -> None:
    doc = RGA("a")
    doc.local_insert(0, "hello")
    doc.local_mark(0, 5, "bold", True)
    doc.local_mark(0, 5, "bold", None)
    assert marks_of(doc) == [("hello", {})]


def test_partial_unmark_splits_the_span() -> None:
    doc = RGA("a")
    doc.local_insert(0, "abcdef")
    doc.local_mark(0, 6, "bold", True)
    doc.local_mark(2, 2, "bold", None)
    assert marks_of(doc) == [
        ("ab", {"bold": True}),
        ("cd", {}),
        ("ef", {"bold": True}),
    ]


def test_overlapping_marks_with_different_keys_coexist() -> None:
    doc = RGA("a")
    doc.local_insert(0, "abcdef")
    doc.local_mark(0, 4, "bold", True)
    doc.local_mark(2, 4, "italic", True)
    assert marks_of(doc) == [
        ("ab", {"bold": True}),
        ("cd", {"bold": True, "italic": True}),
        ("ef", {"italic": True}),
    ]


def test_value_marks_carry_a_payload() -> None:
    doc = RGA("a")
    doc.local_insert(0, "click here")
    doc.local_mark(6, 4, "link", "https://example.com")
    assert marks_of(doc) == [("click ", {}), ("here", {"link": "https://example.com"})]


def test_expanding_mark_covers_text_typed_inside_it() -> None:
    doc = RGA("a")
    doc.local_insert(0, "bold")
    doc.local_mark(0, 4, "bold", True, expand=True)
    doc.local_insert(2, "XX")
    assert doc.text() == "boXXld"
    assert marks_of(doc) == [("boXXld", {"bold": True})], "typing inside bold stays bold"


def test_non_expanding_mark_excludes_later_text() -> None:
    doc = RGA("a")
    doc.local_insert(0, "bold")
    doc.local_mark(0, 4, "bold", True, expand=False)
    doc.local_insert(2, "XX")
    assert doc.text() == "boXXld"
    assert marks_of(doc) == [
        ("bo", {"bold": True}),
        ("XX", {}),
        ("ld", {"bold": True}),
    ]


def test_concurrent_insert_inside_an_expanding_mark_converges() -> None:
    alice = RGA("alice")
    setup = alice.local_insert(0, "bold")
    bob = RGA("bob")
    bob.apply_many(setup)

    marked = alice.local_mark(0, 4, "bold", True, expand=True)
    typed = bob.local_insert(2, "XX")  # concurrent, neither has seen the other

    alice.apply_many(typed)
    bob.apply_many(marked)

    assert alice.text() == bob.text()
    assert marks_of(alice) == marks_of(bob)
    assert marks_of(alice) == [("boXXld", {"bold": True})]


def test_concurrent_insert_inside_a_non_expanding_mark_converges() -> None:
    alice = RGA("alice")
    setup = alice.local_insert(0, "bold")
    bob = RGA("bob")
    bob.apply_many(setup)

    marked = alice.local_mark(0, 4, "bold", True, expand=False)
    typed = bob.local_insert(2, "XX")

    alice.apply_many(typed)
    bob.apply_many(marked)

    assert alice.text() == bob.text()
    assert marks_of(alice) == marks_of(bob), "expand=False must still converge"


def test_conflicting_values_resolve_by_greatest_op_id() -> None:
    """Two sites format the same span differently; both must pick one winner."""
    base = RGA("base")
    setup = base.local_insert(0, "word")

    alice, bob = RGA("alice"), RGA("bob")
    alice.apply_many(setup)
    bob.apply_many(setup)

    a_ops = alice.local_mark(0, 4, "link", "https://alice.example")
    b_ops = bob.local_mark(0, 4, "link", "https://bob.example")

    alice.apply_many(b_ops)
    bob.apply_many(a_ops)

    assert marks_of(alice) == marks_of(bob)
    winner = marks_of(alice)[0][1]["link"]
    assert winner in {"https://alice.example", "https://bob.example"}


def test_mark_remove_remark_converges_in_any_order() -> None:
    base = RGA("base")
    setup = base.local_insert(0, "abcdef")

    ordered, shuffled = RGA("ordered"), RGA("shuffled")
    for replica in (ordered, shuffled):
        replica.apply_many(setup)

    editor = RGA("editor")
    editor.apply_many(setup)
    on = editor.local_mark(0, 6, "bold", True)
    off = editor.local_mark(0, 6, "bold", None)
    again = editor.local_mark(0, 6, "bold", True)

    ordered.apply_many([*on, *off, *again])
    shuffled.apply_many([*again, *on, *off])
    shuffled.apply_many([*on, *off, *again])

    assert marks_of(ordered) == marks_of(shuffled)
    assert marks_of(ordered) == [("abcdef", {"bold": True})], "latest op id wins"


def test_mark_over_deleted_anchors_still_resolves() -> None:
    """Tombstones keep anchors addressable, so the mark survives the deletion."""
    doc = RGA("a")
    doc.local_insert(0, "hello world")
    doc.local_mark(0, 5, "bold", True)
    doc.local_delete(0, 5)  # delete exactly the marked text
    assert doc.text() == " world"
    assert marks_of(doc) == [(" world", {})]
    doc.check_invariants()


def test_mark_survives_deletion_of_only_its_endpoint() -> None:
    doc = RGA("a")
    doc.local_insert(0, "abcdef")
    doc.local_mark(1, 4, "italic", True)  # covers b,c,d,e
    doc.local_delete(4, 1)  # delete 'e', the span's end anchor
    assert doc.text() == "abcdf"
    assert marks_of(doc) == [("a", {}), ("bcd", {"italic": True}), ("f", {})]


def test_mark_whose_anchors_are_unknown_is_inert_then_activates() -> None:
    source = RGA("a")
    inserts = source.local_insert(0, "abcdef")
    mark = source.local_mark(1, 3, "bold", True)

    replica = RGA("b")
    replica.apply_many(mark)  # anchors have not arrived yet
    assert replica.text() == ""
    assert replica.spans() == []

    replica.apply_many(inserts)
    assert marks_of(replica) == marks_of(source)


def test_marks_at_reports_formatting_under_the_cursor() -> None:
    doc = RGA("a")
    doc.local_insert(0, "abcdef")
    doc.local_mark(2, 2, "bold", True)
    assert doc.marks_at(0) == {}
    assert doc.marks_at(2) == {"bold": True}
    assert doc.marks_at(3) == {"bold": True}
    assert doc.marks_at(4) == {}
    assert doc.marks_at(99) == {}


def test_reversed_span_is_normalised() -> None:
    """A hostile or buggy client may send end-before-start; do not misbehave."""
    doc = RGA("a")
    doc.local_insert(0, "abcdef")
    start, end = doc.id_at(1), doc.id_at(4)
    assert start is not None and end is not None
    doc.apply(
        MarkOp(kind="mark", id=OpId("evil", 500), start=end, end=start, key="bold", value=True)
    )
    assert marks_of(doc) == [("a", {}), ("bcde", {"bold": True}), ("f", {})]


def test_single_character_span() -> None:
    doc = RGA("a")
    doc.local_insert(0, "abc")
    doc.local_mark(1, 1, "code", True)
    assert marks_of(doc) == [("a", {}), ("b", {"code": True}), ("c", {})]


def test_marks_survive_snapshot_round_trip() -> None:
    doc = RGA("a")
    doc.local_insert(0, "hello world")
    doc.local_mark(0, 5, "bold", True)
    doc.local_mark(6, 5, "link", "https://example.com")
    doc.local_mark(2, 2, "italic", True, expand=False)

    restored = RGA.load("b", doc.snapshot())
    assert marks_of(restored) == marks_of(doc)

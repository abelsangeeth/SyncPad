"""The wire codec is a trust boundary, so every rejection path gets a test.

Operations arrive from browsers SyncPad does not control. The contract for this
layer is narrow and absolute: decode anything well-formed, and reject everything
else with :class:`OpDecodeError` — never a bare ``KeyError`` or ``TypeError``
escaping into the gateway and becoming a 500.
"""

from __future__ import annotations

import pytest

from syncpad.crdt import (
    MAX_INSERT_RUN,
    DeleteOp,
    InsertOp,
    LamportClock,
    MarkOp,
    OpDecodeError,
    OpId,
    op_from_dict,
    op_to_dict,
    ops_from_dicts,
    ops_to_dicts,
    vv_covers,
    vv_merge,
    vv_min,
)

# --------------------------------------------------------------------- OpId


def test_opid_encodes_and_decodes() -> None:
    oid = OpId("site-a", 42)
    assert oid.encode() == "site-a:42"
    assert OpId.decode("site-a:42") == oid


def test_opid_total_order_is_counter_then_site() -> None:
    assert OpId("b", 1) < OpId("a", 2), "counter dominates"
    assert OpId("a", 5) < OpId("b", 5), "site breaks ties"
    assert OpId("a", 5) <= OpId("a", 5)
    assert OpId("b", 5) > OpId("a", 5)
    assert OpId("b", 5) >= OpId("b", 5)
    assert sorted([OpId("z", 3), OpId("a", 1), OpId("a", 3)]) == [
        OpId("a", 1),
        OpId("a", 3),
        OpId("z", 3),
    ]


def test_opid_comparison_with_other_types_is_not_implemented() -> None:
    for op in ("__lt__", "__le__", "__gt__", "__ge__"):
        assert getattr(OpId("a", 1), op)("nonsense") is NotImplemented


def test_opid_offset_walks_a_run() -> None:
    assert OpId("a", 5).offset(3) == OpId("a", 8)


def test_opid_rejects_malformed_identity() -> None:
    with pytest.raises(ValueError, match="may not contain"):
        OpId("has:colon", 1)
    with pytest.raises(ValueError, match="may not be empty"):
        OpId("", 1)
    with pytest.raises(ValueError, match="counter must be"):
        OpId("a", 0)


@pytest.mark.parametrize("bad", ["", "nocolon", ":5", "site:notanumber", "site:"])
def test_opid_decode_rejects_junk(bad: str) -> None:
    with pytest.raises(ValueError, match="malformed OpId"):
        OpId.decode(bad)


def test_opid_repr_is_readable() -> None:
    assert "7" in repr(OpId("abcdefghij", 7))


# -------------------------------------------------------------- LamportClock


def test_clock_ticks_monotonically() -> None:
    clock = LamportClock("a")
    assert clock.site == "a"
    assert clock.tick() == OpId("a", 1)
    assert clock.tick() == OpId("a", 2)
    assert clock.counter == 2


def test_clock_tick_run_reserves_a_block() -> None:
    clock = LamportClock("a")
    first = clock.tick_run(5)
    assert first == OpId("a", 1)
    assert clock.counter == 5
    assert clock.tick() == OpId("a", 6)


def test_clock_observe_absorbs_remote_time() -> None:
    clock = LamportClock("a")
    clock.observe(OpId("b", 99))
    assert clock.tick() == OpId("a", 100), "local ids must outrank what we have seen"
    clock.observe(OpId("b", 5))
    assert clock.counter == 100, "observing older time must not rewind the clock"


def test_clock_rejects_invalid_construction() -> None:
    with pytest.raises(ValueError):
        LamportClock("bad:site")
    with pytest.raises(ValueError):
        LamportClock("")
    with pytest.raises(ValueError):
        LamportClock("a", -1)
    with pytest.raises(ValueError):
        LamportClock("a").tick_run(0)


def test_clock_repr_is_readable() -> None:
    assert "LamportClock" in repr(LamportClock("abcdefghij", 3))


# ----------------------------------------------------------- version vectors


def test_version_vector_helpers() -> None:
    assert vv_merge({"a": 3, "b": 1}, {"b": 7, "c": 2}) == {"a": 3, "b": 7, "c": 2}
    assert vv_covers({"a": 5}, OpId("a", 5)) is True
    assert vv_covers({"a": 5}, OpId("a", 6)) is False
    assert vv_covers({}, OpId("a", 1)) is False


def test_vv_min_treats_an_unheard_site_as_zero() -> None:
    """Safety for tombstone GC: silence must never be read as agreement."""
    assert vv_min([]) == {}
    assert vv_min([{"a": 5, "b": 3}, {"a": 2}]) == {"a": 2, "b": 0}
    assert vv_min([{"a": 5}, {"a": 5}]) == {"a": 5}


# ---------------------------------------------------------------- round trip


def test_insert_round_trip() -> None:
    op = InsertOp(kind="ins", id=OpId("a", 1), after=OpId("b", 9), text="hello")
    assert op_from_dict(op_to_dict(op)) == op


def test_insert_at_document_start_round_trip() -> None:
    op = InsertOp(kind="ins", id=OpId("a", 1), after=None, text="hi")
    encoded = op_to_dict(op)
    assert encoded["after"] is None
    assert op_from_dict(encoded) == op


def test_delete_round_trip() -> None:
    op = DeleteOp(kind="del", id=OpId("a", 3), targets=(OpId("b", 1), OpId("b", 2)))
    assert op_from_dict(op_to_dict(op)) == op


def test_mark_round_trip() -> None:
    op = MarkOp(
        kind="mark",
        id=OpId("a", 3),
        start=OpId("b", 1),
        end=OpId("b", 5),
        key="link",
        value="https://example.com",
        expand=False,
    )
    assert op_from_dict(op_to_dict(op)) == op


def test_batch_round_trip() -> None:
    ops = [
        InsertOp(kind="ins", id=OpId("a", 1), after=None, text="hi"),
        DeleteOp(kind="del", id=OpId("a", 3), targets=(OpId("a", 1),)),
    ]
    assert ops_from_dicts(ops_to_dicts(ops)) == ops


def test_encoding_a_non_operation_raises() -> None:
    with pytest.raises(TypeError, match="not an operation"):
        op_to_dict("definitely not an op")  # type: ignore[arg-type]


# ------------------------------------------------------------ hostile input


@pytest.mark.parametrize(
    "payload,match",
    [
        ({"k": "wat"}, "unknown operation kind"),
        ({}, "unknown operation kind"),
        ({"k": "ins", "id": "a:1", "after": None}, "'tx' must be a string"),
        ({"k": "ins", "id": "a:1", "after": None, "tx": 12}, "'tx' must be a string"),
        ({"k": "ins", "id": "a:1", "after": None, "tx": ""}, "must be non-empty"),
        ({"k": "ins", "id": "a:1", "after": 7, "tx": "x"}, "'after' must be"),
        ({"k": "ins", "id": "a:1", "after": "bogus", "tx": "x"}, "malformed OpId"),
        ({"k": "ins", "id": "nope", "after": None, "tx": "x"}, "malformed OpId"),
        ({"k": "ins", "id": 5, "after": None, "tx": "x"}, "must be an OpId string"),
        ({"k": "del", "id": "a:1"}, "'tg' must be a list"),
        ({"k": "del", "id": "a:1", "tg": []}, "must be non-empty"),
        ({"k": "del", "id": "a:1", "tg": [7]}, "must be OpId strings"),
        ({"k": "del", "id": "a:1", "tg": ["junk"]}, "malformed OpId"),
        ({"k": "mark", "id": "a:1", "s": "a:1", "e": "a:2", "key": "evil"}, "'key' must be"),
        ({"k": "mark", "id": "a:1", "s": "a:1", "e": "a:2", "key": 9}, "'key' must be"),
        (
            {"k": "mark", "id": "a:1", "s": "a:1", "e": "a:2", "key": "bold", "v": 3.5},
            "'v' must be",
        ),
        (
            {
                "k": "mark",
                "id": "a:1",
                "s": "a:1",
                "e": "a:2",
                "key": "bold",
                "v": True,
                "x": "yes",
            },
            "'x' must be a bool",
        ),
        ({"k": "mark", "id": "a:1", "e": "a:2", "key": "bold"}, "must be an OpId string"),
    ],
)
def test_malformed_operations_are_rejected_cleanly(payload: dict, match: str) -> None:
    with pytest.raises(OpDecodeError, match=match):
        op_from_dict(payload)


def test_oversized_insert_run_is_rejected() -> None:
    payload = {"k": "ins", "id": "a:1", "after": None, "tx": "x" * (MAX_INSERT_RUN + 1)}
    with pytest.raises(OpDecodeError, match="exceeds"):
        op_from_dict(payload)


def test_oversized_delete_batch_is_rejected() -> None:
    payload = {"k": "del", "id": "a:1", "tg": [f"b:{i + 1}" for i in range(MAX_INSERT_RUN + 1)]}
    with pytest.raises(OpDecodeError, match="exceeds"):
        op_from_dict(payload)


def test_oversized_mark_value_is_rejected() -> None:
    payload = {
        "k": "mark",
        "id": "a:1",
        "s": "a:1",
        "e": "a:2",
        "key": "link",
        "v": "u" * 3000,
    }
    with pytest.raises(OpDecodeError, match="too long"):
        op_from_dict(payload)


def test_non_object_operation_is_rejected() -> None:
    with pytest.raises(OpDecodeError, match="must be an object"):
        op_from_dict(["not", "a", "dict"])  # type: ignore[arg-type]


def test_non_list_batch_is_rejected() -> None:
    with pytest.raises(OpDecodeError, match="'ops' must be a list"):
        ops_from_dicts({"not": "a list"})


def test_mark_expand_defaults_to_true_when_absent() -> None:
    op = op_from_dict({"k": "mark", "id": "a:1", "s": "a:1", "e": "a:2", "key": "bold", "v": True})
    assert isinstance(op, MarkOp)
    assert op.expand is True

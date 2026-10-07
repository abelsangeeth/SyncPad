"""Frame encoding and, mostly, frame rejection.

The gateway's contract with a browser is that no input can produce a traceback
or a dropped connection with no explanation. So every malformed shape gets a
test, and every one of them must raise :class:`BadFrame` carrying the error code
the client will be told.
"""

from __future__ import annotations

import pytest

from syncpad.crdt import RGA
from syncpad.gateway.protocol import (
    MAX_FRAME_BYTES,
    MAX_OPS_PER_FRAME,
    BadFrame,
    ErrorCode,
    ack,
    cursor_frame,
    decode_frame,
    encode_frame,
    error,
    ops_frame,
    parse_cursor,
    parse_hello,
    parse_ops,
    pong,
    presence_frame,
    welcome,
)


def roundtrip(obj: dict) -> dict:
    return decode_frame(encode_frame(obj))


def test_encode_decode_round_trip() -> None:
    assert roundtrip({"t": "ping"}) == {"t": "ping"}
    assert roundtrip({"t": "ops", "ops": [], "cseq": 3})["cseq"] == 3


def test_decode_accepts_bytes_and_str() -> None:
    assert decode_frame('{"t":"ping"}')["t"] == "ping"
    assert decode_frame(b'{"t":"ping"}')["t"] == "ping"


@pytest.mark.parametrize(
    "raw,match",
    [
        ("not json at all", "malformed JSON"),
        ("[1,2,3]", "must be a JSON object"),
        ('"a string"', "must be a JSON object"),
        ("{}", "missing its 't'"),
        ('{"t":123}', "missing its 't'"),
    ],
)
def test_malformed_frames_are_rejected(raw: str, match: str) -> None:
    with pytest.raises(BadFrame, match=match):
        decode_frame(raw)


def test_oversized_frames_are_rejected_before_parsing() -> None:
    """The size check must precede the parse, or the cap buys nothing."""
    payload = b'{"t":"ops","pad":"' + b"x" * (MAX_FRAME_BYTES + 10) + b'"}'
    with pytest.raises(BadFrame) as info:
        decode_frame(payload)
    assert info.value.code == ErrorCode.TOO_LARGE


# ------------------------------------------------------------------- hello


def test_parse_hello_extracts_its_fields() -> None:
    doc, token, share, since = parse_hello(
        {"t": "hello", "doc": "d1", "token": "jwt", "share": "s1", "since": 7}
    )
    assert (doc, token, share, since) == ("d1", "jwt", "s1", 7)


def test_parse_hello_defaults_are_anonymous_and_cold() -> None:
    doc, token, share, since = parse_hello({"t": "hello", "doc": "d1"})
    assert (doc, token, share, since) == ("d1", None, None, 0)


@pytest.mark.parametrize(
    "frame,match",
    [
        ({"t": "hello"}, "doc must be"),
        ({"t": "hello", "doc": ""}, "doc must be"),
        ({"t": "hello", "doc": 5}, "doc must be"),
        ({"t": "hello", "doc": "d", "token": 5}, "token must be"),
        ({"t": "hello", "doc": "d", "share": 5}, "share must be"),
        ({"t": "hello", "doc": "d", "since": -1}, "since must be"),
        ({"t": "hello", "doc": "d", "since": "3"}, "since must be"),
        ({"t": "hello", "doc": "d", "since": True}, "since must be"),
    ],
)
def test_parse_hello_rejects_bad_fields(frame: dict, match: str) -> None:
    with pytest.raises(BadFrame, match=match):
        parse_hello(frame)


# --------------------------------------------------------------------- ops


def test_parse_ops_decodes_operations() -> None:
    doc = RGA("a")
    from syncpad.crdt import ops_to_dicts

    encoded = ops_to_dicts(doc.local_insert(0, "hello"))
    ops, cseq = parse_ops({"t": "ops", "ops": encoded, "cseq": 12})
    assert len(ops) == 1
    assert cseq == 12


def test_parse_ops_defaults_cseq() -> None:
    _, cseq = parse_ops({"t": "ops", "ops": []})
    assert cseq == 0


@pytest.mark.parametrize(
    "frame,match",
    [
        ({"t": "ops"}, "must be a list"),
        ({"t": "ops", "ops": "nope"}, "must be a list"),
        ({"t": "ops", "ops": [{"k": "bogus"}]}, "invalid operation"),
        ({"t": "ops", "ops": [], "cseq": "x"}, "cseq must be"),
        ({"t": "ops", "ops": [], "cseq": True}, "cseq must be"),
    ],
)
def test_parse_ops_rejects_bad_frames(frame: dict, match: str) -> None:
    with pytest.raises(BadFrame, match=match):
        parse_ops(frame)


def test_too_many_operations_in_one_frame_is_rejected() -> None:
    """Bounds the work a single message can cause."""
    payload = [
        {"k": "ins", "id": f"a:{i + 1}", "after": None, "tx": "x"}
        for i in range(MAX_OPS_PER_FRAME + 1)
    ]
    with pytest.raises(BadFrame) as info:
        parse_ops({"t": "ops", "ops": payload})
    assert info.value.code == ErrorCode.TOO_LARGE


# ------------------------------------------------------------------ cursor


def test_parse_cursor_accepts_anchors_and_nulls() -> None:
    assert parse_cursor({"anchor": "a:1", "head": "a:5"}) == ("a:1", "a:5")
    assert parse_cursor({}) == (None, None)
    assert parse_cursor({"anchor": None, "head": None}) == (None, None)


@pytest.mark.parametrize("frame", [{"anchor": 5}, {"head": []}, {"anchor": "x" * 100}])
def test_parse_cursor_rejects_bad_anchors(frame: dict) -> None:
    with pytest.raises(BadFrame, match="must be an OpId string"):
        parse_cursor(frame)


# ---------------------------------------------------------- server frames


def test_welcome_frame_shape() -> None:
    doc = RGA("a")
    ops = doc.local_insert(0, "hi")
    frame = decode_frame(
        welcome(
            doc_id="d1",
            site="s1",
            seq=4,
            snapshot_b64="YmxvYg==",
            ops=[(4, ops)],
            peers=[{"site": "s2"}],
            role="editor",
            user={"id": "u1"},
            title="Doc",
        )
    )
    assert frame["t"] == "welcome"
    assert frame["doc"] == "d1"
    assert frame["seq"] == 4
    assert frame["snapshot"] == "YmxvYg=="
    assert frame["ops"][0]["seq"] == 4
    assert frame["role"] == "editor"
    assert frame["title"] == "Doc"


def test_ops_ack_presence_cursor_error_and_pong_frames() -> None:
    doc = RGA("a")
    ops = doc.local_insert(0, "x")

    assert decode_frame(ops_frame(seq=2, ops=ops, origin="s1"))["origin"] == "s1"
    assert decode_frame(ack(cseq=9, seq=3)) == {"t": "ack", "cseq": 9, "seq": 3}
    assert decode_frame(presence_frame([{"site": "a"}]))["peers"] == [{"site": "a"}]

    cursor = decode_frame(cursor_frame(site="s", anchor="a:1", head=None, user=None))
    assert cursor["anchor"] == "a:1" and cursor["head"] is None

    failure = decode_frame(error(ErrorCode.RATE_LIMITED, "slow down", fatal=True))
    assert failure["code"] == "rate_limited" and failure["fatal"] is True

    assert decode_frame(pong()) == {"t": "pong"}


def test_bad_frame_carries_a_default_code() -> None:
    assert BadFrame("nope").code == ErrorCode.BAD_FRAME
    assert BadFrame("nope", ErrorCode.FORBIDDEN).code == ErrorCode.FORBIDDEN

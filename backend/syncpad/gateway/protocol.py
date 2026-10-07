"""The wire protocol: every frame that crosses a SyncPad WebSocket.

Frames are JSON objects discriminated by ``t``. JSON rather than a binary format
because the client is a browser and the debuggability is worth more than the
bytes; ``orjson`` rather than ``json`` because a busy document moves thousands of
these per second and the difference shows up in the latency histogram.

Decoding is a trust boundary. Everything here treats input as hostile: size caps
before parsing, type checks on every field, and a single exception type
(:class:`BadFrame`) so the socket handler can answer with a clean error frame
instead of a stack trace and a dropped connection.
"""

from __future__ import annotations

from typing import Any, Final

import orjson

from syncpad.crdt import Op, OpDecodeError, ops_from_dicts, ops_to_dicts

__all__ = [
    "MAX_FRAME_BYTES",
    "MAX_OPS_PER_FRAME",
    "BadFrame",
    "ErrorCode",
    "ack",
    "batch",
    "cursor_frame",
    "decode_frame",
    "encode_frame",
    "error",
    "ops_frame",
    "pong",
    "presence_frame",
    "welcome",
]

#: Hard cap on a single inbound frame. Above this the connection is answered
#: with ``too_large`` and the payload is never parsed.
MAX_FRAME_BYTES: Final = 256 * 1024

#: Operations one frame may carry. Bounds the work a single message can cause.
MAX_OPS_PER_FRAME: Final = 256


class ErrorCode:
    AUTH_FAILED = "auth_failed"
    FORBIDDEN = "forbidden"
    NOT_FOUND = "not_found"
    BAD_FRAME = "bad_frame"
    RATE_LIMITED = "rate_limited"
    TOO_LARGE = "too_large"
    INTERNAL = "internal"


class BadFrame(ValueError):
    """A frame that could not be understood. Carries the code to answer with."""

    def __init__(self, message: str, code: str = ErrorCode.BAD_FRAME) -> None:
        super().__init__(message)
        self.code = code


def encode_frame(obj: dict[str, Any]) -> bytes:
    return orjson.dumps(obj)


def decode_frame(raw: str | bytes) -> dict[str, Any]:
    """Parse and structurally validate one inbound frame."""
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    if len(raw) > MAX_FRAME_BYTES:
        raise BadFrame(f"frame of {len(raw)} bytes exceeds {MAX_FRAME_BYTES}", ErrorCode.TOO_LARGE)
    try:
        parsed = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise BadFrame(f"malformed JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise BadFrame("frame must be a JSON object")
    kind = parsed.get("t")
    if not isinstance(kind, str):
        raise BadFrame("frame is missing its 't' discriminator")
    return parsed


def parse_hello(frame: dict[str, Any]) -> tuple[str, str | None, str | None, int]:
    """Validate a ``hello`` frame -> (doc_id, token, share_token, since)."""
    doc_id = frame.get("doc")
    if not isinstance(doc_id, str) or not doc_id:
        raise BadFrame("hello.doc must be a document id")

    token = frame.get("token")
    if token is not None and not isinstance(token, str):
        raise BadFrame("hello.token must be a string or null")

    share = frame.get("share")
    if share is not None and not isinstance(share, str):
        raise BadFrame("hello.share must be a string or null")

    since = frame.get("since", 0)
    if not isinstance(since, int) or isinstance(since, bool) or since < 0:
        raise BadFrame("hello.since must be a non-negative integer")

    return doc_id, token, share, since


def parse_ops(frame: dict[str, Any]) -> tuple[list[Op], int]:
    """Validate an ``ops`` frame -> (operations, client sequence)."""
    raw = frame.get("ops")
    if not isinstance(raw, list):
        raise BadFrame("ops.ops must be a list")
    if len(raw) > MAX_OPS_PER_FRAME:
        raise BadFrame(
            f"{len(raw)} operations exceeds the {MAX_OPS_PER_FRAME} per-frame limit",
            ErrorCode.TOO_LARGE,
        )
    try:
        operations = ops_from_dicts(raw)
    except OpDecodeError as exc:
        raise BadFrame(f"invalid operation: {exc}") from exc

    cseq = frame.get("cseq", 0)
    if not isinstance(cseq, int) or isinstance(cseq, bool):
        raise BadFrame("ops.cseq must be an integer")
    return operations, cseq


def parse_cursor(frame: dict[str, Any]) -> tuple[str | None, str | None]:
    """Validate a ``cursor`` frame -> (anchor, head) as encoded OpIds."""
    anchor, head = frame.get("anchor"), frame.get("head")
    for name, value in (("anchor", anchor), ("head", head)):
        if value is not None and (not isinstance(value, str) or len(value) > 64):
            raise BadFrame(f"cursor.{name} must be an OpId string or null")
    return anchor, head


# --------------------------------------------------------------- server frames


def welcome(
    *,
    doc_id: str,
    site: str,
    seq: int,
    snapshot_b64: str | None,
    ops: list[tuple[int, list[Op]]],
    peers: list[dict[str, Any]],
    role: str,
    user: dict[str, Any] | None,
    title: str,
) -> bytes:
    """The opening frame: everything a client needs to start editing.

    A cold client gets a snapshot; a reconnecting one that supplied ``since``
    gets only the operations it missed, which is what makes a dropped connection
    cheap to recover from.
    """
    return encode_frame(
        {
            "t": "welcome",
            "doc": doc_id,
            "title": title,
            "site": site,
            "seq": seq,
            "snapshot": snapshot_b64,
            "ops": [{"seq": s, "ops": ops_to_dicts(batch)} for s, batch in ops],
            "peers": peers,
            "role": role,
            "you": user,
        }
    )


def ops_frame(*, seq: int, ops: list[Op], origin: str) -> bytes:
    return encode_frame({"t": "ops", "seq": seq, "ops": ops_to_dicts(ops), "origin": origin})


def ack(*, cseq: int, seq: int) -> bytes:
    """Confirm a client batch was persisted, so the client can drop its buffer."""
    return encode_frame({"t": "ack", "cseq": cseq, "seq": seq})


def presence_frame(peers: list[dict[str, Any]]) -> bytes:
    return encode_frame({"t": "presence", "peers": peers})


def cursor_frame(
    *, site: str, anchor: str | None, head: str | None, user: dict[str, Any] | None
) -> bytes:
    return encode_frame({"t": "cursor", "site": site, "anchor": anchor, "head": head, "user": user})


def error(code: str, message: str, *, fatal: bool = False) -> bytes:
    return encode_frame({"t": "error", "code": code, "msg": message, "fatal": fatal})


def pong() -> bytes:
    return encode_frame({"t": "pong"})


def batch(frames: list[bytes | str]) -> bytes:
    """Wrap several already-encoded frames into one.

    Emitted by the gateway only when frames pile up within a single event-loop
    iteration, which happens under fan-out load. Clients must unwrap it and
    handle each inner frame exactly as if it had arrived on its own.
    """
    parts = [f if isinstance(f, str) else f.decode("utf-8") for f in frames]
    return ('{"t":"batch","frames":[' + ",".join(parts) + "]}").encode("utf-8")

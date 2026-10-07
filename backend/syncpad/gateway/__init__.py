"""The realtime edge: WebSocket sessions, cross-replica fan-out, presence."""

from __future__ import annotations

from syncpad.gateway.hub import Hub, InMemoryHub, RedisHub, make_hub
from syncpad.gateway.presence import (
    MemoryPresence,
    Peer,
    PresenceStore,
    RedisPresence,
    make_presence,
)
from syncpad.gateway.protocol import BadFrame, ErrorCode, decode_frame, encode_frame

__all__ = [
    "BadFrame",
    "ErrorCode",
    "Hub",
    "InMemoryHub",
    "MemoryPresence",
    "Peer",
    "PresenceStore",
    "RedisHub",
    "RedisPresence",
    "decode_frame",
    "encode_frame",
    "make_hub",
    "make_presence",
]

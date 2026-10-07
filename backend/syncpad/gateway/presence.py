"""Who else is in this document, and where their cursor is.

Presence is soft state with a hard requirement: it must never accumulate ghosts.
A browser tab closed by a laptop lid, a process killed mid-connection, a network
partition — none of these produce a clean "leave", and every one of them would
otherwise leave a phantom collaborator in the sidebar forever.

So presence is stored with a TTL and swept on read. A peer that stops sending
heartbeats simply expires. That also means presence needs no cleanup path on
crash, which is the only cleanup path that can be relied upon.

In Redis each document is one hash of peers plus a sorted set of last-seen
timestamps, so a sweep is a range query rather than a scan of every field.
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from typing import Any, Protocol, runtime_checkable

import orjson

from syncpad.config import Settings

__all__ = ["MemoryPresence", "Peer", "PresenceStore", "RedisPresence", "make_presence"]

log = logging.getLogger("syncpad.presence")


@dataclass(slots=True)
class Peer:
    """One live editing session. Keyed by site, not by user.

    Two browser tabs are two peers with two cursors, which is correct: they edit
    independently and each has its own Lamport clock.
    """

    site: str
    user_id: str | None
    name: str
    color: str
    anchor: str | None = None
    head: str | None = None
    last_seen: float = 0.0

    def to_public(self) -> dict[str, Any]:
        """The shape sent to browsers. Deliberately excludes anything private."""
        return {
            "site": self.site,
            "userId": self.user_id,
            "name": self.name,
            "color": self.color,
            "anchor": self.anchor,
            "head": self.head,
        }


@runtime_checkable
class PresenceStore(Protocol):
    async def join(self, doc_id: str, peer: Peer) -> None: ...
    async def leave(self, doc_id: str, site: str) -> None: ...
    async def touch(self, doc_id: str, site: str, anchor: str | None, head: str | None) -> None: ...
    async def peers(self, doc_id: str) -> list[Peer]: ...
    async def close(self) -> None: ...


class MemoryPresence:
    """Per-process presence. Correct whenever there is only one replica."""

    def __init__(self, ttl: float = 30.0) -> None:
        self._ttl = ttl
        self._docs: dict[str, dict[str, Peer]] = {}

    async def join(self, doc_id: str, peer: Peer) -> None:
        peer.last_seen = time.monotonic()
        self._docs.setdefault(doc_id, {})[peer.site] = peer

    async def leave(self, doc_id: str, site: str) -> None:
        peers = self._docs.get(doc_id)
        if peers is None:
            return
        peers.pop(site, None)
        if not peers:
            self._docs.pop(doc_id, None)

    async def touch(self, doc_id: str, site: str, anchor: str | None, head: str | None) -> None:
        peer = self._docs.get(doc_id, {}).get(site)
        if peer is None:
            return
        peer.anchor = anchor
        peer.head = head
        peer.last_seen = time.monotonic()

    async def peers(self, doc_id: str) -> list[Peer]:
        peers = self._docs.get(doc_id)
        if not peers:
            return []
        cutoff = time.monotonic() - self._ttl
        alive = {site: p for site, p in peers.items() if p.last_seen >= cutoff}
        if len(alive) != len(peers):
            self._docs[doc_id] = alive
        return list(alive.values())

    async def close(self) -> None:
        self._docs.clear()


class RedisPresence:
    """Presence shared across replicas, expiring on its own.

    Peers live in a hash keyed by site and are indexed by last-seen time in a
    sorted set. Sweeping is then ``ZRANGEBYSCORE`` for the expired members
    followed by a single pipelined delete, rather than deserialising every peer
    on every read.
    """

    def __init__(self, url: str, ttl: float = 30.0) -> None:
        self._url = url
        self._ttl = ttl
        # Redis EXPIRE takes whole seconds, and `EXPIRE key 0` deletes the key
        # immediately. A short TTL would therefore truncate to zero and wipe
        # presence the instant it was written, emptying the collaborator list
        # for everyone. The floor keeps the backstop a backstop.
        self._key_ttl = max(1, int(ttl * 4))
        self._redis = None

    async def _client(self):
        if self._redis is None:
            from syncpad.gateway.hub import redis_client

            self._redis = redis_client(self._url)
        return self._redis

    @staticmethod
    def _hash_key(doc_id: str) -> str:
        return f"syncpad:presence:{doc_id}"

    @staticmethod
    def _index_key(doc_id: str) -> str:
        return f"syncpad:presence:{doc_id}:seen"

    async def join(self, doc_id: str, peer: Peer) -> None:
        redis = await self._client()
        peer.last_seen = time.time()
        payload = orjson.dumps(asdict(peer))
        async with redis.pipeline(transaction=False) as pipe:
            pipe.hset(self._hash_key(doc_id), peer.site, payload)
            pipe.zadd(self._index_key(doc_id), {peer.site: peer.last_seen})
            # Belt and braces: if every peer vanishes without a leave, the keys
            # themselves expire rather than lingering forever.
            pipe.expire(self._hash_key(doc_id), self._key_ttl)
            pipe.expire(self._index_key(doc_id), self._key_ttl)
            await pipe.execute()

    async def leave(self, doc_id: str, site: str) -> None:
        redis = await self._client()
        async with redis.pipeline(transaction=False) as pipe:
            pipe.hdel(self._hash_key(doc_id), site)
            pipe.zrem(self._index_key(doc_id), site)
            await pipe.execute()

    async def touch(self, doc_id: str, site: str, anchor: str | None, head: str | None) -> None:
        redis = await self._client()
        raw = await redis.hget(self._hash_key(doc_id), site)
        if raw is None:
            return
        data = orjson.loads(raw)
        data["anchor"] = anchor
        data["head"] = head
        data["last_seen"] = time.time()
        async with redis.pipeline(transaction=False) as pipe:
            pipe.hset(self._hash_key(doc_id), site, orjson.dumps(data))
            pipe.zadd(self._index_key(doc_id), {site: data["last_seen"]})
            pipe.expire(self._hash_key(doc_id), self._key_ttl)
            pipe.expire(self._index_key(doc_id), self._key_ttl)
            await pipe.execute()

    async def peers(self, doc_id: str) -> list[Peer]:
        redis = await self._client()
        cutoff = time.time() - self._ttl

        expired = await redis.zrangebyscore(self._index_key(doc_id), "-inf", cutoff)
        if expired:
            sites = [s.decode() if isinstance(s, bytes) else s for s in expired]
            async with redis.pipeline(transaction=False) as pipe:
                pipe.hdel(self._hash_key(doc_id), *sites)
                pipe.zrem(self._index_key(doc_id), *sites)
                await pipe.execute()

        raw_peers = await redis.hgetall(self._hash_key(doc_id))
        out: list[Peer] = []
        for payload in raw_peers.values():
            try:
                out.append(Peer(**orjson.loads(payload)))
            except (orjson.JSONDecodeError, TypeError):
                log.warning("dropping unreadable presence entry in doc=%s", doc_id)
        return out

    async def close(self) -> None:
        if self._redis is not None:
            import contextlib

            with contextlib.suppress(Exception):
                await self._redis.aclose()
            self._redis = None


def make_presence(settings: Settings) -> PresenceStore:
    if settings.redis_url:
        return RedisPresence(settings.redis_url, settings.presence_ttl_s)
    return MemoryPresence(settings.presence_ttl_s)

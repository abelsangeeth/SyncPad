"""Cross-replica fan-out — the reason SyncPad scales sideways.

A single-process editor is easy: one dict of sockets, broadcast to all of them.
The moment there are two replicas behind a load balancer, two people editing the
same document may be connected to different processes, and a broadcast that only
reaches local sockets silently splits the document in half.

The usual fix is sticky sessions: pin each document to one replica. That trades
the problem for a worse one — now the load balancer has state, a replica restart
disconnects every editor it owned, and a hot document cannot be spread across
machines. SyncPad instead sends every operation through Redis Pub/Sub, so any
replica can serve any socket for any document and the load balancer can stay
dumb (see ``deploy/nginx.conf``, which deliberately uses plain round-robin).

Two implementations behind one Protocol:

* :class:`RedisHub` for production.
* :class:`InMemoryHub` for tests and single-process dev, where Redis would be
  pure ceremony. It shares a process-level broker registry so two instances with
  the same ``broker`` genuinely talk to each other — which lets the test suite
  simulate two replicas without either Redis or a second process.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

from syncpad.config import Settings
from syncpad.crdt import new_site_id

__all__ = ["Hub", "InMemoryHub", "RedisHub", "SubToken", "channel_for", "make_hub", "redis_client"]

log = logging.getLogger("syncpad.hub")

Handler = Callable[[bytes], Awaitable[None]]


def channel_for(doc_id: str) -> str:
    return f"syncpad:doc:{doc_id}"


def redis_client(url: str):
    """Build the Redis client.

    A named seam rather than an inline import: it lets the test suite substitute
    an in-process fake and exercise the real :class:`RedisHub` logic — the
    single-connection multiplexing, the channel bookkeeping, the reconnect path
    — without a Redis server. Those are the parts most likely to be wrong, and
    they would otherwise only ever run in CI.
    """
    from redis.asyncio import Redis

    return Redis.from_url(url, decode_responses=False)


class SubToken:
    """Opaque handle returned by ``subscribe`` and passed back to unsubscribe."""

    __slots__ = ("doc_id", "handler")

    def __init__(self, doc_id: str, handler: Handler) -> None:
        self.doc_id = doc_id
        self.handler = handler


@runtime_checkable
class Hub(Protocol):
    """Publish/subscribe for document traffic, scoped to one document each."""

    gateway_id: str

    async def start(self) -> None: ...
    async def close(self) -> None: ...
    async def publish(self, doc_id: str, payload: bytes) -> None: ...
    async def subscribe(self, doc_id: str, handler: Handler) -> SubToken: ...
    async def unsubscribe(self, token: SubToken) -> None: ...


# --------------------------------------------------------------- in-process


#: Process-global brokers, keyed by name. Two InMemoryHub instances sharing a
#: broker behave like two replicas sharing a Redis — which is exactly what the
#: cross-replica tests need.
_BROKERS: dict[str, dict[str, set[Handler]]] = defaultdict(lambda: defaultdict(set))


class InMemoryHub:
    """Single-process fan-out. No Redis, same semantics."""

    def __init__(self, broker: str = "default") -> None:
        self.gateway_id = new_site_id()
        self._broker = broker
        self._own: set[SubToken] = set()
        self._closed = False

    @property
    def _channels(self) -> dict[str, set[Handler]]:
        return _BROKERS[self._broker]

    async def start(self) -> None:
        self._closed = False

    async def close(self) -> None:
        self._closed = True
        for token in list(self._own):
            await self.unsubscribe(token)

    async def publish(self, doc_id: str, payload: bytes) -> None:
        if self._closed:
            return
        # Snapshot the handler set: a handler may subscribe or unsubscribe while
        # we are delivering, and mutating during iteration would raise.
        for handler in list(self._channels.get(doc_id, ())):
            try:
                await handler(payload)
            except Exception:
                log.exception("in-memory subscriber failed for doc=%s", doc_id)

    async def subscribe(self, doc_id: str, handler: Handler) -> SubToken:
        token = SubToken(doc_id, handler)
        self._channels[doc_id].add(handler)
        self._own.add(token)
        return token

    async def unsubscribe(self, token: SubToken) -> None:
        self._channels.get(token.doc_id, set()).discard(token.handler)
        if not self._channels.get(token.doc_id):
            self._channels.pop(token.doc_id, None)
        self._own.discard(token)

    @staticmethod
    def reset_brokers() -> None:
        """Drop all broker state. Test hygiene only."""
        _BROKERS.clear()


# -------------------------------------------------------------------- redis


class RedisHub:
    """Redis Pub/Sub fan-out with one connection and one reader task per process.

    The connection count is the whole point. A naive implementation subscribes
    per document, so a replica holding a thousand documents holds a thousand
    Redis connections and falls over well before the application does. Here a
    single pubsub connection multiplexes every channel and one reader task
    dispatches to per-document handler sets, so connection use is O(replicas)
    rather than O(replicas x documents).
    """

    def __init__(self, url: str, *, reconnect_delay: float = 1.0) -> None:
        self.gateway_id = new_site_id()
        self._url = url
        self._reconnect_delay = reconnect_delay
        self._redis = None
        self._pubsub = None
        self._reader: asyncio.Task[None] | None = None
        self._handlers: dict[str, set[Handler]] = defaultdict(set)
        self._lock = asyncio.Lock()
        self._closed = False
        # redis-py refuses to read from a pubsub connection that has not
        # subscribed to anything, so the reader parks here until the first
        # document opens. Without it a replica holding no documents — which is
        # every replica at startup — spins on that error and never settles.
        self._subscribed = asyncio.Event()

    async def start(self) -> None:
        self._closed = False
        self._redis = redis_client(self._url)
        await self._redis.ping()
        self._pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
        self._reader = asyncio.create_task(self._read_loop(), name="syncpad-hub-reader")
        log.info("redis hub connected gateway_id=%s", self.gateway_id)

    async def close(self) -> None:
        self._closed = True
        if self._reader is not None:
            self._reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader
            self._reader = None
        if self._pubsub is not None:
            with contextlib.suppress(Exception):
                await self._pubsub.aclose()
            self._pubsub = None
        if self._redis is not None:
            with contextlib.suppress(Exception):
                await self._redis.aclose()
            self._redis = None

    async def publish(self, doc_id: str, payload: bytes) -> None:
        if self._redis is None or self._closed:
            raise RuntimeError("hub is not started")
        await self._redis.publish(channel_for(doc_id), payload)

    async def subscribe(self, doc_id: str, handler: Handler) -> SubToken:
        async with self._lock:
            first = not self._handlers[doc_id]
            self._handlers[doc_id].add(handler)
            if first and self._pubsub is not None:
                await self._pubsub.subscribe(channel_for(doc_id))
            self._subscribed.set()
        return SubToken(doc_id, handler)

    async def unsubscribe(self, token: SubToken) -> None:
        async with self._lock:
            handlers = self._handlers.get(token.doc_id)
            if handlers is None:
                return
            handlers.discard(token.handler)
            if not handlers:
                self._handlers.pop(token.doc_id, None)
                if self._pubsub is not None:
                    with contextlib.suppress(Exception):
                        await self._pubsub.unsubscribe(channel_for(token.doc_id))
            if not self._handlers:
                self._subscribed.clear()

    async def _read_loop(self) -> None:
        """Dispatch inbound messages, re-subscribing across connection drops.

        A Redis blip must not silently orphan every open document: on reconnect
        the loop re-subscribes to every channel this process still has handlers
        for, so fan-out resumes without anyone having to reload a page.
        """
        while not self._closed:
            try:
                if self._pubsub is None:
                    await asyncio.sleep(self._reconnect_delay)
                    continue
                if not self._handlers:
                    # Nothing subscribed: wait rather than poll a connection
                    # that would reject the read.
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self._subscribed.wait(), timeout=1.0)
                    continue
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if message is None:
                    continue
                channel = message.get("channel")
                if isinstance(channel, bytes):
                    channel = channel.decode("utf-8")
                doc_id = str(channel).removeprefix("syncpad:doc:")
                payload = message.get("data")
                if not isinstance(payload, bytes):
                    continue
                for handler in list(self._handlers.get(doc_id, ())):
                    try:
                        await handler(payload)
                    except Exception:
                        log.exception("subscriber failed for doc=%s", doc_id)
            except asyncio.CancelledError:
                raise
            except RuntimeError:
                # Raised when the last channel unsubscribed between the check
                # above and the read. Benign: loop round and wait again.
                if self._closed:
                    return
                await asyncio.sleep(0.05)
            except Exception:
                if self._closed:
                    return
                log.warning("redis hub reader failed; reconnecting", exc_info=True)
                await asyncio.sleep(self._reconnect_delay)
                with contextlib.suppress(Exception):
                    await self._resubscribe_all()

    async def _resubscribe_all(self) -> None:
        with contextlib.suppress(Exception):
            if self._pubsub is not None:
                await self._pubsub.aclose()
        if self._redis is not None:
            with contextlib.suppress(Exception):
                await self._redis.aclose()

        self._redis = redis_client(self._url)
        await self._redis.ping()
        self._pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
        async with self._lock:
            channels = [channel_for(doc) for doc in self._handlers]
        if channels:
            await self._pubsub.subscribe(*channels)
        log.info("redis hub re-subscribed to %d channels", len(channels))


def make_hub(settings: Settings) -> Hub:
    """Redis when configured, in-process otherwise. The only place this is decided."""
    if settings.redis_url:
        return RedisHub(settings.redis_url)
    return InMemoryHub()

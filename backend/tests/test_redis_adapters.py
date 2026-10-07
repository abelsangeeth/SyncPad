"""The Redis adapters, exercised against an in-process fake.

These cover the production code paths that would otherwise only ever run in CI:
the single multiplexed pubsub connection, per-document channel bookkeeping, the
reconnect-and-resubscribe loop, and presence expiry by sorted-set sweep.

The fake is a real Redis protocol implementation in memory, so the assertions
are about SyncPad's logic rather than about a mock's configured return values.
Two clients sharing one ``FakeServer`` behave like two replicas sharing one
Redis, which is what makes the cross-replica assertions meaningful.
"""

from __future__ import annotations

import asyncio

import pytest

from syncpad.gateway import hub as hub_module
from syncpad.gateway.hub import RedisHub
from syncpad.gateway.presence import Peer, RedisPresence

fakeredis = pytest.importorskip("fakeredis", reason="fakeredis is not installed")


@pytest.fixture
def shared_redis(monkeypatch):
    """Patch the client factory so every client shares one fake server."""
    from fakeredis import FakeServer
    from fakeredis.aioredis import FakeRedis

    server = FakeServer()
    monkeypatch.setattr(
        hub_module, "redis_client", lambda url: FakeRedis(server=server, decode_responses=False)
    )
    return server


async def _drain(queue: asyncio.Queue, timeout: float = 2.0) -> bytes:
    return await asyncio.wait_for(queue.get(), timeout=timeout)


# ------------------------------------------------------------------- the hub


async def test_redis_hub_delivers_to_a_subscriber(shared_redis) -> None:
    hub = RedisHub("redis://fake")
    await hub.start()
    try:
        received: asyncio.Queue[bytes] = asyncio.Queue()
        await hub.subscribe("doc-1", received.put)
        await asyncio.sleep(0.05)

        await hub.publish("doc-1", b"payload")
        assert await _drain(received) == b"payload"
    finally:
        await hub.close()


async def test_redis_hub_crosses_replicas(shared_redis) -> None:
    """The guarantee the architecture rests on, over the real Redis code path."""
    replica_a, replica_b = RedisHub("redis://fake"), RedisHub("redis://fake")
    await replica_a.start()
    await replica_b.start()
    assert replica_a.gateway_id != replica_b.gateway_id
    try:
        on_b: asyncio.Queue[bytes] = asyncio.Queue()
        await replica_b.subscribe("doc-x", on_b.put)
        await asyncio.sleep(0.05)

        await replica_a.publish("doc-x", b"from-replica-a")
        assert await _drain(on_b) == b"from-replica-a"
    finally:
        await replica_a.close()
        await replica_b.close()


async def test_redis_hub_keeps_documents_separate(shared_redis) -> None:
    hub = RedisHub("redis://fake")
    await hub.start()
    try:
        doc_a: asyncio.Queue[bytes] = asyncio.Queue()
        doc_b: asyncio.Queue[bytes] = asyncio.Queue()
        await hub.subscribe("a", doc_a.put)
        await hub.subscribe("b", doc_b.put)
        await asyncio.sleep(0.05)

        await hub.publish("a", b"only-for-a")
        assert await _drain(doc_a) == b"only-for-a"
        assert doc_b.empty()
    finally:
        await hub.close()


async def test_redis_hub_multiplexes_one_connection(shared_redis) -> None:
    """Many documents, one pubsub connection — the scaling property."""
    hub = RedisHub("redis://fake")
    await hub.start()
    try:
        queues = {}
        for i in range(25):
            queue: asyncio.Queue[bytes] = asyncio.Queue()
            queues[f"doc-{i}"] = queue
            await hub.subscribe(f"doc-{i}", queue.put)
        await asyncio.sleep(0.1)

        assert hub._pubsub is not None, "a single pubsub connection serves them all"
        assert len(hub._handlers) == 25

        await hub.publish("doc-7", b"seven")
        assert await _drain(queues["doc-7"]) == b"seven"
    finally:
        await hub.close()


async def test_redis_hub_unsubscribes_the_last_handler(shared_redis) -> None:
    hub = RedisHub("redis://fake")
    await hub.start()
    try:
        first: asyncio.Queue[bytes] = asyncio.Queue()
        second: asyncio.Queue[bytes] = asyncio.Queue()
        token_one = await hub.subscribe("doc", first.put)
        token_two = await hub.subscribe("doc", second.put)
        await asyncio.sleep(0.05)

        await hub.unsubscribe(token_one)
        assert "doc" in hub._handlers, "one handler remains, keep the subscription"

        await hub.unsubscribe(token_two)
        assert "doc" not in hub._handlers, "last handler gone, drop the channel"

        await hub.unsubscribe(token_two)  # idempotent
    finally:
        await hub.close()


async def test_redis_hub_survives_a_failing_subscriber(shared_redis) -> None:
    hub = RedisHub("redis://fake")
    await hub.start()
    try:
        survived: asyncio.Queue[bytes] = asyncio.Queue()

        async def explode(_payload: bytes) -> None:
            raise RuntimeError("broken socket")

        await hub.subscribe("doc", explode)
        await hub.subscribe("doc", survived.put)
        await asyncio.sleep(0.05)

        await hub.publish("doc", b"still-delivered")
        assert await _drain(survived) == b"still-delivered"
    finally:
        await hub.close()


async def test_redis_hub_resubscribes_after_a_connection_drop(shared_redis) -> None:
    """A Redis blip must not silently orphan every open document."""
    hub = RedisHub("redis://fake", reconnect_delay=0.01)
    await hub.start()
    try:
        received: asyncio.Queue[bytes] = asyncio.Queue()
        await hub.subscribe("doc-reconnect", received.put)
        await asyncio.sleep(0.05)

        await hub._resubscribe_all()
        await asyncio.sleep(0.1)

        await hub.publish("doc-reconnect", b"after-reconnect")
        assert await _drain(received) == b"after-reconnect"
    finally:
        await hub.close()


async def test_redis_hub_close_is_idempotent(shared_redis) -> None:
    hub = RedisHub("redis://fake")
    await hub.start()
    await hub.close()
    await hub.close()
    with pytest.raises(RuntimeError):
        await hub.publish("doc", b"x")


# -------------------------------------------------------------- the presence


def _peer(site: str, name: str = "Ada") -> Peer:
    return Peer(site=site, user_id=f"u-{site}", name=name, color="#6366f1")


async def test_presence_join_and_read(shared_redis) -> None:
    presence = RedisPresence("redis://fake", ttl=30.0)
    try:
        await presence.join("doc", _peer("site-a"))
        peers = await presence.peers("doc")
        assert [p.site for p in peers] == ["site-a"]
        assert peers[0].name == "Ada"
    finally:
        await presence.close()


async def test_presence_is_shared_between_replicas(shared_redis) -> None:
    """Two replicas must see one another's collaborators."""
    replica_a = RedisPresence("redis://fake", ttl=30.0)
    replica_b = RedisPresence("redis://fake", ttl=30.0)
    try:
        await replica_a.join("doc", _peer("from-a", "Alice"))
        await replica_b.join("doc", _peer("from-b", "Bob"))

        seen_by_a = {p.name for p in await replica_a.peers("doc")}
        seen_by_b = {p.name for p in await replica_b.peers("doc")}
        assert seen_by_a == seen_by_b == {"Alice", "Bob"}
    finally:
        await replica_a.close()
        await replica_b.close()


async def test_presence_leave_removes_a_peer(shared_redis) -> None:
    presence = RedisPresence("redis://fake", ttl=30.0)
    try:
        await presence.join("doc", _peer("site-a"))
        await presence.join("doc", _peer("site-b"))
        await presence.leave("doc", "site-a")
        assert [p.site for p in await presence.peers("doc")] == ["site-b"]
    finally:
        await presence.close()


async def test_presence_touch_updates_the_cursor(shared_redis) -> None:
    presence = RedisPresence("redis://fake", ttl=30.0)
    try:
        await presence.join("doc", _peer("site-a"))
        await presence.touch("doc", "site-a", "a:1", "a:5")

        peer = (await presence.peers("doc"))[0]
        assert (peer.anchor, peer.head) == ("a:1", "a:5")
    finally:
        await presence.close()


async def test_touching_an_absent_peer_is_a_noop(shared_redis) -> None:
    presence = RedisPresence("redis://fake", ttl=30.0)
    try:
        await presence.touch("doc", "never-joined", "a:1", "a:1")
        assert await presence.peers("doc") == []
    finally:
        await presence.close()


async def test_stale_peers_are_swept_on_read(shared_redis) -> None:
    """The ghost-collaborator guarantee: no clean disconnect required."""
    presence = RedisPresence("redis://fake", ttl=0.05)
    try:
        await presence.join("doc", _peer("vanished"))
        assert len(await presence.peers("doc")) == 1

        await asyncio.sleep(0.12)
        assert await presence.peers("doc") == [], "an idle peer must expire by itself"
    finally:
        await presence.close()


async def test_unreadable_presence_entries_are_dropped_not_fatal(shared_redis) -> None:
    """A bad entry from an older version must not break the whole sidebar."""
    presence = RedisPresence("redis://fake", ttl=30.0)
    try:
        await presence.join("doc", _peer("good"))
        redis = await presence._client()
        await redis.hset("syncpad:presence:doc", "corrupt", b"{not json")

        peers = await presence.peers("doc")
        assert [p.site for p in peers] == ["good"]
    finally:
        await presence.close()


async def test_presence_close_is_idempotent(shared_redis) -> None:
    presence = RedisPresence("redis://fake")
    await presence.join("doc", _peer("a"))
    await presence.close()
    await presence.close()


async def test_empty_document_has_no_peers(shared_redis) -> None:
    presence = RedisPresence("redis://fake")
    try:
        assert await presence.peers("never-opened") == []
    finally:
        await presence.close()


async def test_reader_idles_quietly_before_any_subscription(shared_redis) -> None:
    """Regression: a replica holding no documents must not spin on an error.

    redis-py rejects a read on a pubsub connection that has not subscribed to
    anything. The reader used to call it unconditionally, so a freshly started
    replica — which by definition has no documents open yet — would raise,
    tear down its connection, reconnect, and raise again, several times a
    second, forever. It never affected correctness, which is what made it easy
    to miss: the replica worked, it just burned a core and filled the log.

    fakeredis tolerates the call, so this asserts on the observable symptom:
    the reader survives an idle period and still works afterwards.
    """
    hub = RedisHub("redis://fake", reconnect_delay=0.01)
    await hub.start()
    try:
        await asyncio.sleep(0.3)  # idle, with nothing subscribed
        assert hub._reader is not None and not hub._reader.done(), "the reader died while idle"

        received: asyncio.Queue[bytes] = asyncio.Queue()
        token = await hub.subscribe("late-doc", received.put)
        await asyncio.sleep(0.05)
        await hub.publish("late-doc", b"works-after-idling")
        assert await _drain(received) == b"works-after-idling"

        # And back to idle: unsubscribing the last handler must not spin either.
        await hub.unsubscribe(token)
        await asyncio.sleep(0.2)
        assert not hub._reader.done(), "the reader died after the last unsubscribe"
    finally:
        await hub.close()

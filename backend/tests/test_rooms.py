"""Room lifecycle, and the claim that a room is a cache rather than an owner.

The decisive test here builds *two independent RoomManagers* over one shared
hub and one shared database — two replicas, in everything but the process
boundary — and has them edit the same document. If rooms implied ownership, or
if fan-out were local, they would diverge. They must not.
"""

from __future__ import annotations

import asyncio

import pytest

from syncpad.crdt import RGA
from syncpad.gateway.hub import InMemoryHub
from syncpad.gateway.presence import MemoryPresence
from syncpad.services.rooms import RoomManager


@pytest.fixture(autouse=True)
def clean_brokers():
    InMemoryHub.reset_brokers()
    yield
    InMemoryHub.reset_brokers()


async def make_manager(
    broker: str, snapshots, settings, repo_context
) -> tuple[RoomManager, InMemoryHub]:
    hub = InMemoryHub(broker)
    await hub.start()
    manager = RoomManager(
        hub=hub,
        presence=MemoryPresence(settings.presence_ttl_s),
        snapshots=snapshots,
        settings=settings,
        repo_context=repo_context,
    )
    return manager, hub


async def test_acquire_cold_loads_and_refcounts(snapshots, settings, repo_context, document):
    manager, hub = await make_manager("t1", snapshots, settings, repo_context)
    try:
        first = await manager.acquire(document.id)
        assert manager.active == 1
        assert first.refcount == 1

        second = await manager.acquire(document.id)
        assert second is first, "the same document must share one in-memory replica"
        assert first.refcount == 2
        assert manager.active == 1

        await manager.release(document.id)
        assert manager.active == 1, "still one socket attached"
        await manager.release(document.id)
        assert manager.active == 0, "the last release closes the room"
    finally:
        await manager.shutdown()
        await hub.close()


async def test_releasing_an_unknown_room_is_harmless(snapshots, settings, repo_context):
    manager, hub = await make_manager("t2", snapshots, settings, repo_context)
    try:
        await manager.release("never-opened")
    finally:
        await manager.shutdown()
        await hub.close()


async def test_submit_applies_persists_and_sequences(snapshots, settings, repo_context, document):
    manager, hub = await make_manager("t3", snapshots, settings, repo_context)
    try:
        room = await manager.acquire(document.id)
        author = RGA("author")

        seq = await room.submit(author.local_insert(0, "hello"), site="author", actor_id=None)
        assert seq == 1
        assert room.rga.text() == "hello"

        seq2 = await room.submit(author.local_insert(5, " world"), site="author", actor_id=None)
        assert seq2 == 2
        assert room.rga.text() == "hello world"

        # And it is durable, not just in memory.
        async with repo_context() as repo:
            assert await repo.op_count(document.id) == 2
    finally:
        await manager.shutdown()
        await hub.close()


async def test_submitting_nothing_is_a_noop(snapshots, settings, repo_context, document):
    manager, hub = await make_manager("t4", snapshots, settings, repo_context)
    try:
        room = await manager.acquire(document.id)
        assert await room.submit([], site="x", actor_id=None) == room.seq
    finally:
        await manager.shutdown()
        await hub.close()


async def test_two_replicas_converge_through_the_hub(snapshots, settings, repo_context, document):
    """Two RoomManagers, one document, no affinity — the scaling claim."""
    replica_a, hub_a = await make_manager("shared", snapshots, settings, repo_context)
    replica_b, hub_b = await make_manager("shared", snapshots, settings, repo_context)
    try:
        room_a = await replica_a.acquire(document.id)
        room_b = await replica_b.acquire(document.id)
        assert room_a is not room_b, "these are genuinely separate replicas"

        author = RGA("author-a")
        await room_a.submit(author.local_insert(0, "written on A"), site="author-a", actor_id=None)
        await asyncio.sleep(0.05)
        assert room_b.rga.text() == "written on A", "fan-out did not cross the replica"

        other = RGA("author-b")
        other.apply_many([])
        ops = room_b.rga.local_insert(len(room_b.rga), " and on B")
        await room_b.submit(ops, site=room_b.rga.site, actor_id=None)
        await asyncio.sleep(0.05)

        assert room_a.rga.text() == room_b.rga.text() == "written on A and on B"
    finally:
        await replica_a.shutdown()
        await replica_b.shutdown()
        await hub_a.close()
        await hub_b.close()


async def test_a_replica_joining_late_catches_up_from_storage(
    snapshots, settings, repo_context, document
):
    """A replica that was not running when the edits happened still sees them."""
    replica_a, hub_a = await make_manager("late", snapshots, settings, repo_context)
    room_a = await replica_a.acquire(document.id)
    author = RGA("early")
    await room_a.submit(
        author.local_insert(0, "edited before B existed"), site="early", actor_id=None
    )

    replica_b, hub_b = await make_manager("late", snapshots, settings, repo_context)
    try:
        room_b = await replica_b.acquire(document.id)
        assert room_b.rga.text() == "edited before B existed"
        assert room_b.seq == room_a.seq
    finally:
        await replica_a.shutdown()
        await replica_b.shutdown()
        await hub_a.close()
        await hub_b.close()


async def test_local_sockets_receive_broadcasts(snapshots, settings, repo_context, document):
    manager, hub = await make_manager("t5", snapshots, settings, repo_context)
    try:
        room = await manager.acquire(document.id)
        received: list[bytes] = []
        other: list[bytes] = []

        async def collect(payload: bytes) -> None:
            received.append(payload)

        async def collect_other(payload: bytes) -> None:
            other.append(payload)

        room.attach("site-1", collect)
        room.attach("site-2", collect_other)
        assert room.local_sockets == 2

        author = RGA("site-1")
        await room.submit(author.local_insert(0, "typed"), site="site-1", actor_id=None)

        assert received == [], "the sender must not be echoed its own operations"
        assert len(other) == 1, "every other local socket must receive them"

        room.detach("site-1")
        assert room.local_sockets == 1
    finally:
        await manager.shutdown()
        await hub.close()


async def test_a_dead_socket_does_not_break_the_broadcast(
    snapshots, settings, repo_context, document
):
    manager, hub = await make_manager("t6", snapshots, settings, repo_context)
    try:
        room = await manager.acquire(document.id)
        delivered: list[bytes] = []

        async def explode(_payload: bytes) -> None:
            raise ConnectionError("socket is gone")

        async def collect(payload: bytes) -> None:
            delivered.append(payload)

        room.attach("dead", explode)
        room.attach("alive", collect)

        author = RGA("author")
        await room.submit(author.local_insert(0, "x"), site="author", actor_id=None)
        assert len(delivered) == 1, "one dead socket must not silence the room"
    finally:
        await manager.shutdown()
        await hub.close()


async def test_malformed_hub_payloads_are_ignored(snapshots, settings, repo_context, document):
    """Another replica on a different version must not be able to crash this one."""
    manager, hub = await make_manager("t7", snapshots, settings, repo_context)
    try:
        room = await manager.acquire(document.id)
        before = room.rga.text()

        await room._on_hub_message(b"not json at all")
        await room._on_hub_message(b'{"gw":"other","ops":[{"k":"garbage"}]}')
        await room._on_hub_message(b'{"gw":"other","ops":"not a list"}')

        assert room.rga.text() == before
    finally:
        await manager.shutdown()
        await hub.close()


async def test_a_replicas_own_publication_is_not_applied_twice(
    snapshots, settings, repo_context, document
):
    manager, hub = await make_manager("t8", snapshots, settings, repo_context)
    try:
        room = await manager.acquire(document.id)
        author = RGA("author")
        await room.submit(author.local_insert(0, "once"), site="author", actor_id=None)
        assert room.rga.text() == "once"
        assert room.rga.elements == 4, "the loop-back must be recognised and dropped"
    finally:
        await manager.shutdown()
        await hub.close()


async def test_closing_a_room_banks_a_snapshot(snapshots, settings, repo_context, document):
    """Otherwise the next reader repeats replay work this replica already did."""
    manager, hub = await make_manager("t9", snapshots, settings, repo_context)
    try:
        room = await manager.acquire(document.id)
        author = RGA("author")
        await room.submit(author.local_insert(0, "worth keeping"), site="author", actor_id=None)
        await manager.release(document.id)

        async with repo_context() as repo:
            assert await repo.latest_snapshot(document.id) is not None
    finally:
        await manager.shutdown()
        await hub.close()


async def test_shutdown_closes_every_room(snapshots, settings, repo_context, document):
    manager, hub = await make_manager("t10", snapshots, settings, repo_context)
    try:
        await manager.acquire(document.id)
        assert manager.active == 1
        await manager.shutdown()
        assert manager.active == 0

        with pytest.raises(RuntimeError, match="shutting down"):
            await manager.acquire(document.id)
    finally:
        await hub.close()


async def test_stats_describe_live_rooms(snapshots, settings, repo_context, document):
    manager, hub = await make_manager("t11", snapshots, settings, repo_context)
    try:
        room = await manager.acquire(document.id)
        author = RGA("author")
        await room.submit(author.local_insert(0, "abcdef"), site="author", actor_id=None)
        await room.submit(author.local_delete(0, 2), site="author", actor_id=None)

        stats = manager.stats()
        assert stats["rooms"] == 1
        assert stats["documents"][0]["chars"] == 4
        assert stats["documents"][0]["tombstones"] == 2
    finally:
        await manager.shutdown()
        await hub.close()


async def test_get_returns_none_for_a_document_this_replica_lacks(
    snapshots, settings, repo_context, document
):
    manager, hub = await make_manager("t12", snapshots, settings, repo_context)
    try:
        assert manager.get(document.id) is None
        await manager.acquire(document.id)
        assert manager.get(document.id) is not None
    finally:
        await manager.shutdown()
        await hub.close()


async def test_concurrent_acquires_cold_load_once(snapshots, settings, repo_context, document):
    """A burst of connections to a cold document must not replay it N times."""
    manager, hub = await make_manager("t13", snapshots, settings, repo_context)
    try:
        rooms = await asyncio.gather(*(manager.acquire(document.id) for _ in range(8)))
        assert len({id(r) for r in rooms}) == 1
        assert manager.active == 1
        assert rooms[0].refcount == 8
    finally:
        await manager.shutdown()
        await hub.close()

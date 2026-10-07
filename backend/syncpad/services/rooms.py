"""Rooms: a per-process cache of a document, not ownership of it.

This is the distinction the whole scaling story rests on. A :class:`Room` holds
a live :class:`~syncpad.crdt.RGA` so that operations arriving on this replica can
be applied and broadcast without a database round-trip. It does **not** mean this
replica owns the document. Three replicas may each hold a room for the same
document simultaneously, and that is fine — correctness comes from the CRDT and
the durable sequence in Postgres, never from affinity.

The flow for a locally submitted batch is strictly:

1. apply to the in-memory replica (so the sender sees it immediately),
2. persist to the append-only log, which allocates the sequence,
3. publish to the hub for every other replica,
4. broadcast to local sockets.

Persisting before publishing is deliberate. If the process dies between steps,
the worst case is an operation that was durably recorded but not yet broadcast —
which the next reader picks up from the log. The reverse order would allow an
operation that everyone has seen but nobody has stored.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import orjson

from syncpad.config import Settings
from syncpad.crdt import RGA, Op, ops_from_dicts, ops_to_dicts
from syncpad.gateway.hub import Hub, SubToken
from syncpad.gateway.presence import PresenceStore
from syncpad.storage.snapshots import SnapshotService

__all__ = ["Room", "RoomManager"]

log = logging.getLogger("syncpad.rooms")

LocalSender = Callable[[bytes], Awaitable[None]]


class Room:
    """One document, live in this process."""

    __slots__ = (
        "_hub",
        "_lock",
        "_repo_context",
        "_settings",
        "_snapshot_task",
        "_snapshots",
        "_sockets",
        "_token",
        "doc_id",
        "refcount",
        "rga",
        "seq",
        "snapshot_seq",
    )

    def __init__(
        self,
        doc_id: str,
        rga: RGA,
        seq: int,
        *,
        snapshot_seq: int = 0,
        hub: Hub,
        snapshots: SnapshotService,
        settings: Settings,
        repo_context,
    ) -> None:
        self.doc_id = doc_id
        self.rga = rga
        self.seq = seq
        # Cadence is tracked in memory. Asking the database "is it time for a
        # snapshot yet?" on every keystroke doubles the write path's round trips
        # to answer a question this process already knows the answer to.
        self.snapshot_seq = snapshot_seq
        self._snapshot_task: asyncio.Task[None] | None = None
        self._hub = hub
        self._snapshots = snapshots
        self._settings = settings
        self._repo_context = repo_context
        self._sockets: dict[str, LocalSender] = {}
        self._token: SubToken | None = None
        # Serialises submit() so two sockets on this replica cannot interleave
        # apply/persist/publish and emit operations out of sequence order.
        self._lock = asyncio.Lock()
        self.refcount = 0

    # ------------------------------------------------------------- sockets
    def attach(self, site: str, send: LocalSender) -> None:
        self._sockets[site] = send

    def detach(self, site: str) -> None:
        self._sockets.pop(site, None)

    @property
    def local_sockets(self) -> int:
        return len(self._sockets)

    # ------------------------------------------------------------- editing
    async def submit(self, ops: list[Op], *, site: str, actor_id: str | None) -> int:
        """Apply, persist, then fan out. Returns the allocated sequence."""
        if not ops:
            return self.seq

        async with self._lock:
            self.rga.apply_many(ops)

            async with self._repo_context() as repo:
                seq = await repo.append_ops(self.doc_id, ops, site=site, actor_id=actor_id)
            self.seq = max(self.seq, seq)

            payload = orjson.dumps(
                {
                    "gw": self._hub.gateway_id,
                    "doc": self.doc_id,
                    "seq": seq,
                    "site": site,
                    "ops": ops_to_dicts(ops),
                }
            )

        # Outside the lock: fan-out is I/O and must not block the next submit.
        await self._hub.publish(self.doc_id, payload)
        await self.broadcast_local(_ops_frame(seq=seq, ops=ops, origin=site), exclude=site)

        self._consider_snapshot(seq)
        return seq

    def _consider_snapshot(self, seq: int) -> None:
        """Schedule a snapshot if one is due, without blocking this edit.

        Serialising a large document takes milliseconds that would otherwise be
        charged to whichever unlucky keystroke crossed the threshold — a visible
        stall for one user, at a moment unrelated to anything they did. It runs
        as a background task instead. Snapshots are a cache, so a failed or
        skipped one costs cold-load time and never correctness.
        """
        interval = self._settings.snapshot_every_ops
        if interval <= 0 or seq - self.snapshot_seq < interval:
            return
        if self._snapshot_task is not None and not self._snapshot_task.done():
            return

        self.snapshot_seq = seq

        async def write() -> None:
            try:
                await self._snapshots.maybe_snapshot(self.doc_id, self.rga, seq)
            except Exception:
                log.exception("snapshot failed for doc=%s seq=%s", self.doc_id, seq)

        self._snapshot_task = asyncio.create_task(
            write(), name=f"syncpad-snapshot-{self.doc_id[:8]}"
        )

    async def broadcast_local(self, payload: bytes, exclude: str | None = None) -> None:
        """Send to every socket on this replica except (optionally) one."""
        targets = [(s, send) for s, send in self._sockets.items() if s != exclude]
        if not targets:
            return
        # Decode once here rather than once inside every socket's writer.
        frame = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        results = await asyncio.gather(
            *(send(frame) for _, send in targets), return_exceptions=True
        )
        for (site, _), result in zip(targets, results, strict=True):
            if isinstance(result, Exception):
                # A dead socket is routine; its own handler will clean it up.
                log.debug("dropping frame to site=%s: %s", site, result)

    async def _on_hub_message(self, raw: bytes) -> None:
        """Apply an operation batch that originated on another replica."""
        try:
            message = orjson.loads(raw)
        except orjson.JSONDecodeError:
            log.warning("undecodable hub payload for doc=%s", self.doc_id)
            return

        if message.get("gw") == self._hub.gateway_id:
            # Our own publication looping back: already applied and broadcast.
            return

        try:
            ops = ops_from_dicts(message.get("ops", []))
        except Exception:
            log.warning("undecodable operations from hub for doc=%s", self.doc_id)
            return

        origin = str(message.get("site", ""))
        seq = int(message.get("seq", 0))

        async with self._lock:
            self.rga.apply_many(ops)
            self.seq = max(self.seq, seq)

        await self.broadcast_local(_ops_frame(seq=seq, ops=ops, origin=origin))

    async def relay(self, payload: bytes) -> None:
        """Publish a non-operation frame (presence, cursors) to other replicas."""
        await self._hub.publish(self.doc_id, payload)


def _ops_frame(*, seq: int, ops: list[Op], origin: str) -> bytes:
    return orjson.dumps({"t": "ops", "seq": seq, "ops": ops_to_dicts(ops), "origin": origin})


class RoomManager:
    """Lazily loads rooms, refcounts them, and tears them down when empty."""

    def __init__(
        self,
        *,
        hub: Hub,
        presence: PresenceStore,
        snapshots: SnapshotService,
        settings: Settings,
        repo_context,
    ) -> None:
        self._hub = hub
        self._presence = presence
        self._snapshots = snapshots
        self._settings = settings
        self._repo_context = repo_context
        self._rooms: dict[str, Room] = {}
        # One lock per document, so cold-loading a large document does not
        # serialise every other document's first connection behind it.
        self._locks: dict[str, asyncio.Lock] = {}
        self._closed = False

    @property
    def active(self) -> int:
        return len(self._rooms)

    def _lock_for(self, doc_id: str) -> asyncio.Lock:
        lock = self._locks.get(doc_id)
        if lock is None:
            lock = self._locks[doc_id] = asyncio.Lock()
        return lock

    async def acquire(self, doc_id: str) -> Room:
        """Get or cold-load the room for a document, incrementing its refcount."""
        if self._closed:
            raise RuntimeError("room manager is shutting down")

        async with self._lock_for(doc_id):
            room = self._rooms.get(doc_id)
            if room is None:
                rga, seq, stats = await self._snapshots.load(doc_id)
                room = Room(
                    doc_id,
                    rga,
                    seq,
                    snapshot_seq=stats.snapshot_seq,
                    hub=self._hub,
                    snapshots=self._snapshots,
                    settings=self._settings,
                    repo_context=self._repo_context,
                )
                room._token = await self._hub.subscribe(doc_id, room._on_hub_message)
                self._rooms[doc_id] = room
                log.info(
                    "room opened doc=%s seq=%s snapshot=%s replayed=%s",
                    doc_id,
                    seq,
                    stats.from_snapshot,
                    stats.batches_replayed,
                )
            room.refcount += 1
            return room

    async def release(self, doc_id: str) -> None:
        """Drop a reference; close the room when the last socket leaves."""
        async with self._lock_for(doc_id):
            room = self._rooms.get(doc_id)
            if room is None:
                return
            room.refcount -= 1
            if room.refcount > 0:
                return
            await self._close_room(room)
            self._rooms.pop(doc_id, None)
            self._locks.pop(doc_id, None)

    async def _close_room(self, room: Room) -> None:
        if room._snapshot_task is not None and not room._snapshot_task.done():
            with contextlib.suppress(Exception):
                await asyncio.wait_for(room._snapshot_task, timeout=5)
        if room._token is not None:
            with contextlib.suppress(Exception):
                await self._hub.unsubscribe(room._token)
        # Bank the replay work before discarding the in-memory replica, so the
        # next reader does not redo it.
        if room.seq > 0:
            try:
                await self._snapshots.force_snapshot(room.doc_id, room.rga, room.seq)
            except Exception:
                log.exception("closing snapshot failed for doc=%s", room.doc_id)
        log.info("room closed doc=%s seq=%s", room.doc_id, room.seq)

    def get(self, doc_id: str) -> Room | None:
        """The live room, if this replica currently holds one."""
        return self._rooms.get(doc_id)

    async def shutdown(self) -> None:
        self._closed = True
        for room in list(self._rooms.values()):
            await self._close_room(room)
        self._rooms.clear()
        self._locks.clear()

    def stats(self) -> dict[str, Any]:
        return {
            "rooms": len(self._rooms),
            "sockets": sum(r.local_sockets for r in self._rooms.values()),
            "documents": [
                {
                    "doc": r.doc_id,
                    "seq": r.seq,
                    "sockets": r.local_sockets,
                    "chars": len(r.rga),
                    "tombstones": r.rga.tombstones,
                }
                for r in self._rooms.values()
            ],
        }

"""The WebSocket endpoint: one connection's whole lifecycle.

Authentication happens in the first frame rather than in the URL, because query
strings end up in proxy logs and browser history and a JWT does not belong in
either. The socket is accepted first, then the ``hello`` frame is validated, and
a failure closes cleanly with a reason the client can display.

Everything after that is a loop with four jobs: apply and fan out operations,
relay cursors, answer pings, and refuse to be a denial-of-service vector. The
last one is why there is a frame-size cap, an operations-per-frame cap, a token
bucket, and an idle timeout — a collaborative editor is an unusually inviting
target because it accepts a high-frequency write stream by design.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import logging
import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from syncpad.api.auth import AuthError, decode_token
from syncpad.config import Settings
from syncpad.crdt import new_site_id
from syncpad.gateway import protocol as proto
from syncpad.gateway.presence import Peer
from syncpad.services.rate_limit import TokenBucket
from syncpad.storage import DocumentNotFound, DocumentRepo, can_edit

__all__ = ["router"]

log = logging.getLogger("syncpad.ws")

router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    """Serve one editing session.

    Any replica can serve any socket for any document: nothing in this function
    assumes this process has seen the document before, and nothing persists in
    this process that another replica could not reconstruct.
    """
    app = ws.app
    settings: Settings = app.state.settings
    await ws.accept()

    site = new_site_id()
    room = None
    doc_id = ""
    joined = False

    try:
        hello = await asyncio.wait_for(_receive_frame(ws), timeout=settings.ws_handshake_timeout_s)
        doc_id, token, share_token, since = proto.parse_hello(hello)

        user, role = await _authorise(app, doc_id, token, share_token)
        if role is None:
            await _fail(ws, proto.ErrorCode.FORBIDDEN, "no access to this document")
            return

        room = await app.state.rooms.acquire(doc_id)
        # Attach before the welcome is built, so operations committed while we
        # are serialising the snapshot are not lost — but hold them in a buffer,
        # because a client must never see a frame before its welcome.
        writer = _SocketWriter(ws)
        room.attach(site, writer.send)

        peer = Peer(
            site=site,
            user_id=user["id"] if user else None,
            name=user["name"] if user else "Guest",
            color=user["color"] if user else _guest_color(site),
        )
        await app.state.presence.join(doc_id, peer)
        joined = True

        await _send_welcome(ws, app, room, site, since, role, user)
        await writer.release()
        await _broadcast_presence(app, room)
        app.state.metrics.connection_opened()

        await _session_loop(ws, app, room, site, role, settings, writer)

    except WebSocketDisconnect:
        pass
    except TimeoutError:
        await _fail(ws, proto.ErrorCode.BAD_FRAME, "timed out waiting for hello")
    except proto.BadFrame as exc:
        await _fail(ws, exc.code, str(exc))
    except DocumentNotFound:
        await _fail(ws, proto.ErrorCode.NOT_FOUND, "document not found")
    except Exception:
        log.exception("websocket session failed doc=%s site=%s", doc_id, site)
        await _fail(ws, proto.ErrorCode.INTERNAL, "internal error")
    finally:
        # Cleanup runs as a shielded task rather than inline. A handler is
        # cancelled — not merely returned from — whenever the server is shutting
        # down or the ASGI host tears the connection down, and `CancelledError`
        # derives from BaseException, so an inline `suppress(Exception)` would
        # not catch it: the awaits below would simply stop part-way. The result
        # is a ghost collaborator stuck in everyone else's sidebar until the
        # presence TTL expires, and a room whose refcount never reaches zero.
        cleanup = asyncio.create_task(
            _teardown(app, room, doc_id, site, joined=joined),
            name=f"syncpad-teardown-{site[:8]}",
        )
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.shield(cleanup)
        with contextlib.suppress(Exception):
            if ws.client_state is not WebSocketState.DISCONNECTED:
                await ws.close()


async def _teardown(app, room, doc_id: str, site: str, *, joined: bool) -> None:
    """Leave presence, detach the socket, notify peers, release the room."""
    if joined:
        with contextlib.suppress(Exception):
            await app.state.presence.leave(doc_id, site)
    if room is None:
        return
    room.detach(site)
    try:
        await _broadcast_presence(app, room)
    except Exception:
        log.warning("could not notify peers of departure doc=%s", doc_id, exc_info=True)
    with contextlib.suppress(Exception):
        await app.state.rooms.release(doc_id)
    app.state.metrics.connection_closed()


class _SocketWriter:
    """Serialises and coalesces writes to one socket.

    Three jobs, all of which matter under load:

    * **welcome comes first.** The socket joins the room before the welcome is
      built — otherwise an operation committed while the snapshot is being
      serialised would reach neither — so anything broadcast in that window is
      held and flushed immediately afterwards.

    * **frames never interleave.** Writes go through one flush task, so two
      concurrent broadcasts cannot suspend into each other and splice their JSON.

    * **fan-out leaves the critical path.** ``send`` enqueues and returns
      immediately; the socket write happens on the next event-loop iteration.
      This matters more than it sounds. Fan-out is inherently N-squared — every
      operation from every writer is written to every other participant — so
      fifty editors typing produce thousands of socket writes per second, and
      making the submitting request wait for all of them turns one person's
      keystroke into everyone else's latency.

      Deferring by exactly one loop iteration also coalesces for free: when the
      loop is idle each frame is sent on its own with no added delay, and when
      it is busy everything that piled up during the tick leaves as a single
      ``batch`` frame. The batching appears precisely when it is needed and
      costs nothing when it is not.

    A client that cannot keep up is disconnected rather than buffered without
    limit. It reconnects with ``since`` and is caught up from the log, which is
    cheaper for everyone than an ever-growing queue in the gateway.
    """

    __slots__ = (
        "_flush_task",
        "_flushing",
        "_max_queue",
        "_open",
        "_pending",
        "_queue",
        "_ws",
        "overflowed",
    )

    def __init__(self, ws: WebSocket, max_queue: int = 2048) -> None:
        self._ws = ws
        self._pending: list[bytes | str] | None = []
        self._open = False
        self._queue: list[bytes | str] = []
        self._flushing = False
        # Held deliberately. asyncio keeps only a weak reference to a running
        # task, so a flush with no strong reference anywhere can be collected
        # mid-flight and the frames it was about to write vanish silently.
        self._flush_task: asyncio.Task[None] | None = None
        self._max_queue = max_queue
        self.overflowed = False

    async def send(self, payload: bytes | str) -> None:
        if not self._open:
            assert self._pending is not None
            self._pending.append(payload)
            return
        if len(self._queue) >= self._max_queue:
            # This socket is not draining. Mark it and stop buffering; the
            # session loop closes it and the client resyncs on reconnect.
            self.overflowed = True
            return
        self._queue.append(payload)
        if not self._flushing:
            self._flushing = True
            self._flush_task = asyncio.create_task(self._flush(), name="syncpad-flush")

    async def release(self) -> None:
        """Mark welcome as delivered and release anything queued behind it."""
        queued, self._pending, self._open = self._pending or [], None, True
        for payload in queued:
            await self.send(payload)

    async def _flush(self) -> None:
        # One yield: everything produced during this tick coalesces into it.
        await asyncio.sleep(0)
        self._flushing = False
        batch, self._queue = self._queue, []
        if not batch:
            return
        try:
            if len(batch) == 1:
                await _send(self._ws, batch[0])
                return
            # Concatenate frames that are already serialised JSON rather than
            # parsing and re-encoding them.
            parts = [f if isinstance(f, str) else f.decode("utf-8") for f in batch]
            await _send(self._ws, '{"t":"batch","frames":[' + ",".join(parts) + "]}")
        except Exception:
            log.debug("flush failed for a socket; it will be cleaned up")


async def _session_loop(ws, app, room, site, role, settings, writer) -> None:
    """Read frames until the client goes away or misbehaves."""
    bucket = TokenBucket(settings.rate_ops_per_s, settings.rate_burst)
    editable = can_edit(role)

    while True:
        try:
            frame = await asyncio.wait_for(_receive_frame(ws), timeout=settings.ws_idle_timeout_s)
        except TimeoutError:
            # The client has gone quiet. A ping proves the socket is still
            # there; a dead one raises here and unwinds to the finally block.
            await _send(ws, proto.encode_frame({"t": "ping"}))
            continue
        except proto.BadFrame as exc:
            await _send(ws, proto.error(exc.code, str(exc)))
            if exc.code == proto.ErrorCode.TOO_LARGE:
                continue
            continue

        if writer.overflowed:
            # The client stopped draining. Closing is kinder than an unbounded
            # queue: it reconnects with `since` and is caught up from the log.
            log.info("closing a socket that could not keep up doc=%s", room.doc_id)
            await _fail(ws, proto.ErrorCode.INTERNAL, "connection fell too far behind")
            return

        kind = frame["t"]

        if kind == "ops":
            if not editable:
                await _send(
                    ws,
                    proto.error(
                        proto.ErrorCode.FORBIDDEN,
                        f"role '{role}' may read but not edit this document",
                    ),
                )
                continue
            try:
                ops, cseq = proto.parse_ops(frame)
            except proto.BadFrame as exc:
                await _send(ws, proto.error(exc.code, str(exc)))
                continue

            if not bucket.allow(len(ops) or 1):
                await _send(
                    ws,
                    proto.error(
                        proto.ErrorCode.RATE_LIMITED,
                        f"slow down; retry in {bucket.retry_after():.1f}s",
                    ),
                )
                app.state.metrics.rate_limited()
                continue

            started = time.perf_counter()
            actor = frame.get("_actor")
            seq = await room.submit(ops, site=site, actor_id=actor)
            app.state.metrics.ops_applied(len(ops))
            app.state.metrics.observe_broadcast(time.perf_counter() - started)
            await _send(ws, proto.ack(cseq=cseq, seq=seq))

        elif kind == "cursor":
            try:
                anchor, head = proto.parse_cursor(frame)
            except proto.BadFrame as exc:
                await _send(ws, proto.error(exc.code, str(exc)))
                continue
            await app.state.presence.touch(room.doc_id, site, anchor, head)
            await _broadcast_presence(app, room)

        elif kind == "ping":
            await _send(ws, proto.pong())

        elif kind == "pong":
            continue

        else:
            await _send(ws, proto.error(proto.ErrorCode.BAD_FRAME, f"unknown frame type '{kind}'"))


async def _send_welcome(ws, app, room, site: str, since: int, role: str, user) -> None:
    """Send the opening frame: a snapshot, or just the missed operations.

    A reconnecting client that supplies ``since`` and is still within the
    retained log gets a delta. That is what makes a flaky connection cheap: a
    tunnel dropping every few minutes costs a handful of operations rather than
    a full document download each time.
    """
    snapshot_b64: str | None = None
    missed: list = []

    if since and since <= room.seq:
        async with app.state.repo_context() as repo:
            missed = await repo.ops_since(room.doc_id, since)
        # A delta is only a win while it is smaller than the document.
        if sum(len(batch) for _, batch in missed) > app.state.settings.max_catchup_ops:
            missed = []
            snapshot_b64 = base64.b64encode(room.rga.snapshot()).decode("ascii")
    else:
        snapshot_b64 = base64.b64encode(room.rga.snapshot()).decode("ascii")

    async with app.state.repo_context() as repo:
        document = await repo.require(room.doc_id)
        title = document.title

    peers = [p.to_public() for p in await app.state.presence.peers(room.doc_id) if p.site != site]

    await _send(
        ws,
        proto.welcome(
            doc_id=room.doc_id,
            site=site,
            seq=room.seq,
            snapshot_b64=snapshot_b64,
            ops=missed,
            peers=peers,
            role=role,
            user=user,
            title=title,
        ),
    )


async def _authorise(app, doc_id: str, token: str | None, share_token: str | None):
    """Resolve (user, role) from a JWT, a share link, or public access."""
    settings: Settings = app.state.settings
    user = None

    if token:
        try:
            claims = decode_token(token, settings)
        except AuthError:
            return None, None
        async with app.state.repo_context() as repo:
            found = await repo.user_by_id(claims.get("sub", ""))
        if found is None:
            return None, None
        user = {"id": found.id, "name": found.name, "email": found.email, "color": found.color}

    async with app.state.repo_context() as repo:
        repo: DocumentRepo
        role = await repo.role_for(doc_id, user["id"] if user else None)

        if share_token:
            link = await repo.resolve_share_link(share_token)
            if link is not None and link.document_id == doc_id:
                # A share link can only ever raise access, never lower it.
                role = _max_role(role, link.role)

    return user, role


def _max_role(a: str | None, b: str | None) -> str | None:
    from syncpad.storage import ROLE_RANK

    candidates = [r for r in (a, b) if r is not None]
    if not candidates:
        return None
    return max(candidates, key=lambda r: ROLE_RANK.get(r, -1))


async def _broadcast_presence(app, room) -> None:
    peers = await app.state.presence.peers(room.doc_id)
    payload = proto.presence_frame([p.to_public() for p in peers])
    await room.broadcast_local(payload)


async def _receive_frame(ws: WebSocket) -> dict:
    message = await ws.receive()
    if message.get("type") == "websocket.disconnect":
        raise WebSocketDisconnect(message.get("code", 1000))
    raw = message.get("text")
    if raw is None:
        raw = message.get("bytes")
    if raw is None:
        raise proto.BadFrame("empty frame")
    return proto.decode_frame(raw)


async def _send(ws: WebSocket, payload: bytes | str) -> None:
    """Write one frame.

    Accepts str as well as bytes so a fan-out can decode its payload once and
    hand the same string to every socket, instead of paying a UTF-8 round trip
    per recipient — which at fifty editors is forty-nine wasted conversions for
    every keystroke anyone types.
    """
    if ws.client_state is WebSocketState.DISCONNECTED:
        return
    await ws.send_text(payload if isinstance(payload, str) else payload.decode("utf-8"))


async def _fail(ws: WebSocket, code: str, message: str) -> None:
    with contextlib.suppress(Exception):
        await _send(ws, proto.error(code, message, fatal=True))
    with contextlib.suppress(Exception):
        await ws.close(code=4000)


def _guest_color(site: str) -> str:
    """A stable colour for an anonymous visitor, derived from their site id."""
    palette = (
        "#ef4444",
        "#f97316",
        "#eab308",
        "#22c55e",
        "#06b6d4",
        "#3b82f6",
        "#8b5cf6",
        "#ec4899",
    )
    return palette[sum(site.encode()) % len(palette)]

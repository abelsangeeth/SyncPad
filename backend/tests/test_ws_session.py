"""End-to-end WebSocket sessions over a real ASGI transport.

These run the genuine article: a real socket, real frames, the real room, the
real hub, and the real database. The headline assertion is the one the whole
project exists for — two clients editing concurrently end up with identical
text, and so does the server.
"""

from __future__ import annotations

import base64
import json

import anyio
import pytest

from syncpad.crdt import RGA, ops_from_dicts, ops_to_dicts

#: Every read in this module is bounded. Starlette's ``receive_json`` blocks
#: forever, which turns any wrong expectation — or any genuine server bug that
#: fails to send a frame — into a hung suite rather than a failing test. Going
#: through the underlying anyio stream lets us put a deadline on it, so a
#: missing frame is reported in seconds with a useful message.
RECV_TIMEOUT = 5.0


def recv(ws, timeout: float = RECV_TIMEOUT) -> dict:
    """Receive one frame, or fail with TimeoutError instead of blocking."""

    async def _receive():
        with anyio.fail_after(timeout):
            return await ws._send_rx.receive()

    message = ws.portal.call(_receive)
    ws._raise_on_close(message)
    return json.loads(message["text"])


class Editor:
    """A test client that keeps a local replica, like the browser does."""

    def __init__(self, ws, welcome: dict) -> None:
        self.ws = ws
        self.welcome = welcome
        self.site = welcome["site"]
        self.seq = welcome["seq"]
        self.rga = (
            RGA.load(self.site, base64.b64decode(welcome["snapshot"]))
            if welcome.get("snapshot")
            else RGA(self.site)
        )
        for batch in welcome.get("ops", []):
            self.rga.apply_many(ops_from_dicts(batch["ops"]))
        self.cseq = 0
        self.peers: list = list(welcome.get("peers", []))

    def type(self, index: int, text: str) -> None:
        ops = self.rga.local_insert(index, text)
        self.cseq += 1
        self.ws.send_json({"t": "ops", "ops": ops_to_dicts(ops), "cseq": self.cseq})

    def erase(self, index: int, length: int) -> None:
        ops = self.rga.local_delete(index, length)
        self.cseq += 1
        self.ws.send_json({"t": "ops", "ops": ops_to_dicts(ops), "cseq": self.cseq})

    def format(self, index: int, length: int, key: str, value) -> None:
        ops = self.rga.local_mark(index, length, key, value)
        self.cseq += 1
        self.ws.send_json({"t": "ops", "ops": ops_to_dicts(ops), "cseq": self.cseq})

    def _consume(self, frame: dict) -> dict:
        """Apply whatever a frame carries, tracking the highest sequence seen."""
        if frame["t"] == "ops":
            self.rga.apply_many(ops_from_dicts(frame["ops"]))
            self.seq = max(self.seq, frame["seq"])
        elif frame["t"] == "ack":
            self.seq = max(self.seq, frame["seq"])
        elif frame["t"] == "presence":
            self.peers = frame["peers"]
        return frame

    def pump_until(self, kind: str, limit: int = 40) -> dict:
        """Read frames, applying what they carry, until ``kind`` arrives."""
        for _ in range(limit):
            frame = self._consume(recv(self.ws))
            if frame["t"] == kind:
                return frame
        raise AssertionError(f"never received a {kind!r} frame")

    def await_ack(self, limit: int = 40) -> int:
        """Block until this client's own submission is acknowledged.

        Returns the sequence the server assigned. Frames that arrive first —
        presence updates, a peer's concurrent operations — are applied on the
        way past rather than discarded, which is what makes this safe to call
        while another editor is typing.
        """
        return self.pump_until("ack", limit)["seq"]

    def catch_up(self, seq: int, limit: int = 40) -> None:
        """Read until this client has seen everything up to ``seq``.

        Synchronising on the sequence number rather than on "the next frame of
        type X" is what makes concurrent-editing tests deterministic: it does
        not matter what order the frames arrive in, only that the client has
        caught up.
        """
        for _ in range(limit):
            if self.seq >= seq:
                return
            self._consume(recv(self.ws))
        if self.seq < seq:
            raise AssertionError(f"stuck at seq {self.seq}, expected {seq}")

    def await_presence(self, peers: int, limit: int = 40) -> list:
        """Read until a presence frame reports exactly ``peers`` participants."""
        for _ in range(limit):
            frame = self._consume(recv(self.ws))
            if frame["t"] == "presence" and len(frame["peers"]) == peers:
                return frame["peers"]
        raise AssertionError(f"never saw a presence frame with {peers} peers")


def read_until(ws, kind: str, limit: int = 20) -> dict:
    """Next frame of a given type, skipping presence chatter.

    A client's own join triggers a presence broadcast, so almost nothing is
    reliably "the very next frame" except the welcome itself.
    """
    for _ in range(limit):
        frame = recv(ws)
        if frame["t"] == kind:
            return frame
    raise AssertionError(f"never received a {kind!r} frame")


def connect(
    ws_client, doc_id: str, token: str | None = None, since: int = 0, share: str | None = None
):
    ws = ws_client.websocket_connect("/ws")
    ws.__enter__()
    hello: dict = {"t": "hello", "doc": doc_id, "since": since}
    if token:
        hello["token"] = token
    if share:
        hello["share"] = share
    ws.send_json(hello)
    return ws


@pytest.fixture
def session(ws_client):
    """A registered user plus a document, both created over HTTP."""
    response = ws_client.post(
        "/api/auth/register",
        json={"email": "ws@example.com", "name": "WS User", "password": "hunter2-hunter2"},
    )
    assert response.status_code == 201, response.text
    token = response.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    doc_id = ws_client.post("/api/documents", json={"title": "WS Doc"}, headers=headers).json()[
        "id"
    ]
    return {"token": token, "headers": headers, "doc": doc_id, "client": ws_client}


# ------------------------------------------------------------------ handshake


def test_welcome_frame_opens_a_session(session) -> None:
    ws = connect(session["client"], session["doc"], session["token"])
    try:
        welcome = recv(ws)
        assert welcome["t"] == "welcome"
        assert welcome["doc"] == session["doc"]
        assert welcome["role"] == "owner"
        assert welcome["you"]["name"] == "WS User"
        assert welcome["site"]
        assert welcome["title"] == "WS Doc"
    finally:
        ws.__exit__(None, None, None)


def test_unknown_document_is_refused(session) -> None:
    ws = connect(session["client"], "no-such-document", session["token"])
    try:
        frame = recv(ws)
        assert frame["t"] == "error"
        assert frame["code"] in {"not_found", "forbidden"}
        assert frame["fatal"] is True
    finally:
        ws.__exit__(None, None, None)


def test_anonymous_access_to_a_private_document_is_refused(session) -> None:
    ws = connect(session["client"], session["doc"], token=None)
    try:
        frame = recv(ws)
        assert frame["t"] == "error"
        assert frame["code"] == "forbidden"
    finally:
        ws.__exit__(None, None, None)


def test_a_bad_token_is_refused(session) -> None:
    ws = connect(session["client"], session["doc"], token="not.a.jwt")
    try:
        assert recv(ws)["code"] == "forbidden"
    finally:
        ws.__exit__(None, None, None)


def test_malformed_hello_is_rejected(ws_client) -> None:
    with ws_client.websocket_connect("/ws") as ws:
        ws.send_json({"t": "hello"})  # no document
        frame = recv(ws)
        assert frame["t"] == "error"
        assert frame["code"] == "bad_frame"


# -------------------------------------------------------------- convergence


def test_two_editors_converge(session) -> None:
    """The headline property, over real sockets."""
    client, doc, token = session["client"], session["doc"], session["token"]
    ws_a = connect(client, doc, token)
    ws_b = connect(client, doc, token)
    try:
        alice = Editor(ws_a, recv(ws_a))
        bob = Editor(ws_b, recv(ws_b))

        alice.type(0, "Hello from Alice. ")
        bob.catch_up(alice.await_ack())

        bob.type(len(bob.rga), "And from Bob.")
        alice.catch_up(bob.await_ack())

        assert alice.rga.text() == bob.rga.text()
        assert "Alice" in alice.rga.text() and "Bob" in alice.rga.text()
    finally:
        ws_a.__exit__(None, None, None)
        ws_b.__exit__(None, None, None)

    stored = client.get(f"/api/documents/{doc}", headers=session["headers"]).json()
    assert stored["text"] == alice.rga.text(), "the server must agree with the clients"


def test_concurrent_edits_at_the_same_position_converge(session) -> None:
    client, doc, token = session["client"], session["doc"], session["token"]
    ws_a = connect(client, doc, token)
    ws_b = connect(client, doc, token)
    try:
        alice = Editor(ws_a, recv(ws_a))
        bob = Editor(ws_b, recv(ws_b))

        alice.type(0, "SEED")
        bob.catch_up(alice.await_ack())

        # Both type at index 2 without having seen the other's edit.
        alice.type(2, "<A>")
        bob.type(2, "<B>")
        latest = max(alice.await_ack(), bob.await_ack())
        alice.catch_up(latest)
        bob.catch_up(latest)

        assert alice.rga.text() == bob.rga.text(), (
            f"diverged: {alice.rga.text()!r} vs {bob.rga.text()!r}"
        )
        assert "<A>" in alice.rga.text() and "<B>" in alice.rga.text()
    finally:
        ws_a.__exit__(None, None, None)
        ws_b.__exit__(None, None, None)


def test_deletes_and_marks_propagate(session) -> None:
    client, doc, token = session["client"], session["doc"], session["token"]
    ws_a = connect(client, doc, token)
    ws_b = connect(client, doc, token)
    try:
        alice = Editor(ws_a, recv(ws_a))
        bob = Editor(ws_b, recv(ws_b))

        alice.type(0, "the quick brown fox")
        bob.catch_up(alice.await_ack())

        alice.erase(4, 6)  # "quick "
        bob.catch_up(alice.await_ack())
        assert bob.rga.text() == "the brown fox"

        alice.format(4, 5, "bold", True)
        bob.catch_up(alice.await_ack())
        assert [(s.text, dict(s.marks)) for s in bob.rga.spans()] == [
            ("the ", {}),
            ("brown", {"bold": True}),
            (" fox", {}),
        ]
    finally:
        ws_a.__exit__(None, None, None)
        ws_b.__exit__(None, None, None)


# ------------------------------------------------------------- reconnection


def test_reconnecting_with_since_receives_only_the_delta(session) -> None:
    """A dropped connection costs a few operations, not a whole document."""
    client, doc, token = session["client"], session["doc"], session["token"]

    ws_a = connect(client, doc, token)
    alice = Editor(ws_a, recv(ws_a))
    alice.type(0, "first pass. ")
    seq_at_disconnect = alice.await_ack()
    offline_replica = alice.rga  # the browser keeps its replica across a drop
    ws_a.__exit__(None, None, None)

    # Someone else edits while we are away.
    ws_b = connect(client, doc, token)
    bob = Editor(ws_b, recv(ws_b))
    bob.type(len(bob.rga), "second pass.")
    bob.await_ack()
    ws_b.__exit__(None, None, None)

    ws_c = connect(client, doc, token, since=seq_at_disconnect)
    try:
        welcome = recv(ws_c)
        assert welcome["t"] == "welcome"
        assert welcome["snapshot"] is None, "a delta was expected, not a snapshot"
        assert len(welcome["ops"]) >= 1, "the missed operations should have been sent"

        # Apply the delta onto the replica we kept, exactly as the client does.
        for batch in welcome["ops"]:
            offline_replica.apply_many(ops_from_dicts(batch["ops"]))
        assert offline_replica.text() == "first pass. second pass."
    finally:
        ws_c.__exit__(None, None, None)


def test_cold_connect_receives_a_snapshot(session) -> None:
    client, doc, token = session["client"], session["doc"], session["token"]
    ws_a = connect(client, doc, token)
    alice = Editor(ws_a, recv(ws_a))
    alice.type(0, "content that must survive")
    alice.pump_until("ack")
    ws_a.__exit__(None, None, None)

    ws_b = connect(client, doc, token, since=0)
    try:
        welcome = recv(ws_b)
        assert welcome["snapshot"] is not None
        fresh = Editor(ws_b, welcome)
        assert fresh.rga.text() == "content that must survive"
    finally:
        ws_b.__exit__(None, None, None)


# ---------------------------------------------------------------- presence


def test_peers_appear_and_disappear(session) -> None:
    client, doc, token = session["client"], session["doc"], session["token"]
    ws_a = connect(client, doc, token)
    try:
        alice = Editor(ws_a, recv(ws_a))

        ws_b = connect(client, doc, token)
        try:
            bob_welcome = recv(ws_b)
            assert bob_welcome["t"] == "welcome"
            assert len(bob_welcome["peers"]) == 1, "Bob should see Alice already present"
            assert len(alice.await_presence(2)) == 2
        finally:
            ws_b.__exit__(None, None, None)

        assert len(alice.await_presence(1)) == 1, "Bob must vanish when his socket closes"
    finally:
        # Leaving a socket open would block the test client's own teardown,
        # turning a failed assertion into a hung suite.
        ws_a.__exit__(None, None, None)


def test_cursor_updates_are_broadcast(session) -> None:
    client, doc, token = session["client"], session["doc"], session["token"]
    ws_a = connect(client, doc, token)
    ws_b = connect(client, doc, token)
    try:
        alice = Editor(ws_a, recv(ws_a))
        bob = Editor(ws_b, recv(ws_b))

        alice.type(0, "abcdef")
        bob.catch_up(alice.await_ack())

        anchor = alice.rga.id_at(2).encode()
        ws_a.send_json({"t": "cursor", "anchor": anchor, "head": anchor})

        for _ in range(20):
            frame = bob._consume(recv(ws_b))
            if frame["t"] == "presence" and any(p["anchor"] == anchor for p in frame["peers"]):
                break
        else:
            raise AssertionError("the peer's cursor never reached the other client")
    finally:
        ws_a.__exit__(None, None, None)
        ws_b.__exit__(None, None, None)


# --------------------------------------------------------- errors and limits


def test_a_viewer_may_read_but_not_edit(ws_client) -> None:
    owner = ws_client.post(
        "/api/auth/register",
        json={"email": "own@example.com", "name": "Owner", "password": "hunter2-hunter2"},
    ).json()
    reader = ws_client.post(
        "/api/auth/register",
        json={"email": "read@example.com", "name": "Reader", "password": "hunter2-hunter2"},
    ).json()
    headers = {"Authorization": f"Bearer {owner['access_token']}"}
    doc = ws_client.post("/api/documents", json={"title": "ACL"}, headers=headers).json()["id"]
    ws_client.post(
        f"/api/documents/{doc}/collaborators",
        json={"email": "read@example.com", "role": "viewer"},
        headers=headers,
    )

    ws = connect(ws_client, doc, reader["access_token"])
    try:
        welcome = recv(ws)
        assert welcome["role"] == "viewer"

        scratch = RGA(welcome["site"])
        ws.send_json({"t": "ops", "ops": ops_to_dicts(scratch.local_insert(0, "nope")), "cseq": 1})
        frame = read_until(ws, "error")
        assert frame["code"] == "forbidden"
        assert frame["fatal"] is False, "a refused edit must not close the socket"

        # The socket still works afterwards.
        ws.send_json({"t": "ping"})
        assert read_until(ws, "pong")["t"] == "pong"
    finally:
        ws.__exit__(None, None, None)


def test_malformed_operations_do_not_kill_the_session(session) -> None:
    ws = connect(session["client"], session["doc"], session["token"])
    try:
        assert recv(ws)["t"] == "welcome"
        ws.send_json({"t": "ops", "ops": [{"k": "nonsense"}], "cseq": 1})
        assert read_until(ws, "error")["code"] == "bad_frame"

        ws.send_json({"t": "ping"})
        assert read_until(ws, "pong")["t"] == "pong"
    finally:
        ws.__exit__(None, None, None)


def test_unknown_frame_types_are_reported(session) -> None:
    ws = connect(session["client"], session["doc"], session["token"])
    try:
        assert recv(ws)["t"] == "welcome"
        ws.send_json({"t": "teleport"})
        frame = read_until(ws, "error")
        assert frame["code"] == "bad_frame"
        assert "teleport" in frame["msg"]
    finally:
        ws.__exit__(None, None, None)


def test_a_flood_is_rate_limited_not_fatal(ws_client) -> None:
    """Sustained abuse gets refused; the connection survives to be told so."""
    from syncpad.config import Settings

    registered = ws_client.post(
        "/api/auth/register",
        json={"email": "flood@example.com", "name": "Flood", "password": "hunter2-hunter2"},
    ).json()
    token = registered["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    doc = ws_client.post("/api/documents", json={"title": "Flood"}, headers=headers).json()["id"]

    # Tighten the limit for this app instance so the test stays fast.
    settings: Settings = ws_client.app.state.settings
    settings.rate_ops_per_s = 1.0
    settings.rate_burst = 5

    ws = connect(ws_client, doc, token)
    try:
        welcome = recv(ws)
        local = RGA(welcome["site"])
        limited = False
        for i in range(40):
            ops = local.local_insert(len(local), "x")
            ws.send_json({"t": "ops", "ops": ops_to_dicts(ops), "cseq": i})
            frame = recv(ws)
            while frame["t"] not in ("ack", "error"):
                frame = recv(ws)

            if frame["t"] == "error" and frame["code"] == "rate_limited":
                limited = True
                break
        assert limited, "a 40-operation burst against a 5-token bucket must be throttled"

        ws.send_json({"t": "ping"})
        assert recv(ws)["t"] == "pong", "throttling must not close the socket"
    finally:
        ws.__exit__(None, None, None)


def test_public_documents_allow_anonymous_readers(ws_client) -> None:
    registered = ws_client.post(
        "/api/auth/register",
        json={"email": "pub@example.com", "name": "Pub", "password": "hunter2-hunter2"},
    ).json()
    headers = {"Authorization": f"Bearer {registered['access_token']}"}
    doc = ws_client.post("/api/documents", json={"title": "Public"}, headers=headers).json()["id"]
    ws_client.put(f"/api/documents/{doc}/public", json={"is_public": True}, headers=headers)

    ws = connect(ws_client, doc, token=None)
    try:
        welcome = recv(ws)
        assert welcome["t"] == "welcome"
        assert welcome["role"] == "viewer"
        assert welcome["you"] is None
    finally:
        ws.__exit__(None, None, None)


def test_a_share_link_grants_editing(ws_client) -> None:
    registered = ws_client.post(
        "/api/auth/register",
        json={"email": "share@example.com", "name": "Share", "password": "hunter2-hunter2"},
    ).json()
    headers = {"Authorization": f"Bearer {registered['access_token']}"}
    doc = ws_client.post("/api/documents", json={"title": "Shared"}, headers=headers).json()["id"]
    link = ws_client.post(
        f"/api/documents/{doc}/share", json={"role": "editor"}, headers=headers
    ).json()

    ws = connect(ws_client, doc, token=None, share=link["token"])
    try:
        welcome = recv(ws)
        assert welcome["role"] == "editor", "a share link must raise an anonymous role"
    finally:
        ws.__exit__(None, None, None)

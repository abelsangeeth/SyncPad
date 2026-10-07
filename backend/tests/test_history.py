"""Version history, time travel, and restore-as-an-edit.

The critical property is that restore never rewrites the log. A test that only
checked "the text came back" would pass for a destructive implementation too, so
the tests here also assert that the sequence number *advanced* — that the past
was reached by appending, not by deleting.
"""

from __future__ import annotations

from syncpad.api.history import diff_to_ops
from syncpad.crdt import RGA


async def _edit(auth_client, doc_id: str, text: str) -> int:
    """Append one batch of typing through the repo, returning the new sequence."""
    from syncpad.storage import DocumentRepo

    app = auth_client._transport.app
    async with app.state.repo_context() as repo:
        repo: DocumentRepo
        current, _, _ = await app.state.snapshots.load(doc_id)
        ops = current.local_insert(len(current), text)
        return await repo.append_ops(doc_id, ops, site=current.site, actor_id=None)


# ------------------------------------------------------------- diff_to_ops


def test_diff_produces_no_ops_when_nothing_changed() -> None:
    doc = RGA("a")
    doc.local_insert(0, "unchanged")
    assert diff_to_ops(doc, "unchanged") == []


def test_diff_transforms_text_exactly() -> None:
    doc = RGA("a")
    doc.local_insert(0, "the quick brown fox")
    ops = diff_to_ops(doc, "the slow brown cat")
    assert ops
    assert doc.text() == "the slow brown cat"


def test_diff_is_minimal_rather_than_retyping_everything() -> None:
    """A one-word restore must not tombstone the whole document."""
    doc = RGA("a")
    doc.local_insert(0, "a " * 500 + "word")
    before = doc.tombstones

    diff_to_ops(doc, "a " * 500 + "term")
    assert doc.text().endswith("term")
    assert doc.tombstones - before < 20, "restore rewrote far more than it needed to"


def test_diff_handles_insertion_deletion_and_replacement() -> None:
    for source, target in [
        ("", "brand new"),
        ("delete me", ""),
        ("abc", "abXc"),
        ("hello world", "world hello"),
        ("héllo 🌍", "héllo 🌎"),
    ]:
        doc = RGA("a")
        if source:
            doc.local_insert(0, source)
        diff_to_ops(doc, target)
        assert doc.text() == target, f"{source!r} -> {target!r}"


def test_diff_ops_converge_on_a_peer() -> None:
    """The ops a restore produces must merge like any other edit."""
    author = RGA("author")
    setup = author.local_insert(0, "original text")
    peer = RGA("peer")
    peer.apply_many(setup)

    restore_ops = diff_to_ops(author, "restored text")
    peer.apply_many(restore_ops)
    assert peer.text() == author.text() == "restored text"


# ---------------------------------------------------------------- endpoints


async def test_history_lists_revisions(auth_client) -> None:
    created = await auth_client.post("/api/documents", json={"title": "H", "text": "one"})
    doc_id = created.json()["id"]
    await _edit(auth_client, doc_id, " two")
    await _edit(auth_client, doc_id, " three")

    response = await auth_client.get(f"/api/documents/{doc_id}/history")
    assert response.status_code == 200
    body = response.json()
    assert body["seq"] == 3
    assert len(body["revisions"]) == 3
    assert body["revisions"][0]["seq"] == 3, "newest first"
    assert all(r["ops"] >= 1 for r in body["revisions"])
    assert "earliest_reachable" in body


async def test_history_records_the_author(auth_client) -> None:
    created = await auth_client.post("/api/documents", json={"title": "H", "text": "mine"})
    response = await auth_client.get(f"/api/documents/{created.json()['id']}/history")
    revision = response.json()["revisions"][0]
    assert revision["author_id"] == auth_client.syncpad_user["id"]
    assert revision["author_name"] == "Owner"


async def test_version_at_returns_historical_text(auth_client) -> None:
    created = await auth_client.post("/api/documents", json={"title": "H", "text": "v1"})
    doc_id = created.json()["id"]
    await _edit(auth_client, doc_id, "-v2")
    await _edit(auth_client, doc_id, "-v3")

    assert (await auth_client.get(f"/api/documents/{doc_id}/history/at/1")).json()["text"] == "v1"
    at_two = await auth_client.get(f"/api/documents/{doc_id}/history/at/2")
    assert at_two.json()["text"] == "v1-v2"
    assert (await auth_client.get(f"/api/documents/{doc_id}/history/at/3")).json()[
        "text"
    ] == "v1-v2-v3"


async def test_version_at_zero_is_empty(auth_client) -> None:
    created = await auth_client.post("/api/documents", json={"title": "H", "text": "anything"})
    response = await auth_client.get(f"/api/documents/{created.json()['id']}/history/at/0")
    assert response.json()["text"] == ""


async def test_version_beyond_retention_is_gone_not_missing(auth_client, settings) -> None:
    """Compacted history must answer 410, not a plausible empty document."""
    created = await auth_client.post("/api/documents", json={"title": "H", "text": "start"})
    doc_id = created.json()["id"]
    for i in range(30):
        await _edit(auth_client, doc_id, f" {i}")
        app = auth_client._transport.app
        current, seq, _ = await app.state.snapshots.load(doc_id)
        await app.state.snapshots.maybe_snapshot(doc_id, current, seq)

    earliest = (await auth_client.get(f"/api/documents/{doc_id}/history")).json()[
        "earliest_reachable"
    ]
    assert earliest > 0, "nothing was compacted, so this test proves nothing"

    response = await auth_client.get(f"/api/documents/{doc_id}/history/at/1")
    assert response.status_code == 410
    assert "compacted" in response.json()["detail"]


async def test_restore_appends_rather_than_rewriting(auth_client) -> None:
    """The load-bearing property: history only ever grows."""
    created = await auth_client.post("/api/documents", json={"title": "R", "text": "version one"})
    doc_id = created.json()["id"]
    await _edit(auth_client, doc_id, " plus two")
    await _edit(auth_client, doc_id, " plus three")

    before = (await auth_client.get(f"/api/documents/{doc_id}/history")).json()
    assert before["seq"] == 3

    restored = await auth_client.post(f"/api/documents/{doc_id}/history/restore/1")
    assert restored.status_code == 200
    body = restored.json()
    assert body["text"] == "version one"
    assert body["restored_from"] == 1
    assert body["seq"] > before["seq"], "restore must append a new revision"

    after = (await auth_client.get(f"/api/documents/{doc_id}/history")).json()
    assert len(after["revisions"]) > len(before["revisions"])
    assert (await auth_client.get(f"/api/documents/{doc_id}")).json()["text"] == "version one"


async def test_restore_is_itself_restorable(auth_client) -> None:
    created = await auth_client.post("/api/documents", json={"title": "R", "text": "alpha"})
    doc_id = created.json()["id"]
    await _edit(auth_client, doc_id, " beta")

    rolled_back = await auth_client.post(f"/api/documents/{doc_id}/history/restore/1")
    assert rolled_back.json()["text"] == "alpha"

    # Undo the undo by restoring the revision that had both words.
    rolled_forward = await auth_client.post(f"/api/documents/{doc_id}/history/restore/2")
    assert rolled_forward.json()["text"] == "alpha beta"


async def test_restoring_the_current_state_is_a_noop(auth_client) -> None:
    created = await auth_client.post("/api/documents", json={"title": "R", "text": "same"})
    doc_id = created.json()["id"]
    response = await auth_client.post(f"/api/documents/{doc_id}/history/restore/1")
    assert response.status_code == 200
    assert response.json()["ops_applied"] == 0


async def test_restore_requires_edit_rights(auth_client, make_user, owned_doc) -> None:
    viewer_headers, _, _ = await make_user(email="ro@example.com")
    await auth_client.post(
        f"/api/documents/{owned_doc}/collaborators",
        json={"email": "ro@example.com", "role": "viewer"},
    )
    response = await auth_client.post(
        f"/api/documents/{owned_doc}/history/restore/0", headers=viewer_headers
    )
    assert response.status_code == 403


async def test_history_requires_read_access(auth_client, make_user, owned_doc) -> None:
    stranger_headers, _, _ = await make_user(email="nosy@example.com")
    response = await auth_client.get(
        f"/api/documents/{owned_doc}/history", headers=stranger_headers
    )
    assert response.status_code == 404


async def test_negative_sequence_is_rejected(auth_client, owned_doc) -> None:
    assert (await auth_client.get(f"/api/documents/{owned_doc}/history/at/-1")).status_code == 422

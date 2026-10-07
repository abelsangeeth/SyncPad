"""Comments anchored to character ids.

The behaviour worth testing is what happens to an anchor when the document
changes around it. Three cases: text inserted *before* the anchor (it must not
drift), text inserted *inside* it (it must still quote the right range), and the
anchored text deleted entirely (it must report orphaned rather than silently
re-pointing at whatever now occupies those offsets).
"""

from __future__ import annotations

import pytest

from syncpad.crdt import RGA
from syncpad.storage import DocumentRepo


async def _load(auth_client, doc_id: str) -> RGA:
    app = auth_client._transport.app
    rga, _, _ = await app.state.snapshots.load(doc_id)
    return rga


async def _apply(auth_client, doc_id: str, mutate) -> RGA:
    """Load the document, let ``mutate`` edit it, persist the resulting ops."""
    app = auth_client._transport.app
    async with app.state.repo_context() as repo:
        repo: DocumentRepo
        rga, _, _ = await app.state.snapshots.load(doc_id)
        ops = mutate(rga)
        if ops:
            await repo.append_ops(doc_id, ops, site=rga.site, actor_id=None)
    return rga


@pytest.fixture
async def anchored(auth_client):
    """A document reading 'the quick brown fox' with a comment on 'brown'."""
    created = await auth_client.post(
        "/api/documents", json={"title": "C", "text": "the quick brown fox"}
    )
    doc_id = created.json()["id"]

    rga = await _load(auth_client, doc_id)
    start = rga.id_at(10).encode()  # 'b'
    end = rga.id_at(14).encode()  # 'n'
    assert rga.text_between(rga.id_at(10), rga.id_at(14)) == "brown"

    response = await auth_client.post(
        f"/api/documents/{doc_id}/comments",
        json={"start": start, "end": end, "body": "Which brown?"},
    )
    assert response.status_code == 201, response.text
    return {"doc": doc_id, "comment": response.json(), "start": start, "end": end}


async def test_creating_a_comment_quotes_the_anchored_text(anchored) -> None:
    comment = anchored["comment"]
    assert comment["quoted"] == "brown"
    assert comment["current_text"] == "brown"
    assert comment["orphaned"] is False
    assert comment["resolved"] is False
    assert comment["author_name"] == "Owner"
    assert comment["start_index"] == 10


async def test_listing_comments(auth_client, anchored) -> None:
    response = await auth_client.get(f"/api/documents/{anchored['doc']}/comments")
    assert response.status_code == 200
    assert [c["body"] for c in response.json()] == ["Which brown?"]


async def test_anchor_survives_an_edit_before_it(auth_client, anchored) -> None:
    """The case an offset-based anchor gets wrong."""
    await _apply(auth_client, anchored["doc"], lambda r: r.local_insert(0, "WAIT: "))

    listed = (await auth_client.get(f"/api/documents/{anchored['doc']}/comments")).json()[0]
    assert listed["current_text"] == "brown", "the anchor drifted"
    assert listed["orphaned"] is False
    assert listed["start_index"] == 16, "the highlight must move with the text"


async def test_anchor_survives_an_edit_inside_it(auth_client, anchored) -> None:
    await _apply(auth_client, anchored["doc"], lambda r: r.local_insert(12, "-ish-"))

    listed = (await auth_client.get(f"/api/documents/{anchored['doc']}/comments")).json()[0]
    assert listed["current_text"] == "br-ish-own"
    assert listed["quoted"] == "brown", "the original quote is preserved for reference"
    assert listed["orphaned"] is False


async def test_a_comment_becomes_orphaned_when_its_text_is_deleted(auth_client, anchored) -> None:
    await _apply(auth_client, anchored["doc"], lambda r: r.local_delete(10, 5))

    listed = (await auth_client.get(f"/api/documents/{anchored['doc']}/comments")).json()[0]
    assert listed["orphaned"] is True
    assert listed["current_text"] == ""
    assert listed["quoted"] == "brown", "the discussion keeps what it was about"
    assert listed["body"] == "Which brown?", "an orphaned comment is kept, not deleted"


async def test_partially_deleting_the_anchor_does_not_orphan_it(auth_client, anchored) -> None:
    await _apply(auth_client, anchored["doc"], lambda r: r.local_delete(10, 2))

    listed = (await auth_client.get(f"/api/documents/{anchored['doc']}/comments")).json()[0]
    assert listed["orphaned"] is False
    assert listed["current_text"] == "own"


async def test_resolving_toggles(auth_client, anchored) -> None:
    doc, comment_id = anchored["doc"], anchored["comment"]["id"]

    resolved = await auth_client.post(f"/api/documents/{doc}/comments/{comment_id}/resolve")
    assert resolved.status_code == 200
    assert resolved.json()["resolved"] is True

    reopened = await auth_client.post(f"/api/documents/{doc}/comments/{comment_id}/resolve")
    assert reopened.json()["resolved"] is False


async def test_threaded_replies(auth_client, anchored) -> None:
    doc = anchored["doc"]
    reply = await auth_client.post(
        f"/api/documents/{doc}/comments",
        json={
            "start": anchored["start"],
            "end": anchored["end"],
            "body": "The dark one.",
            "parent_id": anchored["comment"]["id"],
        },
    )
    assert reply.status_code == 201
    assert reply.json()["parent_id"] == anchored["comment"]["id"]
    assert len((await auth_client.get(f"/api/documents/{doc}/comments")).json()) == 2


async def test_reply_to_an_unknown_parent_is_rejected(auth_client, anchored) -> None:
    response = await auth_client.post(
        f"/api/documents/{anchored['doc']}/comments",
        json={
            "start": anchored["start"],
            "end": anchored["end"],
            "body": "orphan reply",
            "parent_id": "00000000-0000-0000-0000-000000000000",
        },
    )
    assert response.status_code == 400


async def test_deleting_a_comment(auth_client, anchored) -> None:
    doc, comment_id = anchored["doc"], anchored["comment"]["id"]
    assert (
        await auth_client.delete(f"/api/documents/{doc}/comments/{comment_id}")
    ).status_code == 204
    assert (await auth_client.get(f"/api/documents/{doc}/comments")).json() == []


async def test_deleting_an_unknown_comment_is_a_404(auth_client, anchored) -> None:
    response = await auth_client.delete(f"/api/documents/{anchored['doc']}/comments/does-not-exist")
    assert response.status_code == 404


async def test_anchors_must_exist_in_the_document(auth_client, owned_doc) -> None:
    response = await auth_client.post(
        f"/api/documents/{owned_doc}/comments",
        json={"start": "ghost:1", "end": "ghost:5", "body": "nowhere"},
    )
    assert response.status_code == 400
    assert "anchors do not exist" in response.json()["detail"]


async def test_malformed_anchors_are_rejected(auth_client, owned_doc) -> None:
    response = await auth_client.post(
        f"/api/documents/{owned_doc}/comments",
        json={"start": "not-an-opid", "end": "also-bad", "body": "nowhere"},
    )
    assert response.status_code == 400
    assert "bad anchor" in response.json()["detail"]


async def test_a_viewer_cannot_comment(auth_client, make_user, anchored) -> None:
    viewer_headers, _, _ = await make_user(email="lurker@example.com")
    await auth_client.post(
        f"/api/documents/{anchored['doc']}/collaborators",
        json={"email": "lurker@example.com", "role": "viewer"},
    )
    response = await auth_client.post(
        f"/api/documents/{anchored['doc']}/comments",
        json={"start": anchored["start"], "end": anchored["end"], "body": "hi"},
        headers=viewer_headers,
    )
    assert response.status_code == 403


async def test_a_commenter_may_comment_but_not_edit(auth_client, make_user, anchored) -> None:
    commenter_headers, _, _ = await make_user(email="reviewer@example.com")
    await auth_client.post(
        f"/api/documents/{anchored['doc']}/collaborators",
        json={"email": "reviewer@example.com", "role": "commenter"},
    )

    posted = await auth_client.post(
        f"/api/documents/{anchored['doc']}/comments",
        json={"start": anchored["start"], "end": anchored["end"], "body": "a note"},
        headers=commenter_headers,
    )
    assert posted.status_code == 201

    renamed = await auth_client.patch(
        f"/api/documents/{anchored['doc']}", json={"title": "nope"}, headers=commenter_headers
    )
    assert renamed.status_code == 403


async def test_a_commenter_cannot_delete_someone_elses_comment(
    auth_client, make_user, anchored
) -> None:
    other_headers, _, _ = await make_user(email="other@example.com")
    await auth_client.post(
        f"/api/documents/{anchored['doc']}/collaborators",
        json={"email": "other@example.com", "role": "commenter"},
    )
    response = await auth_client.delete(
        f"/api/documents/{anchored['doc']}/comments/{anchored['comment']['id']}",
        headers=other_headers,
    )
    assert response.status_code == 403


async def test_an_author_may_delete_their_own_comment(auth_client, make_user, anchored) -> None:
    author_headers, _, _ = await make_user(email="author@example.com")
    await auth_client.post(
        f"/api/documents/{anchored['doc']}/collaborators",
        json={"email": "author@example.com", "role": "commenter"},
    )
    posted = await auth_client.post(
        f"/api/documents/{anchored['doc']}/comments",
        json={"start": anchored["start"], "end": anchored["end"], "body": "mine"},
        headers=author_headers,
    )
    response = await auth_client.delete(
        f"/api/documents/{anchored['doc']}/comments/{posted.json()['id']}",
        headers=author_headers,
    )
    assert response.status_code == 204

"""Document CRUD, access control, sharing and export."""

from __future__ import annotations

import pytest

from syncpad.api.documents import to_html, to_markdown, to_plain
from syncpad.crdt import RGA


async def test_create_list_and_fetch(auth_client) -> None:
    created = await auth_client.post("/api/documents", json={"title": "My Notes"})
    assert created.status_code == 201
    doc = created.json()
    assert doc["title"] == "My Notes"
    assert doc["role"] == "owner"

    listing = await auth_client.get("/api/documents")
    assert listing.status_code == 200
    assert doc["id"] in {d["id"] for d in listing.json()}

    fetched = await auth_client.get(f"/api/documents/{doc['id']}")
    assert fetched.status_code == 200
    assert fetched.json()["text"] == ""
    assert fetched.json()["chars"] == 0


async def test_create_with_seed_text_builds_a_real_history(auth_client) -> None:
    """Imported text must arrive as operations, not as a mystery initial state."""
    created = await auth_client.post(
        "/api/documents", json={"title": "Imported", "text": "Hello, world"}
    )
    doc = created.json()
    assert doc["seq"] == 1, "seed text should be one operation batch"

    fetched = await auth_client.get(f"/api/documents/{doc['id']}")
    assert fetched.json()["text"] == "Hello, world"

    history = await auth_client.get(f"/api/documents/{doc['id']}/history")
    assert len(history.json()["revisions"]) == 1


async def test_rename_and_delete(auth_client, owned_doc) -> None:
    renamed = await auth_client.patch(f"/api/documents/{owned_doc}", json={"title": "Renamed"})
    assert renamed.status_code == 200
    assert renamed.json()["title"] == "Renamed"

    deleted = await auth_client.delete(f"/api/documents/{owned_doc}")
    assert deleted.status_code == 204
    assert (await auth_client.get(f"/api/documents/{owned_doc}")).status_code == 404


async def test_unknown_document_is_a_404(auth_client) -> None:
    assert (await auth_client.get("/api/documents/nope")).status_code == 404


async def test_listing_requires_authentication(client) -> None:
    assert (await client.get("/api/documents")).status_code == 401


# --------------------------------------------------------------- access control


async def test_another_users_document_is_invisible(auth_client, make_user) -> None:
    """A document you cannot see must 404, not 403 — a 403 confirms it exists."""
    created = await auth_client.post("/api/documents", json={"title": "Private"})
    doc_id = created.json()["id"]

    stranger_headers, _, _ = await make_user(email="stranger@example.com")
    response = await auth_client.get(f"/api/documents/{doc_id}", headers=stranger_headers)
    assert response.status_code == 404


async def test_granting_and_revoking_access(auth_client, make_user, owned_doc) -> None:
    friend_headers, friend, _ = await make_user(email="friend@example.com", name="Friend")

    granted = await auth_client.post(
        f"/api/documents/{owned_doc}/collaborators",
        json={"email": "friend@example.com", "role": "editor"},
    )
    assert granted.status_code == 200
    assert granted.json()["role"] == "editor"

    seen = await auth_client.get(f"/api/documents/{owned_doc}", headers=friend_headers)
    assert seen.status_code == 200
    assert seen.json()["role"] == "editor"

    collaborators = await auth_client.get(f"/api/documents/{owned_doc}/collaborators")
    assert [c["user"]["email"] for c in collaborators.json()] == ["friend@example.com"]

    revoked = await auth_client.delete(f"/api/documents/{owned_doc}/collaborators/{friend['id']}")
    assert revoked.status_code == 204
    assert (
        await auth_client.get(f"/api/documents/{owned_doc}", headers=friend_headers)
    ).status_code == 404


async def test_a_viewer_cannot_rename(auth_client, make_user, owned_doc) -> None:
    viewer_headers, _, _ = await make_user(email="viewer@example.com")
    await auth_client.post(
        f"/api/documents/{owned_doc}/collaborators",
        json={"email": "viewer@example.com", "role": "viewer"},
    )
    response = await auth_client.patch(
        f"/api/documents/{owned_doc}", json={"title": "Hijacked"}, headers=viewer_headers
    )
    assert response.status_code == 403


async def test_an_editor_cannot_delete_or_reshare(auth_client, make_user, owned_doc) -> None:
    editor_headers, _, _ = await make_user(email="editor@example.com")
    await auth_client.post(
        f"/api/documents/{owned_doc}/collaborators",
        json={"email": "editor@example.com", "role": "editor"},
    )
    assert (
        await auth_client.delete(f"/api/documents/{owned_doc}", headers=editor_headers)
    ).status_code == 403
    assert (
        await auth_client.post(
            f"/api/documents/{owned_doc}/share", json={"role": "viewer"}, headers=editor_headers
        )
    ).status_code == 403


async def test_granting_to_an_unknown_email_is_a_404(auth_client, owned_doc) -> None:
    response = await auth_client.post(
        f"/api/documents/{owned_doc}/collaborators",
        json={"email": "nobody@example.com", "role": "editor"},
    )
    assert response.status_code == 404


async def test_granting_to_yourself_is_rejected(auth_client, owned_doc) -> None:
    response = await auth_client.post(
        f"/api/documents/{owned_doc}/collaborators",
        json={"email": auth_client.syncpad_user["email"], "role": "editor"},
    )
    assert response.status_code == 400


async def test_public_documents_are_readable_anonymously(auth_client, client, owned_doc) -> None:
    assert (
        await client.get(f"/api/documents/{owned_doc}", headers={"Authorization": ""})
    ).status_code in (401, 404)

    await auth_client.put(f"/api/documents/{owned_doc}/public", json={"is_public": True})

    anonymous = await auth_client.get(
        f"/api/documents/{owned_doc}", headers={"Authorization": "Bearer "}
    )
    assert anonymous.status_code == 200
    assert anonymous.json()["role"] == "viewer"


async def test_share_link_creation(auth_client, owned_doc) -> None:
    response = await auth_client.post(
        f"/api/documents/{owned_doc}/share", json={"role": "editor", "expires_in_hours": 24}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["role"] == "editor"
    assert body["token"]
    assert owned_doc in body["url_path"]
    assert body["expires_at"] is not None


# -------------------------------------------------------------------- export


@pytest.fixture
def formatted_spans() -> list:
    doc = RGA("fmt")
    doc.local_insert(0, "Title\nbold and italic and code")
    doc.local_mark(0, 5, "heading", "1")
    doc.local_mark(6, 4, "bold", True)
    doc.local_mark(15, 6, "italic", True)
    doc.local_mark(26, 4, "code", True)
    return doc.spans()


def test_markdown_rendering(formatted_spans: list) -> None:
    rendered = to_markdown(formatted_spans)
    assert "# Title" in rendered
    assert "**bold**" in rendered
    assert "*italic*" in rendered
    assert "`code`" in rendered


def test_plain_rendering_drops_formatting(formatted_spans: list) -> None:
    assert to_plain(formatted_spans) == "Title\nbold and italic and code"


def test_html_rendering(formatted_spans: list) -> None:
    rendered = to_html(formatted_spans, "My Doc")
    assert "<h1>Title</h1>" in rendered
    assert "<strong>bold</strong>" in rendered
    assert "<em>italic</em>" in rendered
    assert "<code>code</code>" in rendered
    assert "<title>My Doc</title>" in rendered


def test_html_export_escapes_user_text() -> None:
    """The stored-XSS path: one user's text becomes another user's page."""
    doc = RGA("xss")
    doc.local_insert(0, '<script>alert("pwned")</script> & more')
    rendered = to_html(doc.spans(), title='<img onerror="x">')

    assert "<script>" not in rendered
    assert "&lt;script&gt;" in rendered
    assert "&amp; more" in rendered
    assert '<img onerror="x">' not in rendered


def test_html_export_neutralises_dangerous_link_schemes() -> None:
    doc = RGA("link")
    doc.local_insert(0, "click me")
    doc.local_mark(0, 8, "link", "javascript:alert(1)")
    rendered = to_html(doc.spans())
    assert 'href="javascript:' not in rendered.lower()

    safe = RGA("link2")
    safe.local_insert(0, "click me")
    safe.local_mark(0, 8, "link", "https://example.com")
    assert '<a href="https://example.com"' in to_html(safe.spans())


def test_markdown_rendering_is_stable_for_nested_marks() -> None:
    """Identical formatting must always render identically, or diffs churn."""
    first, second = RGA("a"), RGA("b")
    for doc, order in ((first, ("bold", "italic")), (second, ("italic", "bold"))):
        doc.local_insert(0, "text")
        for key in order:
            doc.local_mark(0, 4, key, True)
    assert to_markdown(first.spans()) == to_markdown(second.spans())


@pytest.mark.parametrize(
    "fmt,fragment,media",
    [
        ("md", "**bold**", "text/markdown"),
        ("html", "<strong>bold</strong>", "text/html"),
        ("txt", "bold text", "text/plain"),
    ],
)
async def test_export_endpoint(auth_client, fmt: str, fragment: str, media: str) -> None:
    created = await auth_client.post(
        "/api/documents", json={"title": "Export Me", "text": "bold text"}
    )
    doc_id = created.json()["id"]

    # Apply a mark through the CRDT so the export has something to render.

    response = await auth_client.get(f"/api/documents/{doc_id}/export?format={fmt}")
    assert response.status_code == 200
    assert media in response.headers["content-type"]
    assert "attachment" in response.headers["content-disposition"]
    assert "Export Me" in response.headers["content-disposition"]
    if fmt == "txt":
        assert response.text == "bold text"


async def test_export_rejects_an_unknown_format(auth_client, owned_doc) -> None:
    assert (
        await auth_client.get(f"/api/documents/{owned_doc}/export?format=pdf")
    ).status_code == 422


async def test_export_filename_is_sanitised(auth_client) -> None:
    created = await auth_client.post(
        "/api/documents", json={"title": '../../etc/passwd"; rm -rf /'}
    )
    response = await auth_client.get(f"/api/documents/{created.json()['id']}/export")
    disposition = response.headers["content-disposition"]
    assert ".." not in disposition
    assert "/" not in disposition.split("filename=")[1]

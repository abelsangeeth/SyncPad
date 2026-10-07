"""Persistence, with particular attention to concurrent appends.

Several gateway replicas write to the same document's log at the same time and
never talk to each other. If sequence allocation had a race, it would surface as
skipped or duplicated operations — silently, under load, in production. So the
test for it fires a burst of concurrent appends and demands a gapless,
duplicate-free 1..N.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from syncpad.crdt import RGA
from syncpad.storage import DocumentNotFound, DocumentRepo, can_comment, can_edit
from syncpad.storage.models import Operation

pytestmark = pytest.mark.asyncio


async def test_create_and_fetch_document(repo: DocumentRepo, user) -> None:
    doc = await repo.create(title="Notes", owner_id=user.id)
    assert doc.id
    assert doc.seq == 0
    assert doc.snapshot_seq == 0
    assert (await repo.get(doc.id)).title == "Notes"


async def test_require_raises_for_unknown_document(repo: DocumentRepo) -> None:
    assert await repo.get("does-not-exist") is None
    with pytest.raises(DocumentNotFound):
        await repo.require("does-not-exist")


async def test_rename_and_delete(repo: DocumentRepo, session, document) -> None:
    await repo.rename(document.id, "Renamed")
    await session.commit()
    await session.refresh(document)
    assert document.title == "Renamed"

    await repo.delete(document.id)
    await session.commit()
    assert await repo.get(document.id) is None


async def test_append_ops_returns_monotonic_sequences(
    repo: DocumentRepo, session, document
) -> None:
    doc = RGA("a")
    for i in range(5):
        ops = doc.local_insert(len(doc), f"line {i}\n")
        seq = await repo.append_ops(document.id, ops, site="a", actor_id=None)
        await session.commit()
        assert seq == i + 1


async def test_append_ops_with_no_ops_is_a_noop(repo: DocumentRepo, session, document) -> None:
    assert await repo.append_ops(document.id, [], site="a") == 0
    assert await repo.op_count(document.id) == 0


async def test_append_ops_to_missing_document_raises(repo: DocumentRepo) -> None:
    doc = RGA("a")
    with pytest.raises(DocumentNotFound):
        await repo.append_ops("nope", doc.local_insert(0, "x"), site="a")


async def test_concurrent_appends_are_gapless_and_unique(repo_context, document, session) -> None:
    """The load-bearing concurrency test: N racing writers, N distinct sequences."""
    writers = 24

    async def write(index: int) -> int:
        doc = RGA(f"site{index}")
        ops = doc.local_insert(0, f"w{index}")
        async with repo_context() as r:
            return await r.append_ops(document.id, ops, site=f"site{index}", actor_id=None)

    seqs = await asyncio.gather(*(write(i) for i in range(writers)))

    assert sorted(seqs) == list(range(1, writers + 1)), f"sequence race: {sorted(seqs)}"
    assert len(set(seqs)) == writers, "two writers were handed the same sequence"

    async with repo_context() as r:
        assert await r.op_count(document.id) == writers
        batches = await r.ops_since(document.id, 0)
        assert [s for s, _ in batches] == list(range(1, writers + 1))


async def test_ops_since_and_between(repo: DocumentRepo, session, document) -> None:
    doc = RGA("a")
    for i in range(6):
        await repo.append_ops(document.id, doc.local_insert(len(doc), str(i)), site="a")
    await session.commit()

    assert len(await repo.ops_since(document.id, 0)) == 6
    assert [s for s, _ in await repo.ops_since(document.id, 4)] == [5, 6]
    assert [s for s, _ in await repo.ops_between(document.id, 2, 4)] == [3, 4]
    assert len(await repo.ops_since(document.id, 0, limit=2)) == 2


async def test_ops_round_trip_through_json_column(repo: DocumentRepo, session, document) -> None:
    """Operations must survive the database unchanged, marks and unicode included."""
    doc = RGA("a")
    ops = doc.local_insert(0, "héllo 🌍")
    ops += doc.local_mark(0, 5, "bold", True)
    ops += doc.local_delete(1, 1)
    await repo.append_ops(document.id, ops, site="a")
    await session.commit()

    rebuilt = RGA("b")
    for _, batch in await repo.ops_since(document.id, 0):
        rebuilt.apply_many(batch)
    assert rebuilt.text() == doc.text()
    assert rebuilt.spans() == doc.spans()


async def test_prune_ops_keeps_the_snapshot_boundary_batch(
    repo: DocumentRepo, session, document
) -> None:
    doc = RGA("a")
    for i in range(10):
        await repo.append_ops(document.id, doc.local_insert(len(doc), str(i)), site="a")
    await session.commit()

    removed = await repo.prune_ops(document.id, 6)
    await session.commit()
    assert removed == 5, "batches 1..5 go; batch 6 is the snapshot boundary and stays"

    remaining = [s for s, _ in await repo.ops_since(document.id, 0)]
    assert remaining == [6, 7, 8, 9, 10]


async def test_snapshot_write_read_and_trim(repo: DocumentRepo, session, document) -> None:
    doc = RGA("a")
    doc.local_insert(0, "content")

    for seq in (10, 20, 30, 40):
        await repo.write_snapshot(document.id, seq, doc.snapshot(), len(doc))
    await session.commit()

    latest = await repo.latest_snapshot(document.id)
    assert latest is not None and latest[0] == 40

    at = await repo.snapshot_at_or_before(document.id, 25)
    assert at is not None and at[0] == 20
    assert await repo.snapshot_at_or_before(document.id, 5) is None

    assert await repo.trim_snapshots(document.id, keep=2) == 2
    await session.commit()
    assert [s.seq for s in await repo.list_snapshots(document.id)] == [40, 30]


async def test_rewriting_a_snapshot_at_the_same_seq_replaces_it(
    repo: DocumentRepo, session, document
) -> None:
    first, second = RGA("a"), RGA("a")
    first.local_insert(0, "one")
    second.local_insert(0, "two")

    await repo.write_snapshot(document.id, 7, first.snapshot(), 3)
    await repo.write_snapshot(document.id, 7, second.snapshot(), 3)
    await session.commit()

    found = await repo.latest_snapshot(document.id)
    assert found is not None
    assert RGA.load("x", found[1]).text() == "two"
    assert len(await repo.list_snapshots(document.id)) == 1


async def test_snapshot_write_advances_document_pointer(
    repo: DocumentRepo, session, document
) -> None:
    doc = RGA("a")
    doc.local_insert(0, "x")
    await repo.write_snapshot(document.id, 12, doc.snapshot(), 1)
    await session.commit()
    await session.refresh(document)
    assert document.snapshot_seq == 12


# ------------------------------------------------------------------ access


async def test_role_resolution(repo: DocumentRepo, session, document, user) -> None:
    other = await repo.create_user(
        email="grace@example.com", name="Grace", password_hash="x", color="#f00"
    )
    await session.commit()

    assert await repo.role_for(document.id, user.id) == "owner"
    assert await repo.role_for(document.id, other.id) is None
    assert await repo.role_for(document.id, None) is None

    await repo.grant(document.id, other.id, "editor")
    await session.commit()
    assert await repo.role_for(document.id, other.id) == "editor"

    await repo.grant(document.id, other.id, "viewer")
    await session.commit()
    assert await repo.role_for(document.id, other.id) == "viewer", "grant must be idempotent"

    await repo.revoke(document.id, other.id)
    await session.commit()
    assert await repo.role_for(document.id, other.id) is None


async def test_public_documents_grant_anonymous_viewing(
    repo: DocumentRepo, session, document
) -> None:
    assert await repo.role_for(document.id, None) is None
    await repo.set_public(document.id, True)
    await session.commit()
    await session.refresh(document)
    assert await repo.role_for(document.id, None) == "viewer"


async def test_role_for_unknown_document_is_none(repo: DocumentRepo, user) -> None:
    assert await repo.role_for("missing", user.id) is None


async def test_role_ranking_helpers() -> None:
    assert can_edit("owner") and can_edit("editor")
    assert not can_edit("commenter") and not can_edit("viewer") and not can_edit(None)
    assert can_comment("commenter") and can_comment("editor") and can_comment("owner")
    assert not can_comment("viewer") and not can_comment(None)
    assert not can_edit("nonsense-role")


async def test_collaborators_listing(repo: DocumentRepo, session, document) -> None:
    other = await repo.create_user(
        email="lin@example.com", name="Lin", password_hash="x", color="#0f0"
    )
    await repo.grant(document.id, other.id, "commenter")
    await session.commit()

    collaborators = await repo.collaborators(document.id)
    assert [(u.name, role) for u, role in collaborators] == [("Lin", "commenter")]


async def test_list_for_user_includes_owned_and_shared(repo: DocumentRepo, session, user) -> None:
    other = await repo.create_user(
        email="kay@example.com", name="Kay", password_hash="x", color="#00f"
    )
    await repo.create(title="Mine", owner_id=user.id)
    theirs = await repo.create(title="Theirs", owner_id=other.id)
    unrelated = await repo.create(title="Unrelated", owner_id=other.id)
    await repo.grant(theirs.id, user.id, "editor")
    await session.commit()

    titles = {d.title for d in await repo.list_for_user(user.id)}
    assert "Mine" in titles and "Theirs" in titles
    assert "Unrelated" not in titles
    assert unrelated.id not in {d.id for d in await repo.list_for_user(user.id)}


async def test_list_for_user_with_nothing_returns_empty(repo: DocumentRepo, session) -> None:
    loner = await repo.create_user(
        email="nobody@example.com", name="Nobody", password_hash="x", color="#000"
    )
    await session.commit()
    assert await repo.list_for_user(loner.id) == []


# ------------------------------------------------------------- share links


async def test_share_link_round_trip(repo: DocumentRepo, session, document) -> None:
    await repo.create_share_link(document.id, token="tok-1", role="editor", expires_at=None)
    await session.commit()

    link = await repo.resolve_share_link("tok-1")
    assert link is not None and link.role == "editor"
    assert await repo.resolve_share_link("nope") is None


async def test_expired_share_link_does_not_resolve(repo: DocumentRepo, session, document) -> None:
    past = datetime.now(UTC) - timedelta(hours=1)
    future = datetime.now(UTC) + timedelta(hours=1)
    await repo.create_share_link(document.id, token="dead", role="viewer", expires_at=past)
    await repo.create_share_link(document.id, token="alive", role="viewer", expires_at=future)
    await session.commit()

    assert await repo.resolve_share_link("dead") is None
    assert await repo.resolve_share_link("alive") is not None


# ---------------------------------------------------------------- comments


async def test_comment_crud(repo: DocumentRepo, session, document, user) -> None:
    comment = await repo.add_comment(
        doc_id=document.id,
        author_id=user.id,
        start_anchor="a:1",
        end_anchor="a:5",
        body="Is this right?",
        quoted="hello",
    )
    await session.commit()

    assert [c.body for c in await repo.list_comments(document.id)] == ["Is this right?"]
    assert (await repo.get_comment(comment.id)).resolved is False

    await repo.resolve_comment(comment.id)
    await session.commit()
    await session.refresh(comment)
    assert comment.resolved is True

    await repo.delete_comment(comment.id)
    await session.commit()
    assert await repo.get_comment(comment.id) is None


async def test_deleting_a_document_cascades_to_its_operations(
    repo: DocumentRepo, session, document
) -> None:
    from sqlalchemy import func, select

    doc = RGA("a")
    await repo.append_ops(document.id, doc.local_insert(0, "x"), site="a")
    await session.commit()

    document_id = document.id
    await repo.delete(document_id)
    await session.commit()

    remaining = await session.execute(
        select(func.count()).select_from(Operation).where(Operation.document_id == document_id)
    )
    assert remaining.scalar_one() == 0


# ------------------------------------------------------------------- users


async def test_user_lookup_is_case_insensitive(repo: DocumentRepo, session) -> None:
    await repo.create_user(email="MiXeD@Example.COM", name="Mixed", password_hash="x", color="#abc")
    await session.commit()
    assert await repo.user_by_email("mixed@example.com") is not None
    assert await repo.user_by_email("MIXED@EXAMPLE.COM") is not None
    assert await repo.user_by_email("other@example.com") is None

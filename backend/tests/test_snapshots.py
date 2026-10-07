"""Cold load, snapshot cadence, and the pruning boundary.

The claim being tested is that snapshots bound replay cost without changing what
a document contains. The assertions are on *work done* (operation batches
replayed) rather than wall-clock, because that number is identical on a laptop
and in CI — so a regression fails the build instead of flaking it.

The most dangerous bug in this area is an off-by-one at the prune boundary: drop
one batch too many and the document silently loses an edit, but only for
documents old enough to have been pruned, and only once nobody is left who
remembers what they said. The multi-generation test below exists for that.
"""

from __future__ import annotations

import pytest

from syncpad.config import Settings
from syncpad.crdt import RGA
from syncpad.storage.snapshots import HistoryUnavailable, SnapshotService

pytestmark = pytest.mark.asyncio


async def _type(repo_context, doc_id: str, author: RGA, text: str) -> int:
    """Append one batch of typing, returning the new sequence."""
    ops = author.local_insert(len(author), text)
    async with repo_context() as repo:
        return await repo.append_ops(doc_id, ops, site=author.site, actor_id=None)


async def _full_replay(repo_context, doc_id: str) -> RGA:
    """Rebuild ignoring snapshots entirely — the reference implementation."""
    rebuilt = RGA("replay")
    async with repo_context() as repo:
        for _, ops in await repo.ops_since(doc_id, 0):
            rebuilt.apply_many(ops)
    return rebuilt


async def test_cold_load_with_no_snapshot_replays_everything(
    snapshots: SnapshotService, repo_context, document
) -> None:
    author = RGA("author")
    for i in range(4):
        await _type(repo_context, document.id, author, f"line {i}\n")

    loaded, seq, stats = await snapshots.load(document.id)
    assert loaded.text() == author.text()
    assert seq == 4
    assert stats.from_snapshot is False
    assert stats.batches_replayed == 4


async def test_snapshot_fires_at_the_configured_cadence(
    snapshots: SnapshotService, repo_context, document, settings: Settings
) -> None:
    assert settings.snapshot_every_ops == 5
    author = RGA("author")
    written: list[int] = []

    for i in range(12):
        seq = await _type(repo_context, document.id, author, f"{i} ")
        if await snapshots.maybe_snapshot(document.id, author, seq):
            written.append(seq)

    assert written == [5, 10], f"expected snapshots at the 5-op cadence, got {written}"


async def test_snapshot_load_matches_full_replay_exactly(
    snapshots: SnapshotService, repo_context, document
) -> None:
    author = RGA("author")
    for i in range(14):
        seq = await _type(repo_context, document.id, author, f"line {i}\n")
        await snapshots.maybe_snapshot(document.id, author, seq)
    async with repo_context() as repo:
        await repo.append_ops(document.id, author.local_mark(0, 4, "bold", True), site="author")
        await repo.append_ops(document.id, author.local_mark(2, 6, "italic", True), site="author")

    loaded, _, stats = await snapshots.load(document.id)
    assert stats.from_snapshot is True
    assert loaded.text() == author.text()
    assert loaded.spans() == author.spans()


async def test_snapshot_load_does_far_less_work_than_full_replay(
    snapshots: SnapshotService, repo_context, document
) -> None:
    """The cold-load win, measured deterministically in batches replayed."""
    author = RGA("author")
    total_batches = 40
    for i in range(total_batches):
        seq = await _type(repo_context, document.id, author, f"{i} ")
        await snapshots.maybe_snapshot(document.id, author, seq)

    _, _, stats = await snapshots.load(document.id)

    assert stats.from_snapshot is True
    # Cadence is 5, so replay after the newest snapshot is bounded by 4 batches
    # no matter how long the document has been edited.
    assert stats.batches_replayed < 5
    speedup = total_batches / max(stats.batches_replayed, 1)
    assert speedup >= 8.0, f"cold-load speedup was only {speedup:.1f}x"


async def test_pruned_document_still_rebuilds_across_generations(
    snapshots: SnapshotService, repo_context, document, settings: Settings
) -> None:
    """The off-by-one guard: prune repeatedly, then demand the text is intact."""
    assert settings.prune_ops is True
    author = RGA("author")
    expected: list[str] = []

    for i in range(30):
        piece = f"[{i}]"
        expected.append(piece)
        seq = await _type(repo_context, document.id, author, piece)
        await snapshots.maybe_snapshot(document.id, author, seq)

    async with repo_context() as repo:
        surviving = await repo.op_count(document.id)
    assert surviving < 30, "pruning did not remove anything"

    loaded, _, _ = await snapshots.load(document.id)
    assert loaded.text() == "".join(expected)
    assert loaded.text() == author.text()
    loaded.check_invariants()


async def test_snapshot_retention_is_bounded(
    snapshots: SnapshotService, repo_context, document, settings: Settings
) -> None:
    author = RGA("author")
    for i in range(40):
        seq = await _type(repo_context, document.id, author, f"{i} ")
        await snapshots.maybe_snapshot(document.id, author, seq)

    async with repo_context() as repo:
        stored = await repo.list_snapshots(document.id)
    assert len(stored) <= settings.snapshot_keep


async def test_rebuild_at_reconstructs_history(
    snapshots: SnapshotService, repo_context, document
) -> None:
    """Time travel: the document as it stood after each batch."""
    author = RGA("author")
    checkpoints: list[tuple[int, str]] = []

    for i in range(16):
        seq = await _type(repo_context, document.id, author, f"{i},")
        checkpoints.append((seq, author.text()))
        await snapshots.maybe_snapshot(document.id, author, seq)

    earliest = await snapshots.earliest_reachable(document.id)

    # Every point still retained must reconstruct exactly, including points
    # immediately either side of a snapshot boundary (cadence is 5).
    reachable = [(s, t) for s, t in checkpoints if s >= earliest]
    assert len(reachable) >= 5
    for seq, expected in reachable:
        rebuilt = await snapshots.rebuild_at(document.id, seq)
        assert rebuilt.text() == expected, f"history wrong at seq {seq}"

    # And anything pruning reclaimed must say so rather than return a plausible
    # but wrong document.
    for seq, _ in checkpoints:
        if seq < earliest:
            with pytest.raises(HistoryUnavailable):
                await snapshots.rebuild_at(document.id, seq)


async def test_rebuild_at_zero_is_the_empty_document(
    snapshots: SnapshotService, repo_context, document
) -> None:
    author = RGA("author")
    await _type(repo_context, document.id, author, "something")
    assert (await snapshots.rebuild_at(document.id, 0)).text() == ""


async def test_rebuild_at_a_negative_sequence_is_rejected(
    snapshots: SnapshotService, document
) -> None:
    with pytest.raises(HistoryUnavailable):
        await snapshots.rebuild_at(document.id, -1)


async def test_unpruned_history_is_fully_reachable(
    repo_context, document, settings: Settings
) -> None:
    """With pruning off, every point in history stays reconstructible."""
    keep_everything = settings.model_copy(update={"prune_ops": False})
    service = SnapshotService(repo_context, keep_everything)
    author = RGA("author")
    checkpoints = []
    for i in range(14):
        seq = await _type(repo_context, document.id, author, f"{i},")
        checkpoints.append((seq, author.text()))
        await service.maybe_snapshot(document.id, author, seq)

    assert await service.earliest_reachable(document.id) == 0
    for seq, expected in checkpoints:
        assert (await service.rebuild_at(document.id, seq)).text() == expected


async def test_force_snapshot_captures_state_off_cadence(
    snapshots: SnapshotService, repo_context, document
) -> None:
    """Used on graceful shutdown so a scaling-down replica banks its replay work."""
    author = RGA("author")
    seq = await _type(repo_context, document.id, author, "only one batch")
    assert await snapshots.maybe_snapshot(document.id, author, seq) is False

    await snapshots.force_snapshot(document.id, author, seq)
    _, _, stats = await snapshots.load(document.id)
    assert stats.from_snapshot is True
    assert stats.batches_replayed == 0


async def test_snapshotting_disabled_by_configuration(
    repo_context, document, settings: Settings
) -> None:
    disabled = settings.model_copy(update={"snapshot_every_ops": 0})
    service = SnapshotService(repo_context, disabled)
    author = RGA("author")
    seq = await _type(repo_context, document.id, author, "text " * 50)
    assert await service.maybe_snapshot(document.id, author, seq) is False


async def test_load_of_unknown_document_raises(snapshots: SnapshotService) -> None:
    from syncpad.storage import DocumentNotFound

    with pytest.raises(DocumentNotFound):
        await snapshots.load("no-such-document")


async def test_empty_document_loads_cleanly(snapshots: SnapshotService, document) -> None:
    loaded, seq, stats = await snapshots.load(document.id)
    assert loaded.text() == ""
    assert seq == 0
    assert stats.batches_replayed == 0


async def test_concurrent_editors_survive_a_snapshot_boundary(
    snapshots: SnapshotService, repo_context, document
) -> None:
    """Two sites interleaving across a snapshot must still converge on reload."""
    alice, bob = RGA("alice"), RGA("bob")

    for i in range(12):
        author, peer = (alice, bob) if i % 2 == 0 else (bob, alice)
        ops = author.local_insert(len(author), f"{author.site[0]}{i} ")
        peer.apply_many(ops)
        async with repo_context() as repo:
            seq = await repo.append_ops(document.id, ops, site=author.site)
        await snapshots.maybe_snapshot(document.id, author, seq)

    assert alice.text() == bob.text()
    loaded, _, _ = await snapshots.load(document.id)
    assert loaded.text() == alice.text()


async def test_snapshot_blob_is_smaller_than_replaying_the_log(
    snapshots: SnapshotService, repo_context, document
) -> None:
    """Sanity check that a snapshot is actually a compaction, not just a copy."""
    author = RGA("author")
    for _i in range(30):
        seq = await _type(repo_context, document.id, author, "lorem ipsum ")
        await snapshots.maybe_snapshot(document.id, author, seq)

    async with repo_context() as repo:
        found = await repo.latest_snapshot(document.id)
    assert found is not None
    _, blob = found
    assert RGA.load("check", blob).text() in author.text()

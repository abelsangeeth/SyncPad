"""Bounding replay cost: why opening an old document is fast.

Without snapshots, opening a document means replaying its entire history. A
document edited for a year replays a year of keystrokes before showing a single
character, and that cost grows without limit while the document itself does not.

A snapshot is a serialised replica state valid as of some sequence number. Cold
load becomes "deserialise the snapshot, replay only what came after it", which
is bounded by the snapshot interval rather than by the document's age. The log
before a snapshot can then be pruned, bounding storage too.

The safety property throughout: **a snapshot is a cache**. Every snapshot in the
database could be deleted and every document would still rebuild correctly from
its operation log, as long as nothing has been pruned. Pruning is what makes
snapshots load-bearing, so it happens only after the snapshot it depends on is
durably written.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager

from syncpad.config import Settings
from syncpad.crdt import RGA
from syncpad.storage.repo import DocumentRepo

__all__ = ["HistoryUnavailable", "LoadStats", "SnapshotService"]

log = logging.getLogger("syncpad.snapshots")


class HistoryUnavailable(LookupError):
    """The requested historical point has been compacted away.

    Surfaced by the API as 410 Gone rather than 404: the version genuinely
    existed, it is simply no longer reconstructible.
    """


RepoContext = Callable[[], AbstractAsyncContextManager[DocumentRepo]]


class LoadStats:
    """What a cold load actually cost, for metrics and for the benchmark.

    Recorded in operation batches rather than seconds because that number is
    deterministic: it is the same on a laptop and in CI, so a regression in
    replay work fails a test instead of flaking one.
    """

    __slots__ = ("batches_replayed", "final_seq", "from_snapshot", "ops_replayed", "snapshot_seq")

    def __init__(self) -> None:
        self.from_snapshot = False
        self.snapshot_seq = 0
        self.batches_replayed = 0
        self.ops_replayed = 0
        self.final_seq = 0

    def __repr__(self) -> str:  # pragma: no cover - diagnostics
        return (
            f"LoadStats(from_snapshot={self.from_snapshot}, snapshot_seq={self.snapshot_seq}, "
            f"batches={self.batches_replayed}, ops={self.ops_replayed}, seq={self.final_seq})"
        )


class SnapshotService:
    """Cold-load and snapshot-cadence policy for documents."""

    def __init__(self, repo_context: RepoContext, settings: Settings) -> None:
        self._repo_context = repo_context
        self._settings = settings

    async def load(self, doc_id: str, *, site: str | None = None) -> tuple[RGA, int, LoadStats]:
        """Rebuild a document: newest snapshot, then only the operations after it."""
        stats = LoadStats()
        async with self._repo_context() as repo:
            document = await repo.require(doc_id)
            snapshot = await repo.latest_snapshot(doc_id)

            if snapshot is not None:
                snapshot_seq, blob = snapshot
                rga = RGA.load(site, blob)
                stats.from_snapshot = True
                stats.snapshot_seq = snapshot_seq
            else:
                snapshot_seq = 0
                rga = RGA(site)

            for seq, ops in await repo.ops_since(doc_id, snapshot_seq):
                rga.apply_many(ops)
                stats.batches_replayed += 1
                stats.ops_replayed += len(ops)
                snapshot_seq = seq

            stats.final_seq = max(snapshot_seq, document.seq)
            return rga, document.seq, stats

    async def rebuild_at(self, doc_id: str, seq: int, *, site: str | None = None) -> RGA:
        """Reconstruct the document exactly as it stood after batch ``seq``.

        This is time travel, and it is why the log is append-only. It reads the
        nearest snapshot at or below ``seq``, then replays forward to ``seq`` and
        no further.

        Raises :class:`HistoryUnavailable` when the requested point predates what
        pruning has left behind. Returning a partial document instead would be
        the worst possible answer: an empty or truncated version that looks like
        a legitimate historical state and would be restored as one.
        """
        if seq < 0:
            raise HistoryUnavailable(f"sequence {seq} is not a valid point in history")

        async with self._repo_context() as repo:
            await repo.require(doc_id)
            snapshot = await repo.snapshot_at_or_before(doc_id, seq)
            if snapshot is not None:
                base_seq, blob = snapshot
                rga = RGA.load(site, blob)
            else:
                base_seq, rga = 0, RGA(site)

            batches = await repo.ops_between(doc_id, base_seq, seq)

            # Sequences are gapless by construction, so a short read means the
            # log was pruned out from under this request.
            if len(batches) != seq - base_seq:
                raise HistoryUnavailable(
                    f"history before sequence {base_seq + len(batches) + 1} has been "
                    f"compacted away; the earliest reachable point is {base_seq}"
                )

            for _, ops in batches:
                rga.apply_many(ops)
            return rga

    async def earliest_reachable(self, doc_id: str) -> int:
        """Oldest sequence :meth:`rebuild_at` can still reconstruct.

        The API surfaces this so a history UI can grey out what pruning has
        reclaimed rather than offering restore points that will fail.
        """
        async with self._repo_context() as repo:
            remaining = await repo.ops_since(doc_id, 0, limit=1)
            oldest_op = remaining[0][0] if remaining else None
            if oldest_op is None or oldest_op == 1:
                return 0  # nothing has been pruned; all of history is intact

            snapshots = await repo.list_snapshots(doc_id)
            oldest_snapshot = min((s.seq for s in snapshots), default=None)
            # Pruning never cuts below the oldest retained snapshot, so that
            # snapshot is exactly the horizon.
            return oldest_snapshot if oldest_snapshot is not None else oldest_op - 1

    async def maybe_snapshot(self, doc_id: str, rga: RGA, seq: int) -> bool:
        """Write a snapshot when enough operations have accumulated.

        Returns True when one was written. Pruning happens only after the write
        has been committed, so an interrupted snapshot can never leave a document
        whose log no longer reaches back far enough to rebuild it.
        """
        interval = self._settings.snapshot_every_ops
        if interval <= 0:
            return False

        async with self._repo_context() as repo:
            document = await repo.require(doc_id)
            if seq - document.snapshot_seq < interval:
                return False

            blob = rga.snapshot()
            await repo.write_snapshot(doc_id, seq, blob, len(rga))
            log.debug(
                "snapshot doc=%s seq=%s bytes=%s elements=%s tombstones=%s",
                doc_id,
                seq,
                len(blob),
                rga.elements,
                rga.tombstones,
            )

        # Second transaction: the snapshot above is durable before anything the
        # log needs is removed.
        if self._settings.prune_ops:
            async with self._repo_context() as repo:
                trimmed = await repo.trim_snapshots(doc_id, self._settings.snapshot_keep)

                # Prune below the OLDEST retained snapshot, not the newest. Cutting
                # at the newest would orphan every older snapshot — they would
                # survive in the table while the operations they need to replay
                # forward were gone, making `snapshot_keep` a lie and every
                # restore point but the last one unreachable.
                retained = await repo.list_snapshots(doc_id)
                floor = min((s.seq for s in retained), default=seq)
                pruned = await repo.prune_ops(doc_id, floor)
                if pruned or trimmed:
                    log.debug(
                        "pruned doc=%s ops<%s=%s snapshots=%s", doc_id, floor, pruned, trimmed
                    )
        return True

    async def force_snapshot(self, doc_id: str, rga: RGA, seq: int) -> None:
        """Snapshot regardless of cadence — used on graceful room shutdown.

        Without this, a replica scaling down would discard the work of replaying
        the log and force the next reader to redo it.
        """
        async with self._repo_context() as repo:
            await repo.write_snapshot(doc_id, seq, rga.snapshot(), len(rga))


async def replay_all(
    repo: DocumentRepo, doc_id: str, site: str | None = None
) -> AsyncIterator[RGA]:
    """Full replay from sequence zero, ignoring snapshots entirely.

    Exists so tests can assert that the snapshot path and the naive path produce
    identical documents — the property that makes snapshots safe to prune behind.
    """
    rga = RGA(site)
    for _, ops in await repo.ops_since(doc_id, 0):
        rga.apply_many(ops)
    yield rga

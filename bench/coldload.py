"""Cold-load cost: how long does opening an old document take?

Without snapshots, opening a document means replaying its entire history, and
that cost grows forever while the document itself does not. A paragraph edited
for a year opens more slowly than the same paragraph created yesterday, for no
reason the user can see.

Snapshots bound it. Cold load becomes "deserialise the latest snapshot, replay
only what came after", which depends on the snapshot interval rather than on the
document's age.

The primary figure reported here is **operations replayed**, not milliseconds.
That number is deterministic — identical on a laptop and in CI — so a regression
fails the build instead of flaking it. Wall-clock is reported alongside it for
human interest, and asserted only loosely.

    python bench/coldload.py --ops 4000 --assert-speedup 3.0
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from syncpad.config import Settings  # noqa: E402
from syncpad.crdt import RGA  # noqa: E402
from syncpad.storage import (  # noqa: E402
    DocumentRepo,
    init_models,
    make_engine,
    make_session_factory,
)
from syncpad.storage.snapshots import SnapshotService  # noqa: E402

SENTENCES = [
    "The gateway is stateless, so any replica can serve any socket. ",
    "Operations are idempotent, which is why duplicate delivery is harmless. ",
    "Tombstones stay until every replica has acknowledged them. ",
    "A snapshot is a cache of a prefix of the log, never the source of truth. ",
]


async def build_history(
    repo_context, doc_id: str, snapshots: SnapshotService, ops: int, max_chars: int
) -> RGA:
    """Write a realistic edit history for a long-lived document.

    Once the document reaches ``max_chars`` it stops growing: further edits
    revise it in place rather than appending. That is what real documents do —
    a specification edited for a year does not end up a year long — and it is
    the case the snapshot design exists for. While the document plateaus, the
    operation log keeps growing, so full-replay cost diverges from the cost of
    loading a snapshot of the current state.
    """
    author = RGA("historian")
    for i in range(ops):
        sentence = SENTENCES[i % len(SENTENCES)]
        if len(author) >= max_chars:
            # Revise in place: delete somewhere, retype somewhere else.
            cut = (i * 37) % max(1, len(author) - len(sentence) - 1)
            batch = author.local_delete(cut, min(len(sentence), len(author) - cut))
            batch += author.local_insert(min(cut, len(author)), sentence)
        elif i and i % 9 == 0 and len(author) > 40:
            batch = author.local_delete(len(author) // 2, 12)
        else:
            batch = author.local_insert(len(author), sentence)

        async with repo_context() as repo:
            seq = await repo.append_ops(doc_id, batch, site=author.site, actor_id=None)
        await snapshots.maybe_snapshot(doc_id, author, seq)
    return author


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ops", type=int, default=2000, help="operation batches to write")
    parser.add_argument("--snapshot-every", type=int, default=200)
    parser.add_argument(
        "--assert-speedup",
        type=float,
        default=None,
        help="minimum replay-work reduction from snapshots",
    )
    parser.add_argument(
        "--max-chars",
        type=int,
        default=20_000,
        help="size the document plateaus at while history grows",
    )
    parser.add_argument("--assert-time-reduction-pct", type=float, default=None)
    args = parser.parse_args()

    workspace = Path(tempfile.mkdtemp(prefix="syncpad-coldload-"))
    settings = Settings(
        env="test",
        database_url=f"sqlite+aiosqlite:///{workspace.as_posix()}/coldload.db",
        snapshot_every_ops=args.snapshot_every,
        # History is kept whole so the "replay everything" baseline is real
        # rather than an artefact of pruning.
        prune_ops=False,
        snapshot_keep=50,
    )

    engine = make_engine(settings)
    await init_models(engine)
    factory = make_session_factory(engine)

    @contextlib.asynccontextmanager
    async def repo_context():
        async with factory() as session:
            repo = DocumentRepo(session)
            try:
                yield repo
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    try:
        async with repo_context() as repo:
            user = await repo.create_user(
                email="cold@example.com", name="Cold", password_hash="x", color="#000"
            )
            document = await repo.create(title="Cold load", owner_id=user.id)
            doc_id = document.id

        snapshots = SnapshotService(repo_context, settings)

        print(f"  writing {args.ops} operation batches…")
        author = await build_history(repo_context, doc_id, snapshots, args.ops, args.max_chars)
        expected = author.text()

        # --- with snapshots -------------------------------------------------
        started = time.perf_counter()
        fast, _, stats = await snapshots.load(doc_id)
        fast_seconds = time.perf_counter() - started

        # --- without: replay the entire log ---------------------------------
        started = time.perf_counter()
        naive = RGA("naive")
        replayed = 0
        async with repo_context() as repo:
            for _, batch in await repo.ops_since(doc_id, 0):
                naive.apply_many(batch)
                replayed += 1
        slow_seconds = time.perf_counter() - started

        if fast.text() != expected or naive.text() != expected:
            print("FAIL: a cold load did not reproduce the document", file=sys.stderr)
            return 1

        work_speedup = round(replayed / max(stats.batches_replayed, 1), 2)
        time_speedup = round(slow_seconds / fast_seconds, 2) if fast_seconds > 0 else 0

        summary = {
            "document_chars": len(expected),
            "operation_batches": replayed,
            "snapshot_every_ops": args.snapshot_every,
            "with_snapshot": {
                "batches_replayed": stats.batches_replayed,
                "from_snapshot": stats.from_snapshot,
                "seconds": round(fast_seconds, 4),
            },
            "full_replay": {
                "batches_replayed": replayed,
                "seconds": round(slow_seconds, 4),
            },
            "replay_work_speedup": work_speedup,
            "wall_clock_speedup": time_speedup,
            "cold_load_time_reduction_pct": (
                round((1 - fast_seconds / slow_seconds) * 100, 1) if slow_seconds else 0
            ),
        }
        print(json.dumps(summary, indent=2))

        out = ROOT / "bench" / "results"
        out.mkdir(parents=True, exist_ok=True)
        (out / "coldload.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

        print(
            f"\n  full replay : {replayed} batches in {slow_seconds * 1000:.0f}ms\n"
            f"  snapshot    : {stats.batches_replayed} batches in {fast_seconds * 1000:.0f}ms\n"
            f"  replay work reduced {work_speedup}x, cold-load time reduced "
            f"{summary['cold_load_time_reduction_pct']}%"
        )

        reduction = summary["cold_load_time_reduction_pct"]
        if (
            args.assert_time_reduction_pct is not None
            and reduction < args.assert_time_reduction_pct
        ):
            print(
                f"FAIL: cold-load time reduction {reduction}% below the "
                f"required {args.assert_time_reduction_pct}%",
                file=sys.stderr,
            )
            return 1

        if args.assert_speedup is not None and work_speedup < args.assert_speedup:
            print(
                f"FAIL: replay-work speedup {work_speedup}x below the "
                f"required {args.assert_speedup}x",
                file=sys.stderr,
            )
            return 1
        return 0
    finally:
        await engine.dispose()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

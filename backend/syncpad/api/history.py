"""Version history and restore, without ever rewriting the log.

Restoring an old version is the interesting part. The obvious implementation —
delete the operations after the chosen point — is wrong in a distributed system:
another replica may hold operations you just deleted, a client may be offline
with unacknowledged edits that descend from them, and the "restore" would race
both. It also destroys the audit trail.

So restore is an *edit*, not a rewind. SyncPad diffs the current text against
the historical text and appends the CRDT operations that transform one into the
other. The log only ever grows, the restore is itself a restorable point, and
concurrent editors see it arrive as ordinary operations that merge the way every
other operation does.
"""

from __future__ import annotations

import difflib
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, HTTPException, Path, Query, Request, status
from pydantic import BaseModel

from syncpad.api.auth import CurrentUser, OptionalUser
from syncpad.api.documents import require_role
from syncpad.crdt import RGA, Op
from syncpad.storage import DocumentRepo
from syncpad.storage.snapshots import HistoryUnavailable

__all__ = ["diff_to_ops", "router"]

router = APIRouter(prefix="/api/documents/{doc_id}/history", tags=["history"])


class RevisionOut(BaseModel):
    seq: int
    created_at: datetime
    site: str
    author_id: str | None
    author_name: str | None
    ops: int
    is_snapshot: bool


class HistoryOut(BaseModel):
    doc_id: str
    seq: int
    earliest_reachable: int
    revisions: list[RevisionOut]


class VersionOut(BaseModel):
    seq: int
    text: str
    chars: int


class RestoreOut(BaseModel):
    seq: int
    restored_from: int
    text: str
    ops_applied: int


@router.get("", response_model=HistoryOut)
async def list_history(
    doc_id: str,
    request: Request,
    user: OptionalUser,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> HistoryOut:
    """Recent revisions, newest first, annotated with snapshot boundaries."""
    await require_role(request, doc_id, user, "viewer")
    repo = DocumentRepo(request.state.session)
    document = await repo.require(doc_id)

    batches = await repo.op_log(doc_id, limit=limit)
    snapshot_seqs = {s.seq for s in await repo.list_snapshots(doc_id)}

    names: dict[str, str] = {}
    for batch in batches:
        if batch.actor_id and batch.actor_id not in names:
            author = await repo.user_by_id(batch.actor_id)
            names[batch.actor_id] = author.name if author else "Unknown"

    return HistoryOut(
        doc_id=doc_id,
        seq=document.seq,
        earliest_reachable=await request.app.state.snapshots.earliest_reachable(doc_id),
        revisions=[
            RevisionOut(
                seq=b.seq,
                created_at=b.created_at,
                site=b.site,
                author_id=b.actor_id,
                author_name=names.get(b.actor_id or ""),
                ops=len(b.payload or []),
                is_snapshot=b.seq in snapshot_seqs,
            )
            for b in batches
        ],
    )


@router.get("/at/{seq}", response_model=VersionOut)
async def version_at(
    doc_id: str,
    request: Request,
    user: OptionalUser,
    seq: Annotated[int, Path(ge=0)],
) -> VersionOut:
    """The document exactly as it stood after operation batch ``seq``."""
    await require_role(request, doc_id, user, "viewer")
    try:
        rga = await request.app.state.snapshots.rebuild_at(doc_id, seq)
    except HistoryUnavailable as exc:
        # 410 rather than 404: this version existed, it is simply no longer
        # reconstructible because compaction reclaimed the operations.
        raise HTTPException(status.HTTP_410_GONE, str(exc)) from exc
    return VersionOut(seq=seq, text=rga.text(), chars=len(rga))


@router.post("/restore/{seq}", response_model=RestoreOut)
async def restore(
    doc_id: str,
    request: Request,
    user: CurrentUser,
    seq: Annotated[int, Path(ge=0)],
) -> RestoreOut:
    """Append the operations that turn the current text back into version ``seq``."""
    await require_role(request, doc_id, user, "editor")

    try:
        historical = await request.app.state.snapshots.rebuild_at(doc_id, seq)
    except HistoryUnavailable as exc:
        raise HTTPException(status.HTTP_410_GONE, str(exc)) from exc

    room = request.app.state.rooms.get(doc_id)
    target_text = historical.text()

    if room is not None:
        # The document is live: edit the in-memory replica so the operations are
        # generated against the state every connected editor currently holds.
        ops = diff_to_ops(room.rga, target_text)
        if not ops:
            return RestoreOut(seq=room.seq, restored_from=seq, text=target_text, ops_applied=0)
        new_seq = await room.submit(ops, site=room.rga.site, actor_id=user.id)
        return RestoreOut(
            seq=new_seq, restored_from=seq, text=room.rga.text(), ops_applied=len(ops)
        )

    current, current_seq, _ = await request.app.state.snapshots.load(doc_id)
    ops = diff_to_ops(current, target_text)
    if not ops:
        return RestoreOut(seq=current_seq, restored_from=seq, text=target_text, ops_applied=0)

    repo = DocumentRepo(request.state.session)
    new_seq = await repo.append_ops(doc_id, ops, site=current.site, actor_id=user.id)
    await request.state.session.commit()
    return RestoreOut(seq=new_seq, restored_from=seq, text=current.text(), ops_applied=len(ops))


def diff_to_ops(rga: RGA, target: str) -> list[Op]:
    """Operations transforming ``rga``'s current text into ``target``.

    Uses a real diff rather than "delete everything and retype it" so that a
    restore touching one paragraph produces a handful of operations instead of
    tombstoning the whole document — which would both bloat the log and destroy
    every comment anchor and concurrent edit in the file.

    Operations are generated back-to-front so that each index is still valid
    when its operation is applied: editing from the front would shift every
    subsequent position.
    """
    source = rga.text()
    if source == target:
        return []

    matcher = difflib.SequenceMatcher(None, source, target, autojunk=False)
    ops: list[Op] = []

    for tag, i1, i2, j1, j2 in reversed(matcher.get_opcodes()):
        if tag == "equal":
            continue
        if tag in ("replace", "delete"):
            ops.extend(rga.local_delete(i1, i2 - i1))
        if tag in ("replace", "insert"):
            ops.extend(rga.local_insert(i1, target[j1:j2]))

    return ops

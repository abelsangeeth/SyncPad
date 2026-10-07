"""Comments anchored to characters, not to offsets.

A comment stored as "characters 40 to 55" is wrong the moment anyone types above
it — and in a collaborative editor, someone always does, often while the comment
is being written. SyncPad stores the :class:`~syncpad.crdt.OpId` of the first and
last character instead. Those ids never change and never move, so the anchor
tracks its text through any amount of concurrent editing, by anyone, forever.

When the anchored text is finally deleted the comment is reported as
``orphaned`` rather than deleted or silently relocated. Relocating it would
attach someone's remark to text they never read; deleting it would destroy a
discussion because one participant removed a sentence.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException, Request, Response, status
from pydantic import BaseModel, Field

from syncpad.api.auth import CurrentUser, OptionalUser
from syncpad.api.documents import load_document, require_role
from syncpad.crdt import RGA, OpId
from syncpad.storage import DocumentRepo

__all__ = ["router"]

router = APIRouter(prefix="/api/documents/{doc_id}/comments", tags=["comments"])


class CommentIn(BaseModel):
    start: str = Field(description="OpId of the first anchored character")
    end: str = Field(description="OpId of the last anchored character")
    body: str = Field(min_length=1, max_length=8000)
    parent_id: str | None = None


class CommentOut(BaseModel):
    id: str
    document_id: str
    author_id: str
    author_name: str
    author_color: str
    body: str
    resolved: bool
    parent_id: str | None
    created_at: datetime
    start: str
    end: str
    #: The text as it read when the comment was made.
    quoted: str
    #: The text the anchors currently span; empty once fully deleted.
    current_text: str
    #: Where the anchor sits now, for rendering the highlight.
    start_index: int | None
    end_index: int | None
    #: True when every anchored character has been deleted.
    orphaned: bool


def _resolve(rga: RGA, start_raw: str, end_raw: str) -> tuple[int | None, int | None, str, bool]:
    """Locate a comment's anchors in the live document."""
    try:
        start_id, end_id = OpId.decode(start_raw), OpId.decode(end_raw)
    except ValueError:
        return None, None, "", True

    if not rga.contains(start_id) or not rga.contains(end_id):
        # The anchoring characters have been garbage collected, or this replica
        # has not caught up yet. Either way the anchor cannot be drawn.
        return None, None, "", True

    current = rga.text_between(start_id, end_id)
    start_index = rga.anchor_index(start_id)
    end_index = rga.anchor_index(end_id)
    # Orphaned only when there is no visible text left between the anchors.
    return start_index, end_index, current, current == ""


async def _to_out(repo: DocumentRepo, comment, rga: RGA) -> CommentOut:
    author = await repo.user_by_id(comment.author_id)
    start_index, end_index, current, orphaned = _resolve(
        rga, comment.start_anchor, comment.end_anchor
    )
    return CommentOut(
        id=comment.id,
        document_id=comment.document_id,
        author_id=comment.author_id,
        author_name=author.name if author else "Unknown",
        author_color=author.color if author else "#94a3b8",
        body=comment.body,
        resolved=comment.resolved,
        parent_id=comment.parent_id,
        created_at=comment.created_at,
        start=comment.start_anchor,
        end=comment.end_anchor,
        quoted=comment.quoted,
        current_text=current,
        start_index=start_index,
        end_index=end_index,
        orphaned=orphaned,
    )


@router.get("", response_model=list[CommentOut])
async def list_comments(doc_id: str, request: Request, user: OptionalUser) -> list[CommentOut]:
    await require_role(request, doc_id, user, "viewer")
    repo = DocumentRepo(request.state.session)
    rga = await load_document(request, doc_id)
    return [await _to_out(repo, c, rga) for c in await repo.list_comments(doc_id)]


@router.post("", response_model=CommentOut, status_code=status.HTTP_201_CREATED)
async def create_comment(
    doc_id: str, payload: CommentIn, request: Request, user: CurrentUser
) -> CommentOut:
    await require_role(request, doc_id, user, "commenter")
    repo = DocumentRepo(request.state.session)
    rga = await load_document(request, doc_id)

    try:
        start_id, end_id = OpId.decode(payload.start), OpId.decode(payload.end)
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"bad anchor: {exc}") from exc

    if not rga.contains(start_id) or not rga.contains(end_id):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "anchors do not exist in this document")

    if payload.parent_id is not None:
        parent = await repo.get_comment(payload.parent_id)
        if parent is None or parent.document_id != doc_id:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "unknown parent comment")

    comment = await repo.add_comment(
        doc_id=doc_id,
        author_id=user.id,
        start_anchor=payload.start,
        end_anchor=payload.end,
        body=payload.body,
        quoted=rga.text_between(start_id, end_id),
        parent_id=payload.parent_id,
    )
    await request.state.session.commit()
    return await _to_out(repo, comment, rga)


@router.post("/{comment_id}/resolve", response_model=CommentOut)
async def resolve_comment(
    doc_id: str, comment_id: str, request: Request, user: CurrentUser
) -> CommentOut:
    await require_role(request, doc_id, user, "commenter")
    repo = DocumentRepo(request.state.session)

    comment = await repo.get_comment(comment_id)
    if comment is None or comment.document_id != doc_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "comment not found")

    await repo.resolve_comment(comment_id, not comment.resolved)
    await request.state.session.commit()
    await request.state.session.refresh(comment)
    return await _to_out(repo, comment, await load_document(request, doc_id))


@router.delete("/{comment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_comment(
    doc_id: str, comment_id: str, request: Request, user: CurrentUser
) -> Response:
    repo = DocumentRepo(request.state.session)
    comment = await repo.get_comment(comment_id)
    if comment is None or comment.document_id != doc_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "comment not found")

    # An author may always delete their own comment; otherwise it takes ownership
    # of the document.
    if comment.author_id != user.id:
        await require_role(request, doc_id, user, "owner")
    else:
        await require_role(request, doc_id, user, "commenter")

    await repo.delete_comment(comment_id)
    await request.state.session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)

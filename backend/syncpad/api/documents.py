"""Document CRUD, sharing, and export.

Reads go through the live room when one exists, so a document being actively
edited exports what is on screen rather than what was last snapshotted. When no
replica holds the document, it is cold-loaded for the request and dropped again.

Export is driven by :meth:`RGA.spans`, so formatting survives the round trip —
and the HTML exporter escapes, because a collaborative editor is a place where
one user's text becomes another user's page.
"""

from __future__ import annotations

import secrets
from datetime import UTC, datetime, timedelta
from html import escape
from typing import Annotated, Literal

from fastapi import APIRouter, HTTPException, Query, Request, Response, status
from pydantic import BaseModel, EmailStr, Field

from syncpad.api.auth import CurrentUser, OptionalUser, UserOut
from syncpad.crdt import RGA, Span
from syncpad.storage import DocumentRepo

__all__ = ["load_document", "router", "to_html", "to_markdown", "to_plain"]

router = APIRouter(prefix="/api/documents", tags=["documents"])


# ------------------------------------------------------------------ schemas


class DocumentOut(BaseModel):
    id: str
    title: str
    owner_id: str
    is_public: bool
    seq: int
    created_at: datetime
    updated_at: datetime
    role: str | None = None


class DocumentDetail(DocumentOut):
    text: str
    chars: int


class CreateIn(BaseModel):
    title: str = Field(default="Untitled", max_length=300)
    text: str = Field(default="", max_length=200_000)


class RenameIn(BaseModel):
    title: str = Field(min_length=1, max_length=300)


class PublicIn(BaseModel):
    is_public: bool


class ShareIn(BaseModel):
    role: Literal["viewer", "commenter", "editor"] = "viewer"
    expires_in_hours: int | None = Field(default=None, ge=1, le=24 * 365)


class ShareOut(BaseModel):
    token: str
    url_path: str
    role: str
    expires_at: datetime | None


class GrantIn(BaseModel):
    email: EmailStr
    role: Literal["viewer", "commenter", "editor"] = "editor"


class CollaboratorOut(BaseModel):
    user: UserOut
    role: str


# ------------------------------------------------------------------ helpers


async def load_document(request: Request, doc_id: str) -> RGA:
    """The live replica if one is loaded here, otherwise a fresh cold load.

    Preferring the live room is not just an optimisation: it is what makes a
    read consistent with what editors currently see, rather than with the last
    persisted snapshot.
    """
    room = request.app.state.rooms.get(doc_id)
    if room is not None:
        return room.rga
    rga, _seq, _stats = await request.app.state.snapshots.load(doc_id)
    return rga


async def require_role(request: Request, doc_id: str, user, minimum: str) -> str:
    """Resolve the caller's role, or raise the right HTTP error."""
    from syncpad.storage import ROLE_RANK

    repo = DocumentRepo(request.state.session)
    if await repo.get(doc_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "document not found")

    role = await repo.role_for(doc_id, user.id if user else None)
    if role is None:
        # 404 rather than 403 for a document the caller cannot see: a 403 would
        # confirm that the id exists, which is an enumeration oracle.
        raise HTTPException(status.HTTP_404_NOT_FOUND, "document not found")
    if ROLE_RANK.get(role, -1) < ROLE_RANK[minimum]:
        raise HTTPException(status.HTTP_403_FORBIDDEN, f"this action requires '{minimum}' access")
    return role


# ------------------------------------------------------------------- routes


@router.get("", response_model=list[DocumentOut])
async def list_documents(request: Request, user: CurrentUser) -> list[DocumentOut]:
    repo = DocumentRepo(request.state.session)
    documents = await repo.list_for_user(user.id)
    return [
        DocumentOut(
            id=d.id,
            title=d.title,
            owner_id=d.owner_id,
            is_public=d.is_public,
            seq=d.seq,
            created_at=d.created_at,
            updated_at=d.updated_at,
            role="owner" if d.owner_id == user.id else await repo.role_for(d.id, user.id),
        )
        for d in documents
    ]


@router.post("", response_model=DocumentOut, status_code=status.HTTP_201_CREATED)
async def create_document(payload: CreateIn, request: Request, user: CurrentUser) -> DocumentOut:
    repo = DocumentRepo(request.state.session)
    document = await repo.create(title=payload.title, owner_id=user.id)

    if payload.text:
        # Seed content goes through the CRDT like any other edit, so an imported
        # document has a real operation history rather than a mystery origin.
        seed = RGA(f"seed-{document.id[:8]}")
        ops = seed.local_insert(0, payload.text)
        await repo.append_ops(document.id, ops, site=seed.site, actor_id=user.id)

    await request.state.session.commit()
    return DocumentOut(
        id=document.id,
        title=document.title,
        owner_id=document.owner_id,
        is_public=document.is_public,
        seq=document.seq,
        created_at=document.created_at,
        updated_at=document.updated_at,
        role="owner",
    )


@router.get("/{doc_id}", response_model=DocumentDetail)
async def get_document(doc_id: str, request: Request, user: OptionalUser) -> DocumentDetail:
    role = await require_role(request, doc_id, user, "viewer")
    repo = DocumentRepo(request.state.session)
    document = await repo.require(doc_id)
    rga = await load_document(request, doc_id)
    return DocumentDetail(
        id=document.id,
        title=document.title,
        owner_id=document.owner_id,
        is_public=document.is_public,
        seq=document.seq,
        created_at=document.created_at,
        updated_at=document.updated_at,
        role=role,
        text=rga.text(),
        chars=len(rga),
    )


@router.patch("/{doc_id}", response_model=DocumentOut)
async def rename_document(
    doc_id: str, payload: RenameIn, request: Request, user: CurrentUser
) -> DocumentOut:
    await require_role(request, doc_id, user, "editor")
    repo = DocumentRepo(request.state.session)
    await repo.rename(doc_id, payload.title)
    await request.state.session.commit()
    document = await repo.require(doc_id)
    return DocumentOut(
        id=document.id,
        title=document.title,
        owner_id=document.owner_id,
        is_public=document.is_public,
        seq=document.seq,
        created_at=document.created_at,
        updated_at=document.updated_at,
    )


@router.put("/{doc_id}/public", response_model=DocumentOut)
async def set_public(
    doc_id: str, payload: PublicIn, request: Request, user: CurrentUser
) -> DocumentOut:
    await require_role(request, doc_id, user, "owner")
    repo = DocumentRepo(request.state.session)
    await repo.set_public(doc_id, payload.is_public)
    await request.state.session.commit()
    document = await repo.require(doc_id)
    return DocumentOut(
        id=document.id,
        title=document.title,
        owner_id=document.owner_id,
        is_public=document.is_public,
        seq=document.seq,
        created_at=document.created_at,
        updated_at=document.updated_at,
    )


@router.delete("/{doc_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_document(doc_id: str, request: Request, user: CurrentUser) -> Response:
    await require_role(request, doc_id, user, "owner")
    repo = DocumentRepo(request.state.session)
    await repo.delete(doc_id)
    await request.state.session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# ------------------------------------------------------------------ sharing


@router.post("/{doc_id}/share", response_model=ShareOut, status_code=status.HTTP_201_CREATED)
async def create_share_link(
    doc_id: str, payload: ShareIn, request: Request, user: CurrentUser
) -> ShareOut:
    await require_role(request, doc_id, user, "owner")
    repo = DocumentRepo(request.state.session)

    token = secrets.token_urlsafe(24)
    expires = (
        datetime.now(UTC) + timedelta(hours=payload.expires_in_hours)
        if payload.expires_in_hours
        else None
    )
    link = await repo.create_share_link(doc_id, token=token, role=payload.role, expires_at=expires)
    await request.state.session.commit()
    return ShareOut(
        token=link.token,
        url_path=f"/?doc={doc_id}&share={link.token}",
        role=link.role,
        expires_at=link.expires_at,
    )


@router.post("/{doc_id}/collaborators", response_model=CollaboratorOut)
async def grant_access(
    doc_id: str, payload: GrantIn, request: Request, user: CurrentUser
) -> CollaboratorOut:
    await require_role(request, doc_id, user, "owner")
    repo = DocumentRepo(request.state.session)

    target = await repo.user_by_email(payload.email)
    if target is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no user with that email")
    if target.id == user.id:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "you already own this document")

    await repo.grant(doc_id, target.id, payload.role)
    await request.state.session.commit()
    return CollaboratorOut(user=UserOut.of(target), role=payload.role)


@router.get("/{doc_id}/collaborators", response_model=list[CollaboratorOut])
async def list_collaborators(
    doc_id: str, request: Request, user: CurrentUser
) -> list[CollaboratorOut]:
    await require_role(request, doc_id, user, "viewer")
    repo = DocumentRepo(request.state.session)
    return [
        CollaboratorOut(user=UserOut.of(u), role=role)
        for u, role in await repo.collaborators(doc_id)
    ]


@router.delete("/{doc_id}/collaborators/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_access(doc_id: str, user_id: str, request: Request, user: CurrentUser) -> Response:
    await require_role(request, doc_id, user, "owner")
    repo = DocumentRepo(request.state.session)
    await repo.revoke(doc_id, user_id)
    await request.state.session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# ------------------------------------------------------------------- export


@router.get("/{doc_id}/export")
async def export_document(
    doc_id: str,
    request: Request,
    user: OptionalUser,
    format: Annotated[Literal["md", "html", "txt"], Query()] = "md",
) -> Response:
    await require_role(request, doc_id, user, "viewer")
    repo = DocumentRepo(request.state.session)
    document = await repo.require(doc_id)
    rga = await load_document(request, doc_id)
    spans = rga.spans()

    if format == "md":
        body, media = to_markdown(spans), "text/markdown; charset=utf-8"
        suffix = "md"
    elif format == "html":
        body, media = to_html(spans, document.title), "text/html; charset=utf-8"
        suffix = "html"
    else:
        body, media = to_plain(spans), "text/plain; charset=utf-8"
        suffix = "txt"

    safe_title = "".join(c for c in document.title if c.isalnum() or c in " -_")[:80].strip()
    filename = f"{safe_title or 'document'}.{suffix}"
    return Response(
        content=body,
        media_type=media,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def to_plain(spans: list[Span]) -> str:
    return "".join(s.text for s in spans)


def to_markdown(spans: list[Span]) -> str:
    """Render marks as inline Markdown.

    Marks are applied innermost-first in a fixed order so that identical
    formatting always produces byte-identical Markdown — otherwise exporting the
    same document twice could yield ``**_x_**`` and ``_**x**_`` and look like a
    change in version control.
    """
    out: list[str] = []
    for span in spans:
        text = span.text
        marks = span.marks

        if marks.get("code"):
            text = f"`{text}`"
        if marks.get("italic"):
            text = f"*{text}*"
        if marks.get("bold"):
            text = f"**{text}**"
        if marks.get("strike"):
            text = f"~~{text}~~"
        link = marks.get("link")
        if isinstance(link, str) and link:
            text = f"[{text}]({link})"
        heading = marks.get("heading")
        if heading:
            level = int(heading) if str(heading).isdigit() else 1
            text = f"{'#' * max(1, min(level, 6))} {text}"
        out.append(text)
    return "".join(out)


def to_html(spans: list[Span], title: str = "Document") -> str:
    """Render to a standalone HTML document.

    Every piece of user-supplied text — the body and the title — is escaped.
    This is the one export path where a mistake is a stored-XSS vulnerability
    rather than a formatting bug, because the text was written by one user and
    is opened by another.
    """
    parts: list[str] = []
    for span in spans:
        text = escape(span.text).replace("\n", "<br>\n")
        marks = span.marks

        if marks.get("code"):
            text = f"<code>{text}</code>"
        if marks.get("italic"):
            text = f"<em>{text}</em>"
        if marks.get("bold"):
            text = f"<strong>{text}</strong>"
        if marks.get("strike"):
            text = f"<s>{text}</s>"
        link = marks.get("link")
        if isinstance(link, str) and link:
            safe = escape(link, quote=True)
            if not safe.lower().startswith(("http://", "https://", "mailto:")):
                # Anything else — javascript:, data: — is rendered as plain text.
                text = f"{text} ({safe})"
            else:
                text = f'<a href="{safe}" rel="noopener noreferrer">{text}</a>'
        heading = marks.get("heading")
        if heading:
            level = int(heading) if str(heading).isdigit() else 1
            level = max(1, min(level, 6))
            text = f"<h{level}>{text}</h{level}>"
        parts.append(text)

    return (
        '<!doctype html>\n<html lang="en">\n<head>\n'
        '<meta charset="utf-8">\n'
        f"<title>{escape(title)}</title>\n"
        "<style>body{max-width:42rem;margin:3rem auto;padding:0 1rem;"
        "font:16px/1.7 ui-sans-serif,system-ui,sans-serif;color:#1f2430}"
        "code{background:#f1f3f7;padding:.1em .3em;border-radius:4px}</style>\n"
        f"</head>\n<body>\n<article>{''.join(parts)}</article>\n</body>\n</html>\n"
    )

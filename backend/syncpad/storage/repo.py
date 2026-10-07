"""Data access for documents, their operation log, and everything hanging off it.

The one genuinely delicate method here is :meth:`DocumentRepo.append_ops`. Every
gateway replica appends to the same log with no coordination between them, so
sequence allocation has to be atomic at the database rather than in Python. It
is done with a single ``UPDATE … RETURNING``, which both Postgres and modern
SQLite execute as one indivisible statement — no ``SELECT FOR UPDATE``, no
advisory lock, no application-level mutex that would only work within one
process anyway.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from syncpad.crdt import Op, ops_from_dicts, ops_to_dicts
from syncpad.storage.models import (
    Comment,
    Document,
    DocumentACL,
    Operation,
    ShareLink,
    Snapshot,
    User,
)

__all__ = ["DocumentNotFound", "DocumentRepo"]


class DocumentNotFound(LookupError):
    """Raised when an operation targets a document that does not exist."""


class DocumentRepo:
    """All persistence for one request or one WebSocket append.

    Holds a session; does not own it. Callers control the transaction boundary,
    because the gateway wants one short transaction per submitted frame while
    the REST API wants one per request.
    """

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    # ------------------------------------------------------------ documents
    async def create(self, *, title: str, owner_id: str) -> Document:
        document = Document(title=title, owner_id=owner_id)
        self.session.add(document)
        await self.session.flush()
        return document

    async def get(self, doc_id: str) -> Document | None:
        return await self.session.get(Document, doc_id)

    async def require(self, doc_id: str) -> Document:
        document = await self.get(doc_id)
        if document is None:
            raise DocumentNotFound(doc_id)
        return document

    async def list_for_user(self, user_id: str) -> list[Document]:
        """Documents a user owns or has been granted access to, newest first."""
        owned = select(Document).where(Document.owner_id == user_id)
        shared = (
            select(Document)
            .join(DocumentACL, DocumentACL.document_id == Document.id)
            .where(DocumentACL.user_id == user_id)
        )
        result = await self.session.execute(
            owned.union(shared).order_by(Document.updated_at.desc())
        )
        # union() yields rows, not entities, so re-hydrate by primary key.
        ids = [row[0] for row in result.all()]
        if not ids:
            return []
        entities = await self.session.execute(
            select(Document).where(Document.id.in_(ids)).order_by(Document.updated_at.desc())
        )
        return list(entities.scalars().all())

    async def rename(self, doc_id: str, title: str) -> None:
        await self.session.execute(
            update(Document)
            .where(Document.id == doc_id)
            .values(title=title, updated_at=datetime.now(UTC))
        )

    async def set_public(self, doc_id: str, is_public: bool) -> None:
        await self.session.execute(
            update(Document).where(Document.id == doc_id).values(is_public=is_public)
        )

    async def delete(self, doc_id: str) -> None:
        await self.session.execute(delete(Document).where(Document.id == doc_id))

    # ------------------------------------------------------------ op log
    async def append_ops(
        self,
        doc_id: str,
        ops: Sequence[Op],
        *,
        site: str,
        actor_id: str | None = None,
    ) -> int:
        """Append a batch of operations and return its sequence number.

        Atomic against concurrent writers on any replica: the sequence is
        allocated by the database in the same statement that bumps it. The retry
        exists for the SQLite case, where a busy writer surfaces as a transient
        ``OperationalError`` rather than as a wait.
        """
        if not ops:
            return (await self.require(doc_id)).seq

        payload = ops_to_dicts(list(ops))
        now = datetime.now(UTC)

        for attempt in range(3):
            try:
                result = await self.session.execute(
                    update(Document)
                    .where(Document.id == doc_id)
                    .values(seq=Document.seq + 1, updated_at=now)
                    .returning(Document.seq)
                )
                row = result.first()
                if row is None:
                    raise DocumentNotFound(doc_id)
                seq = int(row[0])

                self.session.add(
                    Operation(
                        document_id=doc_id,
                        seq=seq,
                        payload=payload,
                        site=site,
                        actor_id=actor_id,
                        created_at=now,
                    )
                )
                await self.session.flush()
                return seq
            except (OperationalError, IntegrityError):
                await self.session.rollback()
                if attempt == 2:
                    raise

        raise RuntimeError("unreachable")  # pragma: no cover

    async def ops_since(
        self, doc_id: str, seq: int, *, limit: int | None = None
    ) -> list[tuple[int, list[Op]]]:
        """Every operation batch after ``seq``, oldest first."""
        stmt = (
            select(Operation.seq, Operation.payload)
            .where(Operation.document_id == doc_id, Operation.seq > seq)
            .order_by(Operation.seq)
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        rows = await self.session.execute(stmt)
        return [(int(s), ops_from_dicts(p)) for s, p in rows.all()]

    async def ops_between(self, doc_id: str, low: int, high: int) -> list[tuple[int, list[Op]]]:
        """Operation batches with ``low < seq <= high`` — used by time travel."""
        rows = await self.session.execute(
            select(Operation.seq, Operation.payload)
            .where(
                Operation.document_id == doc_id,
                Operation.seq > low,
                Operation.seq <= high,
            )
            .order_by(Operation.seq)
        )
        return [(int(s), ops_from_dicts(p)) for s, p in rows.all()]

    async def op_count(self, doc_id: str) -> int:
        result = await self.session.execute(
            select(func.count()).select_from(Operation).where(Operation.document_id == doc_id)
        )
        return int(result.scalar_one())

    async def op_log(self, doc_id: str, *, limit: int = 100) -> list[Operation]:
        """Recent batches as entities, for the version-history listing."""
        rows = await self.session.execute(
            select(Operation)
            .where(Operation.document_id == doc_id)
            .order_by(Operation.seq.desc())
            .limit(limit)
        )
        return list(rows.scalars().all())

    async def prune_ops(self, doc_id: str, upto_seq: int) -> int:
        """Drop operations a snapshot has superseded.

        Strictly ``< upto_seq``: the batch *at* the snapshot's sequence is what
        the snapshot was taken after, and time travel to that exact point still
        wants to read it.
        """
        result = await self.session.execute(
            delete(Operation).where(Operation.document_id == doc_id, Operation.seq < upto_seq)
        )
        return int(result.rowcount or 0)

    # ------------------------------------------------------------ snapshots
    async def latest_snapshot(self, doc_id: str) -> tuple[int, bytes] | None:
        row = await self.session.execute(
            select(Snapshot.seq, Snapshot.blob)
            .where(Snapshot.document_id == doc_id)
            .order_by(Snapshot.seq.desc())
            .limit(1)
        )
        found = row.first()
        return (int(found[0]), found[1]) if found else None

    async def snapshot_at_or_before(self, doc_id: str, seq: int) -> tuple[int, bytes] | None:
        """Nearest snapshot not newer than ``seq`` — the time-travel entry point."""
        row = await self.session.execute(
            select(Snapshot.seq, Snapshot.blob)
            .where(Snapshot.document_id == doc_id, Snapshot.seq <= seq)
            .order_by(Snapshot.seq.desc())
            .limit(1)
        )
        found = row.first()
        return (int(found[0]), found[1]) if found else None

    async def write_snapshot(self, doc_id: str, seq: int, blob: bytes, text_len: int) -> None:
        existing = await self.session.get(Snapshot, {"document_id": doc_id, "seq": seq})
        if existing is not None:
            existing.blob = blob
            existing.text_len = text_len
        else:
            self.session.add(Snapshot(document_id=doc_id, seq=seq, blob=blob, text_len=text_len))
        await self.session.execute(
            update(Document).where(Document.id == doc_id).values(snapshot_seq=seq)
        )
        await self.session.flush()

    async def list_snapshots(self, doc_id: str) -> list[Snapshot]:
        rows = await self.session.execute(
            select(Snapshot).where(Snapshot.document_id == doc_id).order_by(Snapshot.seq.desc())
        )
        return list(rows.scalars().all())

    async def trim_snapshots(self, doc_id: str, keep: int) -> int:
        """Retain only the ``keep`` newest snapshots."""
        rows = await self.session.execute(
            select(Snapshot.seq)
            .where(Snapshot.document_id == doc_id)
            .order_by(Snapshot.seq.desc())
            .offset(max(keep, 1))
        )
        doomed = [int(s) for (s,) in rows.all()]
        if not doomed:
            return 0
        await self.session.execute(
            delete(Snapshot).where(Snapshot.document_id == doc_id, Snapshot.seq.in_(doomed))
        )
        return len(doomed)

    # ------------------------------------------------------------ access
    async def role_for(self, doc_id: str, user_id: str | None) -> str | None:
        """Effective role, or None for no access at all."""
        document = await self.get(doc_id)
        if document is None:
            return None
        if user_id is not None:
            if document.owner_id == user_id:
                return "owner"
            grant = await self.session.get(DocumentACL, {"document_id": doc_id, "user_id": user_id})
            if grant is not None:
                return grant.role
        return "viewer" if document.is_public else None

    async def grant(self, doc_id: str, user_id: str, role: str) -> None:
        existing = await self.session.get(DocumentACL, {"document_id": doc_id, "user_id": user_id})
        if existing is not None:
            existing.role = role
        else:
            self.session.add(DocumentACL(document_id=doc_id, user_id=user_id, role=role))
        await self.session.flush()

    async def revoke(self, doc_id: str, user_id: str) -> None:
        await self.session.execute(
            delete(DocumentACL).where(
                DocumentACL.document_id == doc_id, DocumentACL.user_id == user_id
            )
        )

    async def collaborators(self, doc_id: str) -> list[tuple[User, str]]:
        rows = await self.session.execute(
            select(User, DocumentACL.role)
            .join(DocumentACL, DocumentACL.user_id == User.id)
            .where(DocumentACL.document_id == doc_id)
        )
        return [(user, role) for user, role in rows.all()]

    # ------------------------------------------------------------ share links
    async def create_share_link(
        self, doc_id: str, *, token: str, role: str, expires_at: datetime | None
    ) -> ShareLink:
        link = ShareLink(token=token, document_id=doc_id, role=role, expires_at=expires_at)
        self.session.add(link)
        await self.session.flush()
        return link

    async def resolve_share_link(self, token: str) -> ShareLink | None:
        link = await self.session.get(ShareLink, token)
        if link is None:
            return None
        if link.expires_at is not None:
            expiry = link.expires_at
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=UTC)
            if expiry < datetime.now(UTC):
                return None
        return link

    # ------------------------------------------------------------ comments
    async def add_comment(
        self,
        *,
        doc_id: str,
        author_id: str,
        start_anchor: str,
        end_anchor: str,
        body: str,
        quoted: str,
        parent_id: str | None = None,
    ) -> Comment:
        comment = Comment(
            document_id=doc_id,
            author_id=author_id,
            start_anchor=start_anchor,
            end_anchor=end_anchor,
            body=body,
            quoted=quoted,
            parent_id=parent_id,
        )
        self.session.add(comment)
        await self.session.flush()
        return comment

    async def list_comments(self, doc_id: str) -> list[Comment]:
        rows = await self.session.execute(
            select(Comment).where(Comment.document_id == doc_id).order_by(Comment.created_at)
        )
        return list(rows.scalars().all())

    async def get_comment(self, comment_id: str) -> Comment | None:
        return await self.session.get(Comment, comment_id)

    async def resolve_comment(self, comment_id: str, resolved: bool = True) -> None:
        await self.session.execute(
            update(Comment).where(Comment.id == comment_id).values(resolved=resolved)
        )

    async def delete_comment(self, comment_id: str) -> None:
        await self.session.execute(delete(Comment).where(Comment.id == comment_id))

    # ------------------------------------------------------------ users
    async def user_by_email(self, email: str) -> User | None:
        rows = await self.session.execute(select(User).where(User.email == email.lower()))
        return rows.scalar_one_or_none()

    async def user_by_id(self, user_id: str) -> User | None:
        return await self.session.get(User, user_id)

    async def create_user(self, *, email: str, name: str, password_hash: str, color: str) -> User:
        user = User(email=email.lower(), name=name, password_hash=password_hash, color=color)
        self.session.add(user)
        await self.session.flush()
        return user

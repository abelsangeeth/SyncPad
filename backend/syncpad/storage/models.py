"""The durable schema.

Two ideas shape it. First, the operation log is **append-only**: SyncPad never
rewrites history, not even when a user restores an old version — a restore
appends the operations that transform the present into the past. That makes the
log an audit trail for free and makes every read reproducible.

Second, snapshots exist purely as a **cache of a prefix of that log**. Deleting
every snapshot row would cost cold-load latency and nothing else. Keeping that
property means a snapshot bug can never be a data-loss bug.

Primary keys are UUID strings rather than a native UUID column so the identical
DDL runs on Postgres and on SQLite, which is what lets the whole test suite run
without a database server.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import JSON

__all__ = [
    "ROLES",
    "ROLE_RANK",
    "Base",
    "Comment",
    "Document",
    "DocumentACL",
    "Operation",
    "ShareLink",
    "Snapshot",
    "User",
    "can_comment",
    "can_edit",
]

#: Roles in ascending order of authority.
ROLES = ("viewer", "commenter", "editor", "owner")
ROLE_RANK = {role: i for i, role in enumerate(ROLES)}


def can_edit(role: str | None) -> bool:
    return role is not None and ROLE_RANK.get(role, -1) >= ROLE_RANK["editor"]


def can_comment(role: str | None) -> bool:
    return role is not None and ROLE_RANK.get(role, -1) >= ROLE_RANK["commenter"]


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    email: Mapped[str] = mapped_column(String(320), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(120))
    password_hash: Mapped[str] = mapped_column(String(256))
    #: Stable presence colour, derived from the id so a user looks the same to
    #: everyone in every document without needing to be stored per-session.
    color: Mapped[str] = mapped_column(String(9), default="#6366f1")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    documents: Mapped[list[Document]] = relationship(
        back_populates="owner", cascade="all, delete-orphan"
    )


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    title: Mapped[str] = mapped_column(String(300), default="Untitled")
    owner_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    #: Highest operation sequence number issued for this document. Monotonic,
    #: gapless, and allocated by an atomic UPDATE ... RETURNING so that several
    #: gateway replicas can append concurrently without coordinating.
    seq: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    #: Sequence covered by the newest snapshot; 0 means "no snapshot yet".
    snapshot_seq: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)

    is_public: Mapped[bool] = mapped_column(Boolean, default=False)

    owner: Mapped[User] = relationship(back_populates="documents")
    acl: Mapped[list[DocumentACL]] = relationship(
        back_populates="document", cascade="all, delete-orphan"
    )


class DocumentACL(Base):
    """Per-user access grants. Absence of a row means no access, unless public."""

    __tablename__ = "document_acl"

    document_id: Mapped[str] = mapped_column(
        ForeignKey("documents.id", ondelete="CASCADE"), primary_key=True
    )
    user_id: Mapped[str] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), primary_key=True
    )
    role: Mapped[str] = mapped_column(String(16), default="editor")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    document: Mapped[Document] = relationship(back_populates="acl")


class Operation(Base):
    """One batch of CRDT operations, as submitted in a single WebSocket frame.

    Batching by frame rather than by operation keeps the log short — typing a
    word is one row — and makes replay a bulk read instead of a row-per-keystroke
    scan. ``payload`` holds the encoded operation list.
    """

    __tablename__ = "operations"

    document_id: Mapped[str] = mapped_column(
        ForeignKey("documents.id", ondelete="CASCADE"), primary_key=True
    )
    seq: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    payload: Mapped[list] = mapped_column(JSON)
    site: Mapped[str] = mapped_column(String(40))
    actor_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    __table_args__ = (Index("ix_operations_doc_seq", "document_id", "seq"),)


class Snapshot(Base):
    """A materialised replica state, valid as of ``seq``.

    Purely an optimisation: see this module's docstring.
    """

    __tablename__ = "snapshots"

    document_id: Mapped[str] = mapped_column(
        ForeignKey("documents.id", ondelete="CASCADE"), primary_key=True
    )
    seq: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    blob: Mapped[bytes] = mapped_column(LargeBinary)
    text_len: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class Comment(Base):
    """A comment anchored to character ids rather than to offsets.

    Offsets would rot the instant anyone typed above the comment. Anchoring to
    the ids of the first and last character means the anchor tracks the text it
    was attached to, through concurrent edits, forever — and when that text is
    finally deleted the comment is reported as orphaned rather than silently
    relocated onto whatever now occupies those offsets.
    """

    __tablename__ = "comments"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    document_id: Mapped[str] = mapped_column(
        ForeignKey("documents.id", ondelete="CASCADE"), index=True
    )
    author_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    start_anchor: Mapped[str] = mapped_column(String(48))
    end_anchor: Mapped[str] = mapped_column(String(48))
    #: The text as it read when the comment was made, for display when the
    #: anchor range has since been deleted.
    quoted: Mapped[str] = mapped_column(Text, default="")
    body: Mapped[str] = mapped_column(Text)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    parent_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class ShareLink(Base):
    """A capability URL granting a fixed role without an account."""

    __tablename__ = "share_links"

    token: Mapped[str] = mapped_column(String(64), primary_key=True)
    document_id: Mapped[str] = mapped_column(
        ForeignKey("documents.id", ondelete="CASCADE"), index=True
    )
    role: Mapped[str] = mapped_column(String(16), default="viewer")
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    # No explicit unique constraint on `token`: it is already the primary key.
    # Declaring one anyway is harmless at runtime but leaves `alembic check`
    # permanently reporting a diff, because Postgres never materialises a
    # redundant constraint for the migration to find.

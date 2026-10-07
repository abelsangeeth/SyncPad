"""Durable state: the append-only operation log and its snapshot cache."""

from __future__ import annotations

from syncpad.storage.db import (
    init_models,
    is_sqlite,
    make_engine,
    make_session_factory,
    normalise_url,
)
from syncpad.storage.dsn import DsnPlan, plan_connection, pooling_mode
from syncpad.storage.models import (
    ROLE_RANK,
    ROLES,
    Base,
    Comment,
    Document,
    DocumentACL,
    Operation,
    ShareLink,
    Snapshot,
    User,
    can_comment,
    can_edit,
)
from syncpad.storage.repo import DocumentNotFound, DocumentRepo
from syncpad.storage.snapshots import LoadStats, SnapshotService

__all__ = [
    "ROLES",
    "ROLE_RANK",
    "Base",
    "Comment",
    "Document",
    "DocumentACL",
    "DocumentNotFound",
    "DocumentRepo",
    "DsnPlan",
    "LoadStats",
    "Operation",
    "ShareLink",
    "Snapshot",
    "SnapshotService",
    "User",
    "can_comment",
    "can_edit",
    "init_models",
    "is_sqlite",
    "make_engine",
    "make_session_factory",
    "normalise_url",
    "plan_connection",
    "pooling_mode",
]

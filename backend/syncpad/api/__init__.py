"""HTTP surface: auth, documents, version history, comments, and ops endpoints."""

from __future__ import annotations

from syncpad.api import auth, comments, documents, history, metrics

__all__ = ["auth", "comments", "documents", "history", "metrics"]

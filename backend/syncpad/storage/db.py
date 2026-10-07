"""Engine and session construction — the one place the database dialect matters.

The rest of the codebase is written against the SQLAlchemy async API and does
not know whether it is talking to Postgres or SQLite. That is deliberate: it is
what lets the full test suite, including the WebSocket and snapshot tests, run
on a laptop with no database server while the same code runs on Postgres in
production.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from syncpad.config import Settings
from syncpad.storage.dsn import DsnPlan, normalise_url, plan_connection, pooling_mode
from syncpad.storage.models import Base

__all__ = [
    "DsnPlan",
    "drop_models",
    "init_models",
    "is_sqlite",
    "make_engine",
    "make_session_factory",
    "normalise_url",
    "plan_connection",
    "pooling_mode",
]


def is_sqlite(url: str) -> bool:
    return normalise_url(url).startswith("sqlite")


def _is_memory_sqlite(url: str) -> bool:
    return is_sqlite(url) and (":memory:" in url or "mode=memory" in url)


def make_engine(settings: Settings) -> AsyncEngine:
    """Build the async engine, with pooling appropriate to the backend."""
    plan = plan_connection(settings.database_url)
    url = plan.url
    kwargs: dict[str, object] = {"echo": settings.db_echo, "future": True}

    if _is_memory_sqlite(url):
        # Every session must land on the SAME connection, or each one gets a
        # private empty database and the tests become a ghost story.
        kwargs["poolclass"] = StaticPool
        kwargs["connect_args"] = {"check_same_thread": False}
    elif is_sqlite(url):
        kwargs["connect_args"] = {"check_same_thread": False}
    else:
        kwargs.update(
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_timeout=30,
            # Recycle below the typical cloud-proxy idle cut so a pooled
            # connection is never handed out already dead.
            pool_recycle=1800,
            pool_pre_ping=True,
        )
        if plan.connect_args:
            # Translated libpq parameters, plus the prepared-statement opt-out
            # that transaction-mode pooling requires. See storage/dsn.py.
            kwargs["connect_args"] = plan.connect_args

    engine = create_async_engine(url, **kwargs)
    if is_sqlite(url):
        _enable_sqlite_foreign_keys(engine)
    return engine


def _enable_sqlite_foreign_keys(engine: AsyncEngine) -> None:
    """Turn on foreign key enforcement, which SQLite disables by default.

    Without this, ``ON DELETE CASCADE`` is parsed and then ignored: deleting a
    document leaves its operations, snapshots and comments behind as orphans.
    Postgres enforces constraints unprompted, so the omission would show up only
    on the developer laptops and in the test suite — exactly the places where it
    is most likely to be mistaken for correct behaviour.
    """

    @event.listens_for(engine.sync_engine, "connect")
    def _set_pragmas(dbapi_connection, _record) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA foreign_keys=ON")
            # WAL lets readers proceed during a write, which matters for the
            # single-process dev server serving sockets and HTTP at once.
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=5000")
        finally:
            cursor.close()


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,  # keep ORM objects usable after commit
        autoflush=False,
    )


async def init_models(engine: AsyncEngine) -> None:
    """Create tables if absent.

    Fine for dev, tests and the Docker demo stack. Production migrations are
    Alembic's job — see ``backend/alembic/``.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def drop_models(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


async def session_dependency(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """Yield a session that rolls back on error and always closes."""
    async with factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise

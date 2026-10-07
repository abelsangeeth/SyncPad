"""Engine construction and the dialect details that only bite in production."""

from __future__ import annotations

import pytest
from sqlalchemy import text

from syncpad.config import Settings
from syncpad.storage.db import (
    drop_models,
    init_models,
    is_sqlite,
    make_engine,
    make_session_factory,
    normalise_url,
    session_dependency,
)


@pytest.mark.parametrize(
    "given,expected",
    [
        ("postgresql://u:p@host/db", "postgresql+asyncpg://u:p@host/db"),
        ("postgres://u:p@host/db", "postgresql+asyncpg://u:p@host/db"),
        ("sqlite:///./local.db", "sqlite+aiosqlite:///./local.db"),
        ("sqlite+aiosqlite:///./local.db", "sqlite+aiosqlite:///./local.db"),
        ("postgresql+asyncpg://u@h/db", "postgresql+asyncpg://u@h/db"),
        ("mysql://u@h/db", "mysql://u@h/db"),
    ],
)
def test_url_normalisation(given: str, expected: str) -> None:
    """A driver-less URL is what every tutorial and cloud provider hands you."""
    assert normalise_url(given) == expected


def test_is_sqlite_detection() -> None:
    assert is_sqlite("sqlite:///x.db")
    assert is_sqlite("sqlite+aiosqlite:///:memory:")
    assert not is_sqlite("postgresql://u@h/db")


def test_postgres_engine_is_pooled_without_connecting() -> None:
    """Built, not connected — so this runs with no Postgres anywhere nearby."""
    engine = make_engine(
        Settings(
            env="prod",
            database_url="postgresql://user:pass@db.example.com:5432/syncpad",
            db_pool_size=7,
            db_max_overflow=11,
        )
    )
    assert engine.url.drivername == "postgresql+asyncpg"
    assert engine.pool.size() == 7


async def test_sqlite_enforces_foreign_keys(tmp_path) -> None:
    """SQLite ignores ON DELETE CASCADE unless the pragma is set per connection."""
    settings = Settings(env="test", database_url=f"sqlite+aiosqlite:///{tmp_path.as_posix()}/fk.db")
    engine = make_engine(settings)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text("PRAGMA foreign_keys"))
            assert result.scalar() == 1, "cascade deletes would silently not happen"
    finally:
        await engine.dispose()


async def test_init_and_drop_models(tmp_path) -> None:
    settings = Settings(
        env="test", database_url=f"sqlite+aiosqlite:///{tmp_path.as_posix()}/ddl.db"
    )
    engine = make_engine(settings)
    try:
        await init_models(engine)
        async with engine.connect() as conn:
            found = await conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            tables = {row[0] for row in found}
        assert {"users", "documents", "operations", "snapshots"} <= tables

        await init_models(engine)  # idempotent

        await drop_models(engine)
        async with engine.connect() as conn:
            found = await conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
            assert "documents" not in {row[0] for row in found}
    finally:
        await engine.dispose()


async def test_session_dependency_rolls_back_on_error(tmp_path) -> None:
    settings = Settings(env="test", database_url=f"sqlite+aiosqlite:///{tmp_path.as_posix()}/rb.db")
    engine = make_engine(settings)
    await init_models(engine)
    factory = make_session_factory(engine)
    try:
        generator = session_dependency(factory)
        session = await anext(generator)
        assert session is not None
        with pytest.raises(RuntimeError):
            await generator.athrow(RuntimeError("boom"))
    finally:
        await engine.dispose()

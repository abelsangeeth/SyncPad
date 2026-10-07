"""Shared fixtures.

The whole suite runs against in-memory SQLite and the in-process hub, with no
database server and no Redis. That is a deliberate constraint on the design, not
just a testing convenience: if a feature cannot be exercised without Postgres,
it has leaked dialect-specific behaviour somewhere it should not have.

Tests that genuinely need the real thing are marked ``integration`` and run in
CI against live Postgres and Redis services.
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import AsyncIterator, Callable

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from syncpad.config import Settings
from syncpad.storage import DocumentRepo, init_models, make_engine, make_session_factory
from syncpad.storage.snapshots import SnapshotService

pytest_plugins = ("pytest_asyncio",)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: needs live Postgres/Redis")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if os.environ.get("SYNCPAD_INTEGRATION") == "1":
        return
    skip = pytest.mark.skip(reason="set SYNCPAD_INTEGRATION=1 to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture
def settings(tmp_path) -> Settings:
    """Test configuration: a private SQLite file, no Redis, eager snapshots.

    A file rather than ``:memory:``. An in-memory SQLite database lives inside a
    single connection, so every session has to share one via StaticPool — and
    when several WebSocket sessions and HTTP requests contend for it, one lock
    error invalidates that connection and the pool reopens a brand new, EMPTY
    database underneath the running test. A file per test costs microseconds,
    exercises the real pooling path, and cannot vanish.
    """
    return Settings(
        env="test",
        database_url=f"sqlite+aiosqlite:///{tmp_path.as_posix()}/syncpad-test.db",
        redis_url=None,
        jwt_secret="test-secret-not-for-production",
        snapshot_every_ops=5,
        prune_ops=True,
        snapshot_keep=3,
        presence_ttl_s=5.0,
        ws_idle_timeout_s=5.0,
        serve_frontend=False,
    )


@pytest_asyncio.fixture
async def engine(settings: Settings) -> AsyncIterator[AsyncEngine]:
    engine = make_engine(settings)
    await init_models(engine)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return make_session_factory(engine)


@pytest_asyncio.fixture
async def session(session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[AsyncSession]:
    async with session_factory() as s:
        yield s


@pytest_asyncio.fixture
async def repo(session: AsyncSession) -> DocumentRepo:
    return DocumentRepo(session)


@pytest.fixture
def repo_context(
    session_factory: async_sessionmaker[AsyncSession],
) -> Callable[[], contextlib.AbstractAsyncContextManager[DocumentRepo]]:
    """A fresh repo in its own committed transaction, as the gateway uses it."""

    @contextlib.asynccontextmanager
    async def make() -> AsyncIterator[DocumentRepo]:
        async with session_factory() as s:
            repo = DocumentRepo(s)
            try:
                yield repo
                await s.commit()
            except Exception:
                await s.rollback()
                raise

    return make


@pytest.fixture
def snapshots(repo_context, settings: Settings) -> SnapshotService:
    return SnapshotService(repo_context, settings)


@pytest_asyncio.fixture
async def user(repo: DocumentRepo, session: AsyncSession):
    created = await repo.create_user(
        email="ada@example.com", name="Ada", password_hash="x", color="#6366f1"
    )
    await session.commit()
    return created


@pytest_asyncio.fixture
async def document(repo: DocumentRepo, session: AsyncSession, user):
    doc = await repo.create(title="Test Document", owner_id=user.id)
    await session.commit()
    return doc


# --------------------------------------------------------------- application


@pytest.fixture
def raw_app(settings: Settings):
    """A constructed app whose lifespan has NOT been entered.

    Starlette's TestClient runs the lifespan itself on ``__enter__``. Handing it
    an app that is already inside its lifespan builds a second engine, hub and
    RoomManager over the first, and unwinds them in the wrong order on exit.
    """
    from syncpad.gateway.hub import InMemoryHub
    from syncpad.main import create_app

    InMemoryHub.reset_brokers()
    return create_app(settings)


@pytest_asyncio.fixture
async def app(raw_app):
    """A fully wired app with its lifespan run, for async HTTP tests."""
    async with raw_app.router.lifespan_context(raw_app):
        yield raw_app


@pytest_asyncio.fixture
async def client(app) -> AsyncIterator[httpx.AsyncClient]:
    import httpx

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        yield c


@pytest_asyncio.fixture
async def make_user(client):
    """Register a user and return (headers, user dict, raw token)."""
    counter = {"n": 0}

    async def _make(
        email: str | None = None,
        name: str = "Test User",
        password: str = "hunter2-hunter2",
    ):
        counter["n"] += 1
        address = email or f"user{counter['n']}@example.com"
        response = await client.post(
            "/api/auth/register", json={"email": address, "name": name, "password": password}
        )
        assert response.status_code == 201, response.text
        body = response.json()
        token = body["access_token"]
        return {"Authorization": f"Bearer {token}"}, body["user"], token

    return _make


@pytest_asyncio.fixture
async def auth_client(client, make_user):
    """A client pre-authenticated as a registered user."""
    headers, user, token = await make_user(email="owner@example.com", name="Owner")
    client.headers.update(headers)
    client.syncpad_user = user
    client.syncpad_token = token
    return client


@pytest_asyncio.fixture
async def owned_doc(auth_client):
    """A document owned by ``auth_client``'s user, returned as its id."""
    response = await auth_client.post("/api/documents", json={"title": "Fixture Doc"})
    assert response.status_code == 201, response.text
    return response.json()["id"]


@pytest.fixture
def ws_client(raw_app):
    """A synchronous Starlette test client — the transport that supports WebSockets.

    Takes ``raw_app`` rather than ``app`` because TestClient owns the lifespan.
    """
    from starlette.testclient import TestClient

    with TestClient(raw_app) as tc:
        yield tc

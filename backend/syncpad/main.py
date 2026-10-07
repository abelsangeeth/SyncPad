"""Application assembly and lifecycle.

Every shared resource is built in the lifespan and torn down in reverse order.
Ordering matters on shutdown: rooms close first so that each one can bank its
final snapshot, then the hub (whose fan-out the rooms were using), then the
database engine (which the snapshot writes needed). Disposing the engine first
would make every closing room's snapshot fail — quietly, and only in production,
where shutdown actually happens.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import AsyncIterator
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from syncpad.api import auth, comments, documents, history, metrics
from syncpad.config import Settings, get_settings
from syncpad.gateway import ws
from syncpad.gateway.hub import make_hub
from syncpad.gateway.presence import make_presence
from syncpad.services.rooms import RoomManager
from syncpad.storage import DocumentRepo, init_models, make_engine, make_session_factory
from syncpad.storage.snapshots import SnapshotService

__all__ = ["app", "create_app"]

log = logging.getLogger("syncpad")


def find_frontend(
    explicit: Path | None = None,
    *,
    module: Path | None = None,
    cwd: Path | None = None,
) -> Path | None:
    """Locate the static frontend, or return None if it is not present.

    ``parents[2]`` alone is correct only in a source checkout, where this module
    is ``<root>/backend/syncpad/main.py`` and the sibling of ``backend`` is
    ``frontend``. It is wrong for an installed package: in the container the
    module sits at ``/app/syncpad/main.py``, so ``parents[2]`` is ``/`` and the
    files at ``/app/frontend`` are missed. The result was a 404 on ``/`` in every
    container, which went unnoticed because the compose stack puts nginx in
    front and nginx proxies ``/`` to the gateway rather than serving files, and
    because the load tests speak to the API directly.

    Requiring ``index.html`` rather than just a directory means a stale or empty
    ``frontend/`` cannot be mounted to serve nothing.
    """
    # ``module`` and ``cwd`` are injectable so each candidate position can be
    # tested; the bug this function fixes was precisely that only one of them
    # was ever exercised.
    here = (module or Path(__file__)).resolve()
    candidates: list[Path] = []
    if explicit is not None:
        candidates.append(Path(explicit))
    candidates += [
        here.parents[2] / "frontend",  # source checkout
        here.parents[1] / "frontend",  # installed alongside the package
        (cwd or Path.cwd()) / "frontend",  # container WORKDIR
    ]
    for candidate in candidates:
        if (candidate / "index.html").is_file():
            return candidate
    return None


#: Retained for callers that imported it; prefer :func:`find_frontend`.
FRONTEND_DIR = Path(__file__).resolve().parents[2] / "frontend"


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = make_engine(settings)
        session_factory = make_session_factory(engine)

        if settings.env in ("dev", "test"):
            # Production schema changes go through Alembic; this is for laptops,
            # the test suite, and the Docker demo stack.
            await init_models(engine)

        @contextlib.asynccontextmanager
        async def repo_context() -> AsyncIterator[DocumentRepo]:
            """A repo in its own committed transaction, for non-request callers."""
            async with session_factory() as session:
                repo = DocumentRepo(session)
                try:
                    yield repo
                    await session.commit()
                except Exception:
                    await session.rollback()
                    raise

        hub = make_hub(settings)
        await hub.start()
        presence = make_presence(settings)
        snapshots = SnapshotService(repo_context, settings)
        rooms = RoomManager(
            hub=hub,
            presence=presence,
            snapshots=snapshots,
            settings=settings,
            repo_context=repo_context,
        )

        app.state.settings = settings
        app.state.engine = engine
        app.state.session_factory = session_factory
        app.state.repo_context = repo_context
        app.state.hub = hub
        app.state.presence = presence
        app.state.snapshots = snapshots
        app.state.rooms = rooms
        app.state.metrics = metrics.Metrics()

        log.info(
            "syncpad up env=%s db=%s hub=%s",
            settings.env,
            settings.database_url.split("://", 1)[0],
            "redis" if settings.uses_redis else "in-process",
        )

        try:
            yield
        finally:
            # Reverse construction order; see the module docstring.
            await rooms.shutdown()
            with contextlib.suppress(Exception):
                await presence.close()
            with contextlib.suppress(Exception):
                await hub.close()
            await engine.dispose()

    app = FastAPI(
        title="SyncPad",
        version="1.0.0",
        summary="Real-time collaborative document editor built on an RGA CRDT",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def attach_session(request: Request, call_next):
        """One session per HTTP request, rolled back and closed unconditionally.

        Routes commit explicitly when they mean to. Anything that raises past
        them gets rolled back here rather than leaving a half-applied
        transaction on a pooled connection for the next request to inherit.
        """
        # Deliberately excludes /readyz. That endpoint's whole job is to report
        # a broken database, so it must not be behind a middleware that opens a
        # session first — otherwise a dead database turns readiness into a 500
        # and the operator learns nothing about which dependency failed.
        if request.url.path.startswith("/api/"):
            async with request.app.state.session_factory() as session:
                request.state.session = session
                try:
                    return await call_next(request)
                except Exception:
                    await session.rollback()
                    raise
        return await call_next(request)

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled error on %s %s", request.method, request.url.path)
        return JSONResponse(status_code=500, content={"detail": "internal server error"})

    app.include_router(auth.router)
    app.include_router(documents.router)
    app.include_router(history.router)
    app.include_router(comments.router)
    app.include_router(metrics.router)
    app.include_router(ws.router)

    if settings.serve_frontend:
        frontend = find_frontend(settings.frontend_dir)
        if frontend is not None:
            log.info("serving frontend from %s", frontend)
            # Mounted last so it cannot shadow an API route.
            app.mount("/", StaticFiles(directory=str(frontend), html=True), name="app")
        else:
            # Said out loud, because the symptom is a 404 on "/" while every
            # API route works, which reads as a routing bug rather than a
            # missing directory.
            log.warning(
                "serve_frontend is enabled but no frontend/index.html was found; "
                "the API will work and / will return 404. Set SYNCPAD_FRONTEND_DIR."
            )

    return app


#: Module-level app for ``uvicorn syncpad.main:app --factory``.
app = create_app

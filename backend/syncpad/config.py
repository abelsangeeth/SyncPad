"""Runtime settings.

Every adapter seam in SyncPad is chosen here and nowhere else: Postgres vs
SQLite, Redis vs in-process pub/sub. That is what lets the same code run as a
single process on a laptop and as N stateless replicas behind a load balancer.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

#: Kept as a named constant so prod can detect that nobody chose it.
DEFAULT_DATABASE_URL = "sqlite+aiosqlite:///./syncpad.db"


class Settings(BaseSettings):
    env: Literal["dev", "test", "prod"] = "dev"

    # --- storage -------------------------------------------------------
    database_url: str = DEFAULT_DATABASE_URL
    db_echo: bool = False
    db_pool_size: int = 10
    db_max_overflow: int = 20

    # --- fan-out -------------------------------------------------------
    # None => InMemoryHub + MemoryPresence (single process, no Redis needed).
    redis_url: str | None = None

    # --- auth ----------------------------------------------------------
    jwt_secret: str = "dev-insecure-change-me"
    jwt_alg: str = "HS256"
    access_token_minutes: int = 60 * 24

    # --- durability ----------------------------------------------------
    snapshot_every_ops: int = 200
    prune_ops: bool = True
    #: Snapshots retained per document. Older ones are dropped, so snapshot
    #: storage is bounded while version history keeps several restore points.
    snapshot_keep: int = 5

    # --- realtime ------------------------------------------------------
    presence_ttl_s: float = 30.0
    ws_idle_timeout_s: float = 45.0
    ws_handshake_timeout_s: float = 10.0
    #: Above this many buffered operations, a reconnecting client is sent a
    #: fresh snapshot instead of a delta — past that point the delta is the
    #: more expensive of the two.
    max_catchup_ops: int = 2000
    rate_ops_per_s: float = 120.0
    rate_burst: int = 400
    max_doc_bytes: int = 4 * 1024 * 1024

    # --- http ----------------------------------------------------------
    cors_origins: list[str] = ["*"]
    serve_frontend: bool = True
    #: Override where the static frontend is read from. Normally discovered.
    frontend_dir: Path | None = None

    model_config = SettingsConfigDict(env_prefix="SYNCPAD_", env_file=".env", extra="ignore")

    @model_validator(mode="after")
    def _require_an_explicit_database_in_prod(self) -> Settings:
        """Refuse to run production on the development default.

        The default is SQLite in the working directory. On a container host that
        is an ephemeral filesystem: the server starts, /readyz reports the
        database is fine, documents save, and every one of them is discarded on
        the next restart. A misconfigured deployment that looks healthy is worse
        than one that will not boot, so this is a hard failure rather than a
        warning nobody reads.
        """
        if self.env == "prod" and self.database_url == DEFAULT_DATABASE_URL:
            raise ValueError(
                "SYNCPAD_DATABASE_URL must be set explicitly when SYNCPAD_ENV=prod. "
                "Leaving it unset falls back to SQLite in the working directory, "
                "which on a container host is discarded when the container "
                "restarts, while still reporting itself healthy. Set a Postgres "
                "URL, or pass the SQLite path explicitly if that is truly intended."
            )
        return self

    @property
    def uses_redis(self) -> bool:
        return bool(self.redis_url)


@lru_cache
def get_settings() -> Settings:
    return Settings()

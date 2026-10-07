"""Alembic environment.

The database URL comes from ``Settings`` rather than from alembic.ini, so a
migration cannot accidentally run against a different database than the
application — a mismatch that is easy to create and unpleasant to discover.
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from sqlalchemy.ext.asyncio import async_engine_from_config
from sqlalchemy.pool import NullPool

from alembic import context
from syncpad.config import get_settings
from syncpad.storage.db import plan_connection
from syncpad.storage.models import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

#: Managed providers hand out libpq URLs whose parameters asyncpg rejects, so
#: the same translation the application uses is applied here — otherwise
#: ``alembic upgrade head`` fails against a database the app connects to fine.
_plan = plan_connection(get_settings().database_url)

# ConfigParser reads "%" as interpolation syntax. A generated password
# containing one would blow up on this line with a InterpolationSyntaxError
# that says nothing about passwords, so escape it.
config.set_main_option("sqlalchemy.url", _plan.url.replace("%", "%%"))


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def _run(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    engine = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=NullPool,
        connect_args=_plan.connect_args,
    )
    async with engine.connect() as connection:
        await connection.run_sync(_run)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())

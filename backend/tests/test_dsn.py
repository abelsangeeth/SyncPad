"""Connection-string translation.

These tests exist because of one failure mode: SQLAlchemy's asyncpg dialect
forwards the URL query string straight into ``asyncpg.connect()``, and asyncpg
accepts none of libpq's parameter names. The URL Supabase prints in its
dashboard therefore raises ``TypeError: connect() got an unexpected keyword
argument 'sslmode'`` — an error that mentions neither TLS nor the provider.

The assertions below are about the *plan*, not about a live database, so they
are exact and run everywhere. One integration test at the bottom proves the plan
actually connects.
"""

from __future__ import annotations

import pytest

from syncpad.storage.dsn import normalise_url, plan_connection, pooling_mode

SUPABASE_POOLER_TX = (
    "postgresql://postgres.abcdefghijklmnop:s3cret"
    "@aws-0-ap-south-1.pooler.supabase.com:6543/postgres"
)
SUPABASE_POOLER_SESSION = (
    "postgresql://postgres.abcdefghijklmnop:s3cret"
    "@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"
)
SUPABASE_DIRECT = "postgresql://postgres:s3cret@db.abcdefghijklmnop.supabase.co:5432/postgres"


# --- the headline bug ---------------------------------------------------


def test_sslmode_is_translated_not_forwarded() -> None:
    """``sslmode`` must never reach asyncpg, under any name but ``ssl``."""
    plan = plan_connection(SUPABASE_DIRECT + "?sslmode=require")

    assert "sslmode" not in plan.url, "would become an unexpected kwarg"
    assert "sslmode" not in plan.connect_args
    assert plan.connect_args["ssl"] == "require"


def test_pgbouncer_flag_is_dropped_and_read() -> None:
    """Prisma-style URLs carry a flag that is meaningful to us, not to asyncpg."""
    plan = plan_connection("postgresql://u:p@somehost:5432/db?pgbouncer=true")

    assert "pgbouncer" not in plan.url
    assert "pgbouncer" not in plan.connect_args
    assert plan.mode == "transaction", "the flag is what identifies the pooler"
    # A self-hosted bouncer may speak no TLS, so the mode must not imply it.
    assert "ssl" not in plan.connect_args


def test_no_libpq_only_parameters_survive() -> None:
    """A belt-and-braces sweep of every parameter a dashboard might append."""
    messy = (
        SUPABASE_POOLER_TX + "?sslmode=require&pgbouncer=true"
        "&channel_binding=require&application_name=syncpad&connect_timeout=15"
    )
    plan = plan_connection(messy)

    for rejected in ("sslmode", "pgbouncer", "channel_binding", "application_name"):
        assert rejected not in plan.url
        assert rejected not in plan.connect_args


# --- classifying the connection ----------------------------------------


@pytest.mark.parametrize(
    "url,expected",
    [
        (SUPABASE_POOLER_TX, "transaction"),
        (SUPABASE_POOLER_SESSION, "session"),
        (SUPABASE_DIRECT, "direct"),
        # The docker-compose stack and any self-hosted server: we cannot know,
        # and must not pretend to.
        ("postgresql://syncpad:syncpad@db:5432/syncpad", "unknown"),
        ("sqlite+aiosqlite:///./syncpad.db", "unknown"),
    ],
)
def test_pooling_mode(url: str, expected: str) -> None:
    assert pooling_mode(url) == expected


def test_transaction_pooling_disables_prepared_statements() -> None:
    """Otherwise: intermittent DuplicatePreparedStatementError under load."""
    plan = plan_connection(SUPABASE_POOLER_TX)

    assert plan.connect_args["statement_cache_size"] == 0
    assert plan.connect_args["prepared_statement_cache_size"] == 0
    assert not plan.caches_statements
    # asyncpg numbers statements sequentially, which collides across clients
    # sharing a backend. Unique names are required, not merely advisable.
    name_func = plan.connect_args["prepared_statement_name_func"]
    assert name_func() != name_func()


def test_session_pooling_keeps_prepared_statements() -> None:
    """Session mode holds one backend, so the cache is both safe and valuable."""
    plan = plan_connection(SUPABASE_POOLER_SESSION)

    assert "statement_cache_size" not in plan.connect_args
    assert plan.caches_statements


@pytest.mark.parametrize(
    "url,supported",
    [
        (SUPABASE_DIRECT, True),
        (SUPABASE_POOLER_SESSION, True),
        (SUPABASE_POOLER_TX, False),
    ],
)
def test_listen_notify_support_tracks_pooling_mode(url: str, supported: bool) -> None:
    """LISTEN/NOTIFY needs a backend that stays put between transactions."""
    assert plan_connection(url).supports_listen_notify is supported


# --- the quieter translations ------------------------------------------


def test_application_name_becomes_a_server_setting() -> None:
    plan = plan_connection(SUPABASE_DIRECT + "?application_name=syncpad")
    assert plan.connect_args["server_settings"] == {"application_name": "syncpad"}


def test_connect_timeout_is_renamed_and_typed() -> None:
    plan = plan_connection(SUPABASE_DIRECT + "?connect_timeout=15")
    assert plan.connect_args["timeout"] == 15
    assert isinstance(plan.connect_args["timeout"], int)


def test_tls_is_required_by_default_on_managed_postgres() -> None:
    """asyncpg defaults to 'prefer', which downgrades to plaintext in silence."""
    assert plan_connection(SUPABASE_DIRECT).connect_args["ssl"] == "require"


def test_tls_is_not_forced_on_unrecognised_hosts() -> None:
    """The docker-compose Postgres speaks no TLS; forcing it would break it."""
    plan = plan_connection("postgresql://syncpad:syncpad@db:5432/syncpad")
    assert "ssl" not in plan.connect_args
    assert plan.connect_args == {}


def test_explicit_sslmode_wins_over_the_default() -> None:
    plan = plan_connection(SUPABASE_DIRECT + "?sslmode=verify-full")
    assert plan.connect_args["ssl"] == "verify-full"


def test_unrecognised_parameters_are_left_in_the_url() -> None:
    """Swallowing one would hide the error that names it."""
    plan = plan_connection(SUPABASE_DIRECT + "?some_new_option=7")
    assert "some_new_option=7" in plan.url


def test_sqlite_is_untouched_but_still_normalised() -> None:
    plan = plan_connection("sqlite:///./syncpad.db")
    assert plan.url == "sqlite+aiosqlite:///./syncpad.db"
    assert plan.connect_args == {}


def test_driver_is_upgraded_while_parameters_are_rewritten() -> None:
    plan = plan_connection(SUPABASE_POOLER_TX + "?sslmode=require")
    assert plan.url.startswith("postgresql+asyncpg://")
    assert "postgres.abcdefghijklmnop:s3cret" in plan.url, "credentials preserved"


def test_notes_explain_each_change() -> None:
    """A deploy that fails at 2am is not the moment to read source."""
    plan = plan_connection(SUPABASE_POOLER_TX + "?sslmode=require&pgbouncer=true")
    joined = " | ".join(plan.notes).lower()
    assert "sslmode" in joined
    assert "pgbouncer" in joined
    assert "transaction pooling" in joined


@pytest.mark.parametrize(
    "given,expected",
    [
        ("postgresql://u:p@host/db", "postgresql+asyncpg://u:p@host/db"),
        ("postgres://u:p@host/db", "postgresql+asyncpg://u:p@host/db"),
        ("mysql://u@h/db", "mysql://u@h/db"),
    ],
)
def test_normalise_url_is_unchanged_by_the_move(given: str, expected: str) -> None:
    assert normalise_url(given) == expected


# --- proof that the plan connects --------------------------------------


@pytest.mark.integration
async def test_a_translated_url_actually_connects() -> None:
    """Needs a live Postgres at SYNCPAD_TEST_POSTGRES_URL.

    The unit tests above prove ``sslmode`` is removed; only a real server proves
    what is left is still a working connection.
    """
    import os

    from sqlalchemy import text

    from syncpad.config import Settings
    from syncpad.storage.db import make_engine

    raw = os.environ.get("SYNCPAD_TEST_POSTGRES_URL")
    if not raw:
        pytest.skip("set SYNCPAD_TEST_POSTGRES_URL")

    # Append the parameter that would otherwise raise TypeError.
    joiner = "&" if "?" in raw else "?"
    engine = make_engine(Settings(database_url=f"{raw}{joiner}sslmode=disable"))
    try:
        async with engine.connect() as conn:
            assert (await conn.execute(text("select 1"))).scalar_one() == 1
    finally:
        await engine.dispose()

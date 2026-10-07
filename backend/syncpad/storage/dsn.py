"""Turning a copy-pasted connection string into one asyncpg will accept.

Every managed Postgres provider hands out a libpq URL, because that is what
``psql`` and every non-Python client take. SQLAlchemy's asyncpg dialect forwards
the query string *verbatim* into ``asyncpg.connect()``::

    opts.update(url.query)        # sqlalchemy/dialects/postgresql/asyncpg.py

asyncpg is not libpq and accepts none of libpq's parameter names, so the URL
Supabase shows you in its dashboard fails with a ``TypeError`` about an
unexpected keyword argument ``sslmode`` — an error that names neither TLS nor
the provider, and that no amount of staring at the password will fix.

This module is the translation layer. It is pure string handling with no
network and no engine, so the tests for it are exact.

Two things are worth knowing about pooled Postgres, because both are silent
until they are not:

**Prepared statements do not survive transaction-mode pooling.** In that mode a
server connection is returned to the pool at every ``COMMIT``, so the statement
your session prepared is likely to be prepared on a *different* backend next
time, and asyncpg's numeric statement names collide across clients. The failure
is intermittent ``DuplicatePreparedStatementError`` under load, which reads like
a concurrency bug in the application. Caching is therefore disabled whenever
transaction pooling is detected.

**A pooler is not a direct connection.** Session mode keeps one server backend
for the life of the client connection, so ``LISTEN``/``NOTIFY`` and prepared
statements work. Transaction mode does not. The distinction is a port number,
which is an unfortunate place to hide a semantic difference, so
:func:`pooling_mode` names it explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

__all__ = [
    "DsnPlan",
    "PoolingMode",
    "normalise_url",
    "plan_connection",
    "pooling_mode",
]

PoolingMode = Literal["direct", "session", "transaction", "unknown"]

#: libpq parameters that map onto an asyncpg keyword under a different name.
_RENAMED: dict[str, str] = {
    "sslmode": "ssl",
    "connect_timeout": "timeout",
}

#: libpq parameters that belong in asyncpg's ``server_settings`` dict rather
#: than its keyword arguments.
_SERVER_SETTINGS = frozenset({"application_name", "search_path", "timezone"})

#: Parameters that exist only to instruct *other* clients and must be dropped.
#: ``pgbouncer=true`` is Prisma's flag for "this is a transaction pooler"; it is
#: meaningless to asyncpg but meaningful to us, so it is read before it is
#: discarded.
_CLIENT_ONLY = frozenset(
    {
        "pgbouncer",
        "channel_binding",
        "gssencmode",
        "sslcompression",
        "sslnegotiation",
        "keepalives",
        "keepalives_idle",
        "keepalives_interval",
        "keepalives_count",
        "prefer_simple_protocol",
        "supa",  # Supabase appends this on some dashboard URLs
    }
)

#: asyncpg keywords that may legitimately appear in the query string and should
#: be passed through untouched.
_PASSTHROUGH = frozenset(
    {
        "ssl",
        "timeout",
        "command_timeout",
        "statement_cache_size",
        "max_cached_statement_lifetime",
        "max_cacheable_statement_size",
        "prepared_statement_cache_size",
        "target_session_attrs",
        "direct_tls",
        "krbsrvname",
        "gsslib",
        "passfile",
        "service",
    }
)

#: Supavisor multiplexes transaction-mode clients here. Session mode is 5432.
_SUPAVISOR_TRANSACTION_PORT = 6543

_INT_ARGS = frozenset(
    {
        "timeout",
        "statement_cache_size",
        "max_cached_statement_lifetime",
        "max_cacheable_statement_size",
        "prepared_statement_cache_size",
    }
)


@dataclass(frozen=True)
class DsnPlan:
    """Everything needed to build an engine, with the reasoning kept."""

    url: str
    connect_args: dict[str, object] = field(default_factory=dict)
    mode: PoolingMode = "unknown"
    #: Human-readable record of what was changed and why, for the startup log.
    #: Deploys fail at 2am; a line saying which parameter was rewritten is
    #: worth more than a correct-but-silent connection.
    notes: tuple[str, ...] = ()

    @property
    def caches_statements(self) -> bool:
        return self.connect_args.get("statement_cache_size", 100) != 0

    @property
    def supports_listen_notify(self) -> bool:
        """Transaction pooling breaks LISTEN/NOTIFY: the backend moves."""
        return self.mode in ("direct", "session")


def normalise_url(url: str) -> str:
    """Upgrade bare driver-less URLs to their async driver.

    Operators write ``postgresql://…`` out of habit — and every tutorial, cloud
    provider and ``DATABASE_URL`` convention does too. Silently rejecting that
    with "the asyncio extension requires an async driver" is a bad first five
    minutes, so translate it instead.
    """
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    if url.startswith("postgres://"):  # Heroku-style
        return url.replace("postgres://", "postgresql+asyncpg://", 1)
    if url.startswith("sqlite://") and "+aiosqlite" not in url:
        return url.replace("sqlite://", "sqlite+aiosqlite://", 1)
    return url


def _is_managed_host(host: str) -> bool:
    """True for hosts we know terminate TLS and require it.

    Deliberately narrow. Inferring "managed, therefore TLS" from a pooling mode
    would force TLS onto a self-hosted pgbouncer that speaks plaintext, turning
    a working deployment into a handshake error.
    """
    host = host.lower()
    return "pooler.supabase.com" in host or (
        host.startswith("db.") and host.endswith(".supabase.co")
    )


def pooling_mode(url: str) -> PoolingMode:
    """Classify a Postgres URL by what its connection can promise.

    Returns ``"unknown"`` rather than guessing for hosts we cannot recognise:
    claiming session semantics we cannot deliver would turn a clear startup
    failure into a subtle runtime one.
    """
    parts = urlsplit(normalise_url(url))
    if not parts.scheme.startswith("postgresql"):
        return "unknown"

    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    if query.get("pgbouncer", "").lower() in ("1", "true", "yes"):
        return "transaction"

    host = (parts.hostname or "").lower()
    port = parts.port

    if "pooler.supabase.com" in host:
        return "transaction" if port == _SUPAVISOR_TRANSACTION_PORT else "session"
    if host.startswith("db.") and host.endswith(".supabase.co"):
        return "direct"
    if port == _SUPAVISOR_TRANSACTION_PORT:
        return "transaction"
    return "unknown"


def _coerce(name: str, value: str) -> object:
    if name in _INT_ARGS:
        try:
            return int(value)
        except ValueError:
            return value
    if name == "direct_tls":
        return value.lower() in ("1", "true", "yes")
    return value


def plan_connection(url: str, *, force_mode: PoolingMode | None = None) -> DsnPlan:
    """Rewrite a libpq URL into an asyncpg-safe URL plus connect kwargs.

    ``force_mode`` exists for operators behind a pooler we cannot identify from
    the hostname, and for the tests.
    """
    normalised = normalise_url(url)
    if not normalised.startswith("postgresql"):
        # SQLite and friends need none of this.
        return DsnPlan(url=normalised, mode="unknown")

    parts = urlsplit(normalised)
    pairs = parse_qsl(parts.query, keep_blank_values=True)

    connect_args: dict[str, object] = {}
    server_settings: dict[str, str] = {}
    kept: list[tuple[str, str]] = []
    notes: list[str] = []

    for raw_name, value in pairs:
        name = raw_name.lower()

        if name in _CLIENT_ONLY:
            notes.append(f"dropped {raw_name}= (not an asyncpg parameter)")
            continue

        if name in _SERVER_SETTINGS:
            server_settings[name] = value
            notes.append(f"moved {raw_name}= into server_settings")
            continue

        if name in _RENAMED:
            target = _RENAMED[name]
            connect_args[target] = _coerce(target, value)
            notes.append(f"translated {raw_name}={value} to asyncpg {target}=")
            continue

        if name in _PASSTHROUGH:
            connect_args[name] = _coerce(name, value)
            continue

        # Unrecognised: keep it in the URL. If asyncpg rejects it the error
        # names the parameter, which is more useful than us swallowing it.
        kept.append((raw_name, value))

    if server_settings:
        connect_args["server_settings"] = server_settings

    mode = force_mode or pooling_mode(normalised)

    if mode == "transaction":
        # Order matters only for readability; all three are required together.
        connect_args["statement_cache_size"] = 0
        connect_args["prepared_statement_cache_size"] = 0
        connect_args.setdefault(
            "prepared_statement_name_func", lambda: f"__syncpad_{uuid4().hex}__"
        )
        notes.append(
            "transaction pooling detected: prepared-statement caching disabled "
            "and statement names randomised"
        )

    if _is_managed_host(parts.hostname or "") and "ssl" not in connect_args:
        # Managed Postgres requires TLS, and asyncpg defaults to 'prefer', which
        # silently falls back to plaintext if the server permits it. A local or
        # self-hosted pooler is left alone: it may have no TLS at all.
        connect_args["ssl"] = "require"
        notes.append("managed host with no sslmode given; defaulting to ssl=require")

    rebuilt = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(kept), parts.fragment))
    return DsnPlan(url=rebuilt, connect_args=connect_args, mode=mode, notes=tuple(notes))

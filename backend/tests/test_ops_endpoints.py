"""Metrics, liveness and readiness — and the difference between the last two.

Liveness must never depend on an external service. If ``/healthz`` checked the
database, a brief Postgres blip would fail every replica's liveness probe and
the orchestrator would kill the entire fleet, converting a recoverable outage
into a total one. Readiness is where dependency checks belong, because failing
it removes a replica from rotation without restarting it.
"""

from __future__ import annotations

from syncpad.api.metrics import Metrics


async def test_healthz_is_dependency_free(client) -> None:
    response = await client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_healthz_stays_up_when_the_database_is_gone(app, client) -> None:
    """The property that stops a database blip from killing the whole fleet."""
    await app.state.engine.dispose()

    class Broken:
        def __call__(self):
            raise RuntimeError("database is unreachable")

    app.state.session_factory = Broken()

    response = await client.get("/healthz")
    assert response.status_code == 200, "liveness must not depend on the database"


async def test_readyz_reports_each_dependency(client) -> None:
    response = await client.get("/readyz")
    assert response.status_code == 200
    body = response.json()
    assert body["ready"] is True
    assert body["checks"]["database"] == "ok"
    assert body["checks"]["hub"] == "in-process"
    assert "took_ms" in body
    assert body["rooms"] == 0


async def test_readyz_fails_when_the_database_is_unreachable(app, client) -> None:
    class Broken:
        def __call__(self):
            raise RuntimeError("database is unreachable")

    app.state.session_factory = Broken()

    response = await client.get("/readyz")
    assert response.status_code == 503
    assert response.json()["ready"] is False
    assert response.json()["checks"]["database"] == "unavailable"


async def test_readyz_fails_when_redis_is_unreachable(app, client) -> None:
    class DeadRedis:
        async def ping(self):
            raise ConnectionError("redis is unreachable")

    class RedisBackedHub:
        gateway_id = "gw"
        _redis = DeadRedis()

    app.state.hub = RedisBackedHub()

    response = await client.get("/readyz")
    assert response.status_code == 503
    assert response.json()["checks"]["hub"] == "unavailable"


async def test_metrics_exposes_the_documented_series(client) -> None:
    response = await client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]

    body = response.text
    for series in (
        "syncpad_ws_connections",
        "syncpad_rooms_active",
        "syncpad_ops_applied_total",
        "syncpad_rate_limited_total",
        "syncpad_snapshot_writes_total",
        "syncpad_broadcast_latency_seconds",
        "syncpad_cold_load_seconds",
    ):
        assert series in body, f"{series} is missing from /metrics"


async def test_broadcast_latency_buckets_resolve_around_the_target() -> None:
    """The design is held to sub-60ms, so the histogram needs buckets there."""
    metrics = Metrics()
    buckets = metrics.broadcast_latency._upper_bounds
    assert 0.060 in buckets, "no bucket at the 60ms target"
    assert sum(1 for b in buckets if b <= 0.060) >= 5, "too coarse below the target"


def test_metrics_counters_move() -> None:
    metrics = Metrics()

    metrics.connection_opened()
    metrics.connection_opened()
    metrics.connection_closed()
    assert metrics.ws_connections._value.get() == 1

    metrics.ops_applied(5)
    metrics.ops_applied(3)
    assert metrics.ops_applied_total._value.get() == 8

    metrics.rate_limited()
    assert metrics.rate_limited_total._value.get() == 1

    metrics.snapshot_written()
    assert metrics.snapshot_writes_total._value.get() == 1

    metrics.observe_broadcast(0.012)
    metrics.observe_cold_load(0.4)


def test_each_app_gets_its_own_registry() -> None:
    """A shared registry would raise 'duplicated timeseries' on the second app."""
    first, second = Metrics(), Metrics()
    assert first.registry is not second.registry


async def test_metrics_reports_live_room_count(client, auth_client, ws_client) -> None:
    response = await client.get("/metrics")
    assert "syncpad_rooms_active 0.0" in response.text

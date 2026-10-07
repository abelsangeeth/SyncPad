"""Prometheus metrics, liveness and readiness.

The distinction between the two probes matters in a Kubernetes or ECS
deployment and is routinely got wrong. ``/healthz`` answers "is this process
alive" and must not touch a dependency: if it checked Postgres, a database blip
would make every replica fail its liveness probe and get killed, turning a brief
outage into a total one. ``/readyz`` answers "should traffic be routed here" and
*does* check dependencies, because a replica that cannot reach Redis should be
taken out of rotation without being restarted.
"""

from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Request, Response, status
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from sqlalchemy import text

__all__ = ["Metrics", "router"]

log = logging.getLogger("syncpad.metrics")

router = APIRouter(tags=["ops"])


class Metrics:
    """Owns a private registry so several apps can coexist in one test process.

    The module-level default registry would raise "duplicated timeseries" the
    second time an app is constructed, which is every test after the first.
    """

    def __init__(self) -> None:
        self.registry = CollectorRegistry()

        self.ws_connections = Gauge(
            "syncpad_ws_connections",
            "Currently open WebSocket editing sessions on this replica.",
            registry=self.registry,
        )
        self.rooms_active = Gauge(
            "syncpad_rooms_active",
            "Documents currently held in memory on this replica.",
            registry=self.registry,
        )
        self.ops_applied_total = Counter(
            "syncpad_ops_applied_total",
            "CRDT operations applied and persisted.",
            registry=self.registry,
        )
        self.rate_limited_total = Counter(
            "syncpad_rate_limited_total",
            "Operation frames refused by the per-connection token bucket.",
            registry=self.registry,
        )
        self.snapshot_writes_total = Counter(
            "syncpad_snapshot_writes_total",
            "Snapshots written.",
            registry=self.registry,
        )
        self.broadcast_latency = Histogram(
            "syncpad_broadcast_latency_seconds",
            "Time from receiving an operation frame to completing fan-out.",
            registry=self.registry,
            # Bucketed tightly below 60ms: that is the number the design is
            # held to, so the histogram needs resolution exactly there.
            buckets=(0.005, 0.010, 0.020, 0.030, 0.045, 0.060, 0.100, 0.250, 1.0),
        )
        self.cold_load = Histogram(
            "syncpad_cold_load_seconds",
            "Time to bring a document into memory from snapshot plus replay.",
            registry=self.registry,
            buckets=(0.005, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 5.0),
        )

    # Thin wrappers so call sites read as intent rather than as plumbing.
    def connection_opened(self) -> None:
        self.ws_connections.inc()

    def connection_closed(self) -> None:
        self.ws_connections.dec()

    def ops_applied(self, count: int) -> None:
        self.ops_applied_total.inc(count)

    def rate_limited(self) -> None:
        self.rate_limited_total.inc()

    def snapshot_written(self) -> None:
        self.snapshot_writes_total.inc()

    def observe_broadcast(self, seconds: float) -> None:
        self.broadcast_latency.observe(seconds)

    def observe_cold_load(self, seconds: float) -> None:
        self.cold_load.observe(seconds)


@router.get("/metrics", include_in_schema=False)
async def metrics(request: Request) -> Response:
    state = request.app.state
    state.metrics.rooms_active.set(state.rooms.active)
    return Response(generate_latest(state.metrics.registry), media_type=CONTENT_TYPE_LATEST)


@router.get("/healthz", include_in_schema=False)
async def healthz() -> dict[str, str]:
    """Liveness. Deliberately checks nothing external — see the module docstring."""
    return {"status": "ok"}


@router.get("/readyz", include_in_schema=False)
async def readyz(request: Request, response: Response) -> dict[str, object]:
    """Readiness. Reports each dependency, and 503s if any is down."""
    state = request.app.state
    checks: dict[str, str] = {}
    started = time.perf_counter()

    try:
        async with state.session_factory() as session:
            await session.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as exc:
        log.warning("readiness: database unreachable: %s", exc)
        checks["database"] = "unavailable"

    hub = state.hub
    if hasattr(hub, "_redis") and getattr(hub, "_redis", None) is not None:
        try:
            await hub._redis.ping()
            checks["hub"] = "ok"
        except Exception as exc:
            log.warning("readiness: redis unreachable: %s", exc)
            checks["hub"] = "unavailable"
    else:
        checks["hub"] = "in-process"

    ready = all(v != "unavailable" for v in checks.values())
    if not ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {
        "ready": ready,
        "checks": checks,
        "rooms": state.rooms.active,
        "took_ms": round((time.perf_counter() - started) * 1000, 2),
    }

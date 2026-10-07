"""Does adding replicas actually help? Same load, one replica versus several.

This is the benchmark that tests the project's central architectural claim. The
gateway is stateless: no document is owned by a replica, no session is pinned to
one, and fan-out goes through Redis Pub/Sub rather than through process memory.
If that is true, spreading the same editors across more replicas should reduce
latency — because the per-operation fan-out work, which is what saturates a
single process, is divided between them.

If it is false — if there were hidden affinity, or if the hub were a bottleneck
instead of a relief — adding replicas would leave latency flat or make it worse.
So the run reports both configurations side by side and states the speedup.

Requires Postgres and Redis:

    docker run -d --rm --name pg -e POSTGRES_USER=syncpad -e POSTGRES_PASSWORD=syncpad \\
        -e POSTGRES_DB=syncpad -p 5442:5432 postgres:16-alpine
    docker run -d --rm --name redis -p 6399:6379 redis:7-alpine
    python bench/scaleout.py --clients 50 --duration 15
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

import httpx  # noqa: E402

from bench.loadtest import _percentiles, _server_histogram, _worker  # noqa: E402


async def _start_replica(port: int, database_url: str, redis_url: str):
    """Launch one gateway. Every replica runs an identical command."""
    env = {
        **os.environ,
        "SYNCPAD_ENV": "dev",
        "SYNCPAD_JWT_SECRET": "scaleout-secret",
        "SYNCPAD_DATABASE_URL": database_url,
        "SYNCPAD_REDIS_URL": redis_url,
        "SYNCPAD_RATE_OPS_PER_S": "1000000",
        "SYNCPAD_RATE_BURST": "1000000",
    }
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "uvicorn",
        "syncpad.main:app",
        "--factory",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--log-level",
        "warning",
        cwd=str(ROOT / "backend"),
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )


async def _await_health(port: int, timeout: float = 60.0) -> None:
    deadline = time.time() + timeout
    async with httpx.AsyncClient(timeout=1.0) as probe:
        while time.time() < deadline:
            with contextlib.suppress(Exception):
                if (await probe.get(f"http://127.0.0.1:{port}/healthz")).status_code == 200:
                    return
            await asyncio.sleep(0.5)
    raise RuntimeError(f"replica on port {port} never became healthy")


async def _prepare(url: str) -> tuple[str, str]:
    async with httpx.AsyncClient(base_url=url, timeout=30) as client:
        stamp = int(time.time() * 1000)
        registered = await client.post(
            "/api/auth/register",
            json={
                "email": f"scale{stamp}@example.com",
                "name": "Scale",
                "password": "bench-password-1",
            },
        )
        registered.raise_for_status()
        token = registered.json()["access_token"]
        created = await client.post(
            "/api/documents",
            json={"title": f"Scale-out {stamp}"},
            headers={"Authorization": f"Bearer {token}"},
        )
        created.raise_for_status()
        return created.json()["id"], token


async def _measure(ports: list[int], args, label: str) -> dict:
    """Run the load against ``ports``, spreading clients round-robin.

    Round-robin, with no stickiness whatsoever: each client picks a replica and
    they all edit the same document. That is the configuration a plain
    least-connections load balancer produces, and the one `deploy/nginx.conf`
    deliberately sets up.
    """
    base = f"http://127.0.0.1:{ports[0]}"
    doc_id, token = await _prepare(base)

    workers = max(1, min(args.workers, args.clients))
    per_worker = args.clients // workers
    leftover = args.clients % workers
    writers_total = max(1, int(args.clients * args.writer_fraction))

    jobs = []
    assigned = 0
    for index in range(workers):
        count = per_worker + (1 if index < leftover else 0)
        share = min(count, max(0, writers_total - assigned))
        assigned += share
        jobs.append(
            {
                "url": f"http://127.0.0.1:{ports[index % len(ports)]}",
                "doc_id": doc_id,
                "token": token,
                "clients": count,
                "writers": share,
                "duration": args.duration,
                "think_time": args.think_time,
                "warmup": args.warmup,
            }
        )

    print(f"  {label}: {args.clients} clients over {len(ports)} replica(s)…")
    started = time.perf_counter()
    with mp.get_context("spawn").Pool(processes=workers) as pool:
        outcomes = pool.map(_worker, jobs)
    elapsed = time.perf_counter() - started - args.warmup

    samples: list[float] = []
    sent = received = errors = 0
    for outcome in outcomes:
        samples.extend(outcome["samples"])
        sent += outcome["sent"]
        received += outcome["received"]
        errors += outcome["errors"]

    histograms = []
    for port in ports:
        async with httpx.AsyncClient(timeout=10) as probe:
            histograms.append(
                _server_histogram((await probe.get(f"http://127.0.0.1:{port}/metrics")).text)
            )

    return {
        "label": label,
        "replicas": len(ports),
        "clients": args.clients,
        "ops_sent": sent,
        "frames_received": received,
        "latency_samples": len(samples),
        "errors": errors,
        "throughput_ops_per_s": round(sent / elapsed, 1) if elapsed > 0 else 0,
        "client_observed_latency_ms": _percentiles(samples) if samples else None,
        "per_replica_fanout": histograms,
    }


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clients", type=int, default=50)
    parser.add_argument("--duration", type=float, default=15.0)
    parser.add_argument("--warmup", type=float, default=1.5)
    parser.add_argument("--think-time", type=float, default=0.25)
    parser.add_argument("--writer-fraction", type=float, default=0.5)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--replicas", type=int, default=3)
    parser.add_argument("--base-port", type=int, default=8830)
    parser.add_argument(
        "--database-url",
        default="postgresql+asyncpg://syncpad:syncpad@127.0.0.1:5442/syncpad",
    )
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6399/0")
    parser.add_argument("--assert-speedup", type=float, default=None)
    args = parser.parse_args()

    ports = [args.base_port + i for i in range(args.replicas)]
    processes = []
    try:
        for port in ports:
            processes.append(await _start_replica(port, args.database_url, args.redis_url))
        for port in ports:
            await _await_health(port)
        print(f"  {len(ports)} replicas healthy on ports {ports}")

        single = await _measure(ports[:1], args, "one replica")
        await asyncio.sleep(2)
        spread = await _measure(ports, args, f"{len(ports)} replicas")

        one_p95 = single["client_observed_latency_ms"]["p95"]
        many_p95 = spread["client_observed_latency_ms"]["p95"]
        speedup = round(one_p95 / many_p95, 2) if many_p95 else 0

        report = {
            "single": single,
            "spread": spread,
            "p95_speedup": speedup,
            "verdict": (
                "adding replicas reduced latency"
                if speedup > 1.1
                else "no meaningful improvement from adding replicas"
            ),
        }
        print(json.dumps(report, indent=2))

        out = ROOT / "bench" / "results"
        out.mkdir(parents=True, exist_ok=True)
        (out / "scaleout.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

        print(
            f"\n  one replica : p50 {single['client_observed_latency_ms']['p50']}ms  "
            f"p95 {one_p95}ms"
        )
        print(
            f"  {len(ports)} replicas: p50 {spread['client_observed_latency_ms']['p50']}ms  "
            f"p95 {many_p95}ms"
        )
        print(f"  p95 speedup from horizontal scaling: {speedup}x")

        if args.assert_speedup is not None and speedup < args.assert_speedup:
            print(
                f"FAIL: speedup {speedup}x below the required {args.assert_speedup}x",
                file=sys.stderr,
            )
            return 1
        return 0
    finally:
        for process in processes:
            with contextlib.suppress(ProcessLookupError):
                process.terminate()
            with contextlib.suppress(asyncio.TimeoutError, ProcessLookupError):
                await asyncio.wait_for(process.wait(), timeout=10)


if __name__ == "__main__":
    mp.freeze_support()
    raise SystemExit(asyncio.run(main()))

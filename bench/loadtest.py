"""Broadcast latency under concurrent load.

The claim being measured: with N editors in one document, the time from an
operation leaving one client to arriving at another stays under a target
threshold. That is a number the design is held to, so it is measured rather than
asserted, and the run fails when it regresses.

What is timed is the full round — client A sends a frame, the gateway applies
it, appends it to the durable log, publishes it to the hub, and client B
receives it. Persistence is inside the measurement on purpose: excluding it
would measure a system nobody runs.

**Clients are sharded across processes, and that detail is the difference
between a benchmark and a fiction.** A single Python process cannot host fifty
editors honestly: each operation fans out to every other participant, so fifty
clients receiving one hundred operations per second must parse roughly five
thousand frames per second through one event loop. The loop saturates, frames
queue in the client, and the harness reports its own backlog as server latency —
inflating p95 by two orders of magnitude while the server sits idle. Sharding
across processes removes the artifact.

As a cross-check the run also reports the server's own histogram, which is
measured inside the gateway and cannot be distorted by client-side queueing. If
the two disagree sharply, the harness is the thing under load, not the server.

    python bench/loadtest.py --clients 50 --duration 20 --assert-p95-ms 60
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import multiprocessing as mp
import os
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

import httpx  # noqa: E402
import websockets  # noqa: E402

from syncpad.crdt import RGA, ops_to_dicts  # noqa: E402

PROBE_PREFIX = "<t"
PROBE_SUFFIX = ">"


# --------------------------------------------------------------------- worker


async def _editor(url, doc_id, token, stop, samples, counters, think_time, writer):
    """One simulated participant: a real socket holding a real replica."""
    ws_url = url.replace("http://", "ws://").replace("https://", "wss://") + "/ws"
    async with websockets.connect(ws_url, max_size=8 * 1024 * 1024) as socket:
        await socket.send(json.dumps({"t": "hello", "doc": doc_id, "token": token, "since": 0}))
        welcome = json.loads(await socket.recv())
        if welcome.get("t") != "welcome":
            counters["errors"] += 1
            return
        replica = RGA(welcome["site"])

        async def receive() -> None:
            while not stop.is_set():
                try:
                    frame = json.loads(await asyncio.wait_for(socket.recv(), timeout=0.4))
                except TimeoutError:
                    continue
                except websockets.ConnectionClosed:
                    return
                counters["received"] += 1
                arrived = time.time()
                # The gateway coalesces frames under load; unwrap before timing,
                # or the batched operations would never be measured at all.
                inner = frame["frames"] if frame.get("t") == "batch" else [frame]
                for one in inner:
                    if one.get("t") != "ops":
                        continue
                    for op in one["ops"]:
                        text = op.get("tx", "")
                        if text.startswith(PROBE_PREFIX) and text.endswith(PROBE_SUFFIX):
                            with contextlib.suppress(ValueError):
                                samples.append((arrived - float(text[2:-1])) * 1000)

        reader = asyncio.create_task(receive())
        try:
            if writer:
                cseq = 0
                while not stop.is_set():
                    cseq += 1
                    # Wall clock, not perf_counter: the sender and the receiver
                    # may be in different processes.
                    probe = f"{PROBE_PREFIX}{time.time():.6f}{PROBE_SUFFIX}"
                    ops = replica.local_insert(len(replica), probe)
                    await socket.send(
                        json.dumps({"t": "ops", "ops": ops_to_dicts(ops), "cseq": cseq})
                    )
                    counters["sent"] += len(ops)
                    await asyncio.sleep(think_time)
            else:
                await stop.wait()
        finally:
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reader


async def _worker_async(job: dict) -> dict:
    samples: list[float] = []
    counters = {"sent": 0, "received": 0, "errors": 0}
    stop = asyncio.Event()

    async def guarded(writer: bool) -> None:
        try:
            await _editor(
                job["url"],
                job["doc_id"],
                job["token"],
                stop,
                samples,
                counters,
                job["think_time"],
                writer,
            )
        except Exception:
            counters["errors"] += 1

    tasks = [asyncio.create_task(guarded(i < job["writers"])) for i in range(job["clients"])]
    await asyncio.sleep(job["warmup"])
    await asyncio.sleep(job["duration"])
    stop.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    return {"samples": samples, **counters}


def _worker(job: dict) -> dict:
    """Process entry point. Must be importable at module level for spawn()."""
    return asyncio.run(_worker_async(job))


# --------------------------------------------------------------- orchestration


def _percentiles(values: list[float]) -> dict:
    ordered = sorted(values)

    def pct(p: float) -> float:
        return round(ordered[min(int(len(ordered) * p), len(ordered) - 1)], 2)

    return {
        "min": round(ordered[0], 2),
        "p50": pct(0.50),
        "p90": pct(0.90),
        "p95": pct(0.95),
        "p99": pct(0.99),
        "max": round(ordered[-1], 2),
        "mean": round(statistics.fmean(ordered), 2),
    }


def _server_histogram(metrics: str) -> dict:
    """Server-observed fan-out latency, straight from the Prometheus histogram."""
    buckets: list[tuple[float, float]] = []
    total = 0.0
    seconds = 0.0
    for line in metrics.splitlines():
        if line.startswith("syncpad_broadcast_latency_seconds_bucket"):
            bound = line.split('le="', 1)[1].split('"', 1)[0]
            count = float(line.rsplit(" ", 1)[1])
            buckets.append((float("inf") if bound == "+Inf" else float(bound), count))
        elif line.startswith("syncpad_broadcast_latency_seconds_count"):
            total = float(line.rsplit(" ", 1)[1])
        elif line.startswith("syncpad_broadcast_latency_seconds_sum"):
            seconds = float(line.rsplit(" ", 1)[1])

    if not total:
        return {"observations": 0}

    buckets.sort()
    result = {"observations": int(total), "mean_ms": round(seconds / total * 1000, 2)}
    for label, target in (("p50", 0.50), ("p95", 0.95), ("p99", 0.99)):
        want = total * target
        for bound, count in buckets:
            if count >= want:
                result[f"{label}_under_ms"] = None if bound == float("inf") else bound * 1000
                break
    return result


async def _prepare(url: str) -> tuple[str, str]:
    async with httpx.AsyncClient(base_url=url, timeout=30) as client:
        stamp = int(time.time() * 1000)
        registered = await client.post(
            "/api/auth/register",
            json={
                "email": f"bench{stamp}@example.com",
                "name": "Bench",
                "password": "bench-password-1",
            },
        )
        registered.raise_for_status()
        token = registered.json()["access_token"]

        created = await client.post(
            "/api/documents",
            json={"title": f"Load test {stamp}"},
            headers={"Authorization": f"Bearer {token}"},
        )
        created.raise_for_status()
        return created.json()["id"], token


async def _scrape(url: str) -> str:
    async with httpx.AsyncClient(base_url=url, timeout=10) as client:
        response = await client.get("/metrics")
        return response.text


@contextlib.asynccontextmanager
async def _maybe_server(args: argparse.Namespace):
    if args.url:
        yield
        return

    args.url = f"http://127.0.0.1:{args.port}"
    results_dir = ROOT / "bench" / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    db = results_dir / "bench.db"
    for suffix in ("", "-wal", "-shm"):
        Path(str(db) + suffix).unlink(missing_ok=True)

    env = {
        **os.environ,
        "SYNCPAD_ENV": "dev",
        "SYNCPAD_JWT_SECRET": "bench-secret",
        "SYNCPAD_DATABASE_URL": f"sqlite+aiosqlite:///{db.as_posix()}",
        # The limiter is not what is being measured here.
        "SYNCPAD_RATE_OPS_PER_S": "1000000",
        "SYNCPAD_RATE_BURST": "1000000",
    }
    if args.redis_url:
        env["SYNCPAD_REDIS_URL"] = args.redis_url

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "uvicorn",
        "syncpad.main:app",
        "--factory",
        "--host",
        "127.0.0.1",
        "--port",
        str(args.port),
        "--log-level",
        "warning",
        cwd=str(ROOT / "backend"),
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        async with httpx.AsyncClient(timeout=1.0) as probe:
            for _ in range(60):
                with contextlib.suppress(Exception):
                    if (await probe.get(f"{args.url}/healthz")).status_code == 200:
                        break
                await asyncio.sleep(0.5)
            else:
                raise RuntimeError("server never became healthy")
        yield
    finally:
        process.terminate()
        with contextlib.suppress(asyncio.TimeoutError, ProcessLookupError):
            await asyncio.wait_for(process.wait(), timeout=10)


async def run(args: argparse.Namespace) -> int:
    doc_id, token = await _prepare(args.url)

    workers = max(1, min(args.workers, args.clients))
    per_worker = args.clients // workers
    leftover = args.clients % workers
    writers_total = max(1, int(args.clients * args.writer_fraction))

    jobs = []
    assigned_writers = 0
    for index in range(workers):
        count = per_worker + (1 if index < leftover else 0)
        share = min(count, max(0, writers_total - assigned_writers))
        assigned_writers += share
        jobs.append(
            {
                "url": args.url,
                "doc_id": doc_id,
                "token": token,
                "clients": count,
                "writers": share,
                "duration": args.duration,
                "think_time": args.think_time,
                "warmup": args.warmup,
            }
        )

    print(
        f"  {args.clients} clients ({writers_total} typing) across {workers} processes, "
        f"document {doc_id[:8]}, {args.duration:.0f}s"
    )

    started = time.perf_counter()
    context = mp.get_context("spawn")
    with context.Pool(processes=workers) as pool:
        outcomes = pool.map(_worker, jobs)
    elapsed = time.perf_counter() - started - args.warmup

    samples: list[float] = []
    sent = received = errors = 0
    for outcome in outcomes:
        samples.extend(outcome["samples"])
        sent += outcome["sent"]
        received += outcome["received"]
        errors += outcome["errors"]

    summary = {
        "clients": args.clients,
        "worker_processes": workers,
        "duration_s": round(elapsed, 2),
        "ops_sent": sent,
        "frames_received": received,
        "latency_samples": len(samples),
        "errors": errors,
        "throughput_ops_per_s": round(sent / elapsed, 1) if elapsed > 0 else 0,
        "client_observed_latency_ms": _percentiles(samples) if samples else None,
        "server_observed_fanout": _server_histogram(await _scrape(args.url)),
    }
    print(json.dumps(summary, indent=2))

    out_dir = ROOT / "bench" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "loadtest.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if not samples:
        print("FAIL: no latency samples collected", file=sys.stderr)
        return 1
    if errors:
        print(f"WARNING: {errors} client(s) errored", file=sys.stderr)

    if args.assert_p95_ms is not None:
        p95 = summary["client_observed_latency_ms"]["p95"]
        if p95 > args.assert_p95_ms:
            print(f"FAIL: p95 {p95}ms exceeds the {args.assert_p95_ms}ms budget", file=sys.stderr)
            return 1
        print(
            f"OK: {args.clients} concurrent editors, p95 {p95}ms "
            f"within the {args.assert_p95_ms}ms budget"
        )
    return 0


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clients", type=int, default=50)
    parser.add_argument("--duration", type=float, default=20.0)
    parser.add_argument("--warmup", type=float, default=1.5)
    parser.add_argument("--think-time", type=float, default=0.25)
    parser.add_argument("--writer-fraction", type=float, default=0.5)
    parser.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 4) - 1))
    parser.add_argument("--url", default=None)
    parser.add_argument("--redis-url", default=None)
    parser.add_argument("--port", type=int, default=8791)
    parser.add_argument("--assert-p95-ms", type=float, default=None)
    args = parser.parse_args()

    async with _maybe_server(args):
        return await run(args)


if __name__ == "__main__":
    mp.freeze_support()
    raise SystemExit(asyncio.run(main()))

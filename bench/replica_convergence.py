"""Cross-replica convergence over real Redis and real Postgres.

The unit tests prove the CRDT converges. The scale-out benchmark proves replicas
share load. This proves the two facts hold *together*: clients connected to
different gateway processes, editing the same document simultaneously with no
stickiness of any kind, all end up with identical text — and so does the
database.

This is the end-to-end statement of the architecture. Every earlier test stubs
something out; this one stubs out nothing.

    docker run -d --rm --name pg -e POSTGRES_USER=syncpad -e POSTGRES_PASSWORD=syncpad \\
        -e POSTGRES_DB=syncpad -p 5442:5432 postgres:16-alpine
    docker run -d --rm --name redis -p 6399:6379 redis:7-alpine
    python -m bench.replica_convergence --replicas 3 --clients 9 --ops 40
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import random
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

import httpx  # noqa: E402
import websockets  # noqa: E402

from syncpad.crdt import RGA, ops_from_dicts, ops_to_dicts  # noqa: E402

ALPHABET = "abcdefghijklmnopqrstuvwxyz "


class Participant:
    """One editor, holding a real replica, on a specific gateway process."""

    def __init__(self, name: str, port: int) -> None:
        self.name = name
        self.port = port
        self.socket = None
        self.rga: RGA | None = None
        self.seq = 0
        self.cseq = 0
        self._reader: asyncio.Task | None = None

    async def open(self, doc_id: str, token: str) -> None:
        self.socket = await websockets.connect(
            f"ws://127.0.0.1:{self.port}/ws", max_size=8 * 1024 * 1024
        )
        await self.socket.send(
            json.dumps({"t": "hello", "doc": doc_id, "token": token, "since": 0})
        )
        welcome = json.loads(await self.socket.recv())
        assert welcome["t"] == "welcome", welcome
        self.rga = RGA(welcome["site"])
        self.seq = welcome["seq"]
        self._reader = asyncio.create_task(self._read())

    async def _read(self) -> None:
        assert self.socket is not None
        try:
            async for raw in self.socket:
                frame = json.loads(raw)
                inner = frame["frames"] if frame.get("t") == "batch" else [frame]
                for one in inner:
                    if one.get("t") == "ops":
                        self.rga.apply_many(ops_from_dicts(one["ops"]))
                        self.seq = max(self.seq, one["seq"])
                    elif one.get("t") == "ack":
                        self.seq = max(self.seq, one["seq"])
        except (websockets.ConnectionClosed, asyncio.CancelledError):
            return

    async def edit(self, rnd: random.Random) -> None:
        """One random edit, applied locally and submitted."""
        assert self.rga is not None and self.socket is not None
        visible = len(self.rga)
        roll = rnd.random()

        if roll < 0.65 or visible == 0:
            text = "".join(rnd.choice(ALPHABET) for _ in range(rnd.randint(1, 6)))
            ops = self.rga.local_insert(rnd.randint(0, visible), text)
        elif roll < 0.85:
            start = rnd.randrange(visible)
            ops = self.rga.local_delete(start, rnd.randint(1, min(3, visible - start)))
        else:
            start = rnd.randrange(visible)
            ops = self.rga.local_mark(
                start,
                rnd.randint(1, min(5, visible - start)),
                rnd.choice(("bold", "italic", "code")),
                rnd.choice((True, None)),
            )
        if not ops:
            return
        self.cseq += 1
        await self.socket.send(
            json.dumps({"t": "ops", "ops": ops_to_dicts(ops), "cseq": self.cseq})
        )

    async def close(self) -> None:
        if self._reader:
            self._reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader
        if self.socket:
            await self.socket.close()


def spans_of(replica: RGA) -> list:
    return [[s.text, dict(sorted(s.marks.items()))] for s in replica.spans()]


async def start_replica(port: int, database_url: str, redis_url: str):
    env = {
        **os.environ,
        "SYNCPAD_ENV": "dev",
        "SYNCPAD_JWT_SECRET": "convergence-secret",
        "SYNCPAD_DATABASE_URL": database_url,
        "SYNCPAD_REDIS_URL": redis_url,
        "SYNCPAD_RATE_OPS_PER_S": "1000000",
        "SYNCPAD_RATE_BURST": "1000000",
        # Snapshot aggressively: a snapshot taken mid-session is exactly the
        # kind of thing that could lose an operation, so provoke it.
        "SYNCPAD_SNAPSHOT_EVERY_OPS": "25",
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


async def await_health(port: int, timeout: float = 60.0) -> None:
    deadline = time.time() + timeout
    async with httpx.AsyncClient(timeout=1.0) as probe:
        while time.time() < deadline:
            with contextlib.suppress(Exception):
                if (await probe.get(f"http://127.0.0.1:{port}/healthz")).status_code == 200:
                    return
            await asyncio.sleep(0.5)
    raise RuntimeError(f"replica on {port} never became healthy")


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replicas", type=int, default=3)
    parser.add_argument("--clients", type=int, default=9)
    parser.add_argument("--ops", type=int, default=40, help="edits per client")
    parser.add_argument("--seed", type=int, default=20260920)
    parser.add_argument("--base-port", type=int, default=8860)
    parser.add_argument(
        "--database-url",
        default="postgresql+asyncpg://syncpad:syncpad@127.0.0.1:5442/syncpad",
    )
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6399/0")
    parser.add_argument(
        "--settle-timeout",
        type=float,
        default=60.0,
        help="how long to wait for the backlog to drain",
    )
    args = parser.parse_args()

    ports = [args.base_port + i for i in range(args.replicas)]
    processes = []

    try:
        for port in ports:
            processes.append(await start_replica(port, args.database_url, args.redis_url))
        for port in ports:
            await await_health(port)
        print(f"  {args.replicas} replicas healthy on {ports}")

        base = f"http://127.0.0.1:{ports[0]}"
        async with httpx.AsyncClient(base_url=base, timeout=30) as client:
            stamp = int(time.time() * 1000)
            account = await client.post(
                "/api/auth/register",
                json={
                    "email": f"conv{stamp}@example.com",
                    "name": "Convergence",
                    "password": "bench-password-1",
                },
            )
            account.raise_for_status()
            token = account.json()["access_token"]
            created = await client.post(
                "/api/documents",
                json={"title": f"Convergence {stamp}"},
                headers={"Authorization": f"Bearer {token}"},
            )
            created.raise_for_status()
            doc_id = created.json()["id"]

        # Clients are spread round-robin across replicas — no stickiness.
        participants = [Participant(f"c{i}", ports[i % len(ports)]) for i in range(args.clients)]
        for participant in participants:
            await participant.open(doc_id, token)
        spread = {}
        for participant in participants:
            spread.setdefault(participant.port, 0)
            spread[participant.port] += 1
        print(f"  {args.clients} clients distributed as {spread}")

        rnd = random.Random(args.seed)

        async def churn(participant: Participant, seed: int) -> None:
            local = random.Random(seed)
            for _ in range(args.ops):
                await participant.edit(local)
                await asyncio.sleep(local.uniform(0.005, 0.03))

        started = time.perf_counter()
        await asyncio.gather(*(churn(p, rnd.randrange(1 << 30)) for p in participants))
        elapsed = time.perf_counter() - started
        print(f"  {args.clients * args.ops} concurrent edits issued in {elapsed:.1f}s")

        # Wait for quiescence rather than guessing a settle time.
        #
        # This distinguishes the two things that look identical in a snapshot
        # taken too early: a system that has permanently diverged, and one that
        # is simply still draining. Offered load here deliberately exceeds what
        # one Postgres can absorb, so a backlog is expected — what matters is
        # that it drains to agreement. Eventual consistency is a promise about
        # the limit, not about any particular instant.
        async def gather_server_texts(client) -> dict:
            out = {}
            for port in ports:
                response = await client.get(
                    f"http://127.0.0.1:{port}/api/documents/{doc_id}",
                    headers={"Authorization": f"Bearer {token}"},
                )
                response.raise_for_status()
                out[port] = response.json()["text"]
            return out

        settle_started = time.perf_counter()
        server_texts: dict = {}
        async with httpx.AsyncClient(timeout=30) as client:
            deadline = time.perf_counter() + args.settle_timeout
            previous = None
            while time.perf_counter() < deadline:
                await asyncio.sleep(0.5)
                server_texts = await gather_server_texts(client)
                client_texts = {p.rga.text() for p in participants}
                stable = server_texts == previous
                agreed = len(set(server_texts.values())) == 1 and len(client_texts) == 1
                if agreed and stable:
                    break
                previous = server_texts
        settle_seconds = time.perf_counter() - settle_started

        texts = {p.name: p.rga.text() for p in participants}
        distinct = set(texts.values())
        print(f"  settled after {settle_seconds:.1f}s")

        for participant in participants:
            await participant.close()

        distinct_server = set(server_texts.values())
        agreed = (
            len(distinct) == 1
            and len(distinct_server) == 1
            and (next(iter(distinct)) == next(iter(distinct_server)))
        )

        spans = {json.dumps(spans_of(p.rga)) for p in participants}

        print()
        print(f"  distinct client texts : {len(distinct)}")
        print(f"  distinct client spans : {len(spans)}")
        print(f"  distinct server texts : {len(distinct_server)}")
        print(f"  document length       : {len(next(iter(distinct)))} chars")

        if agreed and len(spans) == 1:
            print(
                f"\nOK: {args.clients} clients across {args.replicas} replicas converged "
                f"on identical text and formatting, and the database agrees"
            )
            return 0

        print("\nFAILED: replicas did not converge", file=sys.stderr)
        for name, text in list(texts.items())[:4]:
            print(f"  {name}: {text[:120]!r}", file=sys.stderr)
        for port, text in server_texts.items():
            print(f"  server:{port}: {text[:120]!r}", file=sys.stderr)
        return 1

    finally:
        for process in processes:
            with contextlib.suppress(ProcessLookupError):
                process.terminate()
            with contextlib.suppress(asyncio.TimeoutError, ProcessLookupError):
                await asyncio.wait_for(process.wait(), timeout=10)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

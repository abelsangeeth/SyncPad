"""``python -m syncpad`` — run the gateway.

Every replica runs this identical command. There is no leader, no shard
assignment and no sticky routing: which replica a socket lands on is the load
balancer's business and nobody else's.
"""

from __future__ import annotations

import argparse
import logging

import uvicorn

from syncpad.config import get_settings


def main() -> None:
    parser = argparse.ArgumentParser(prog="syncpad", description="SyncPad gateway")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true", help="reload on code change")
    parser.add_argument("--log-level", default="info")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)-18s %(message)s",
    )

    settings = get_settings()
    print(f"  SyncPad  ->  http://{args.host}:{args.port}")
    print(f"  database ->  {settings.database_url.split('://', 1)[0]}")
    print(f"  fan-out  ->  {'redis' if settings.uses_redis else 'in-process'}")

    uvicorn.run(
        "syncpad.main:app",
        factory=True,
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level,
        ws="websockets",
    )


if __name__ == "__main__":
    main()

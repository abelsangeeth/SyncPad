"""Assemble the working tree for a Hugging Face Space.

A Space is its own git repo with two fixed requirements: a ``Dockerfile`` at the
repo root, and a ``README.md`` whose YAML front-matter configures the Space. This
project's Dockerfile lives at ``backend/Dockerfile`` and its README is written for
people reading the source, so neither can be used where the Space expects it.

Rather than keep a second Dockerfile that silently drifts from the real one, this
script copies the real one into position. The Space tree is therefore derived,
never edited by hand, and rebuilding it is the only way to change it.

Only what the Dockerfile actually needs is copied. Tests, benchmarks, CI config
and the compose stack stay out: a Space repo is public by default, and a smaller
tree is a faster build and a smaller thing to be wrong about.

    python deploy/hf-space/build_space.py --out build/space

Then, from the project root:

    hf auth login
    hf repos create <user>/<space-name> --type space --space-sdk docker
    hf upload <user>/<space-name> build/space . --type space
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

#: Everything the Dockerfile COPYs, as (source, destination-in-space) pairs.
#: Keep this in step with backend/Dockerfile; the check below fails loudly if a
#: COPY line references something absent.
PAYLOAD: tuple[tuple[str, str], ...] = (
    ("backend/pyproject.toml", "backend/pyproject.toml"),
    ("backend/syncpad", "backend/syncpad"),
    ("backend/alembic.ini", "backend/alembic.ini"),
    ("backend/alembic", "backend/alembic"),
    ("backend/docker-entrypoint.sh", "backend/docker-entrypoint.sh"),
    ("frontend", "frontend"),
    # The real Dockerfile, moved to where a Space requires it. Its COPY paths
    # are already relative to the project root, so they resolve unchanged.
    ("backend/Dockerfile", "Dockerfile"),
)

IGNORE = shutil.ignore_patterns(
    "__pycache__", "*.pyc", "*.pyo", ".pytest_cache", ".ruff_cache", ".hypothesis", "*.db*"
)

FRONT_MATTER = """---
title: {title}
emoji: {emoji}
colorFrom: indigo
colorTo: blue
sdk: docker
app_port: 8000
pinned: false
short_description: Real-time collaborative editor built on an RGA CRDT
---
"""

BODY = """
# SyncPad

A real-time collaborative document editor: many people typing in one document at
once, with edits merged by a **Replicated Growable Array (RGA) CRDT** rather than
by locking. Presence, shared cursors, comments anchored to characters, version
history and offline reconciliation.

The interesting part is that the CRDT is implemented **twice** — once in Python on
the server and once in JavaScript in the browser — and a cross-language
conformance harness generates operation logs in one, replays them in the other in
a different order, and demands identical output. It has caught real bugs that
neither side's own tests could see.

## This Space needs two secrets to start

Set them under **Settings → Variables and secrets**, then restart:

| Secret | Value |
|---|---|
| `SYNCPAD_DATABASE_URL` | A Postgres URL. The session-pooler string from Supabase works as-is, `sslmode` and all. |
| `SYNCPAD_JWT_SECRET` | Any long random string. Every session token is signed with it, so changing it later signs everyone out. |

Without `SYNCPAD_DATABASE_URL` the container **refuses to start**, and says so.
That is deliberate. The setting has a SQLite default, and falling back to it here
would write documents into the container filesystem, report itself healthy, and
discard everything on the next restart. A deployment that will not boot is easier
to diagnose than one that quietly loses data.

If the URL is set but wrong, the container does start and `/readyz` returns
`503` with the reason, so the platform's health check pulls it out rather than
serving sockets it cannot persist.

Schema migrations run on boot because a Space has no release step; see
`backend/docker-entrypoint.sh` for why that is opt-in everywhere else.
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="build/space", help="staging directory")
    parser.add_argument("--title", default="SyncPad")
    parser.add_argument("--emoji", default="\U0001f4dd")
    parser.add_argument(
        "--force", action="store_true", help="replace the staging directory if it exists"
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[2]
    out = (root / args.out).resolve()

    if out.exists():
        if not args.force and any(out.iterdir()):
            print(f"error: {out} exists and is not empty (use --force)", file=sys.stderr)
            return 2
        shutil.rmtree(out)
    out.mkdir(parents=True)

    for src_rel, dst_rel in PAYLOAD:
        src, dst = root / src_rel, out / dst_rel
        if not src.exists():
            print(f"error: {src_rel} is missing from the project", file=sys.stderr)
            return 1
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.is_dir():
            shutil.copytree(src, dst, ignore=IGNORE)
        else:
            shutil.copy2(src, dst)
        print(f"  {src_rel}  ->  {dst_rel}")

    (out / "README.md").write_text(
        FRONT_MATTER.format(title=args.title, emoji=args.emoji) + BODY,
        encoding="utf-8",
    )
    print("  (generated)          ->  README.md")

    # Prove the tree can satisfy its own Dockerfile before a remote build does.
    missing = [
        line.split()[1]
        for line in (out / "Dockerfile").read_text(encoding="utf-8").splitlines()
        if line.startswith("COPY ") and not (out / line.split()[1]).exists()
    ]
    if missing:
        print(f"error: Dockerfile COPYs paths absent from the tree: {missing}", file=sys.stderr)
        return 1

    files = sum(1 for p in out.rglob("*") if p.is_file())
    size = sum(p.stat().st_size for p in out.rglob("*") if p.is_file())
    print(f"\n{out}\n  {files} files, {size / 1024:.0f} KiB, every Dockerfile COPY resolved")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

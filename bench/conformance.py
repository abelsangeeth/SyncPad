"""Cross-language conformance: do the Python and JavaScript CRDTs agree?

SyncPad has two independent RGA implementations — one on the server, one in the
browser — and they only interoperate if they agree exactly on the total order of
operation ids, the integration rule, and the wire format. A disagreement in any
of the three is a split-brain document: two users staring at different text,
each convinced they are looking at the truth.

Unit tests on either side cannot catch that, because each is self-consistent.
So this harness generates random concurrent operation logs with the Python
implementation, ships them to Node, replays them there in a *different* order,
and demands identical text and identical formatting back.

    python bench/conformance.py --trials 200

Exits non-zero on the first disagreement and prints both renderings.
"""

from __future__ import annotations

import argparse
import json
import random
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from syncpad.crdt import RGA, ops_to_dicts  # noqa: E402

ALPHABET = "abcde \né\U0001f30d"
MARK_KEYS = ("bold", "italic", "code")
MARK_VALUES = (True, None)

# The Node side: rebuild the document from the shipped operation log and report
# what it thinks the document says.
DRIVER = """
import { RGA, decodeOps } from '../frontend/src/crdt.js';

let raw = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', (chunk) => { raw += chunk; });
process.stdin.on('end', () => {
  const cases = JSON.parse(raw);
  const results = cases.map((testCase) => {
    const replica = new RGA('js-replica');
    // Several passes so causally early operations drain, exactly as the
    // Python side does when replaying an unordered log.
    for (let pass = 0; pass < 4; pass += 1) {
      replica.applyMany(decodeOps(testCase.ops));
    }
    return {
      text: replica.text(),
      spans: replica.spans().map((s) => [s.text, s.marks]),
    };
  });
  process.stdout.write(JSON.stringify(results));
});
"""


def _spans(doc: RGA) -> list:
    return [[s.text, {k: v for k, v in sorted(s.marks.items())}] for s in doc.spans()]


def build_case(rnd: random.Random, sites: int, steps: int) -> tuple[list, str, list]:
    """Generate one concurrent editing session; return (ops, text, spans)."""
    replicas = [RGA(f"site{i}") for i in range(sites)]
    log: list = []

    for _ in range(steps):
        replica = replicas[rnd.randrange(sites)]
        visible = len(replica)
        roll = rnd.random()

        if roll < 0.6 or visible == 0:
            text = "".join(rnd.choice(ALPHABET) for _ in range(rnd.randint(1, 5)))
            ops = replica.local_insert(rnd.randint(0, visible), text)
        elif roll < 0.8:
            start = rnd.randrange(visible)
            ops = replica.local_delete(start, rnd.randint(1, min(4, visible - start)))
        else:
            start = rnd.randrange(visible)
            ops = replica.local_mark(
                start,
                rnd.randint(1, min(5, visible - start)),
                rnd.choice(MARK_KEYS),
                rnd.choice(MARK_VALUES),
                rnd.choice([True, False]),
            )
        log.extend(ops)

        # Gossip to a random peer so replicas genuinely diverge in between.
        if sites > 1 and rnd.random() < 0.5:
            peer = replicas[rnd.randrange(sites)]
            if peer is not replica:
                peer.apply_many(ops)

    # Settle a reference replica from the shuffled log — the same thing the
    # JavaScript side is about to be asked to do.
    reference = RGA("python-replica")
    shuffled = log[:]
    rnd.shuffle(shuffled)
    for _ in range(4):
        reference.apply_many(shuffled)

    for replica in replicas:
        for _ in range(4):
            replica.apply_many(log)
        if replica.text() != reference.text():
            raise AssertionError("the Python implementation diverged from itself")

    # Ship the log in a different order again, so Node cannot simply replay
    # the sequence Python happened to settle from.
    delivery = log[:]
    rnd.shuffle(delivery)
    return ops_to_dicts(delivery), reference.text(), _spans(reference)


def main() -> int:
    # Windows consoles default to a legacy code page, and the test alphabet is
    # deliberately full of characters it cannot render.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trials", type=int, default=100)
    parser.add_argument("--sites", type=int, default=3)
    parser.add_argument("--steps", type=int, default=25)
    parser.add_argument("--seed", type=int, default=20260920)
    args = parser.parse_args()

    node = shutil.which("node")
    if node is None:
        print("node not found on PATH; skipping conformance check", file=sys.stderr)
        return 0

    rnd = random.Random(args.seed)
    cases, expected = [], []
    for _ in range(args.trials):
        ops, text, spans = build_case(rnd, args.sites, args.steps)
        cases.append({"ops": ops})
        expected.append({"text": text, "spans": spans})

    driver = ROOT / "bench" / "_conformance_driver.mjs"
    driver.write_text(DRIVER, encoding="utf-8")
    try:
        completed = subprocess.run(
            [node, str(driver)],
            input=json.dumps(cases),
            capture_output=True,
            text=True,
            # Windows would otherwise decode the pipe as cp1252 and choke on the
            # first non-Latin-1 character in the test alphabet — which is
            # precisely the content most worth checking for parity.
            encoding="utf-8",
            errors="replace",
            cwd=ROOT,
            timeout=300,
        )
    finally:
        driver.unlink(missing_ok=True)

    if completed.returncode != 0:
        print("node driver failed:", completed.stderr[:4000], file=sys.stderr)
        return 1

    actual = json.loads(completed.stdout)
    failures = 0

    for i, (want, got) in enumerate(zip(expected, actual, strict=True)):
        if want["text"] != got["text"]:
            failures += 1
            print(f"\n--- trial {i}: TEXT DIVERGED ---")
            print(f"  python: {want['text']!r}")
            print(f"  node  : {got['text']!r}")
            if failures >= 3:
                break
            continue

        want_spans = [[t, dict(sorted(m.items()))] for t, m in want["spans"]]
        got_spans = [[t, dict(sorted(m.items()))] for t, m in got["spans"]]
        if want_spans != got_spans:
            failures += 1
            print(f"\n--- trial {i}: FORMATTING DIVERGED ---")
            print(f"  python: {want_spans}")
            print(f"  node  : {got_spans}")
            if failures >= 3:
                break

    total_ops = sum(len(c["ops"]) for c in cases)
    if failures:
        print(f"\nFAILED: {failures} of {args.trials} trials diverged")
        return 1

    print(
        f"OK: {args.trials} trials, {total_ops} operations, "
        f"{args.sites} concurrent sites — Python and JavaScript agree exactly"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

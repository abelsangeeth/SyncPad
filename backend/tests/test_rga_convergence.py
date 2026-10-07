"""Strong eventual consistency, established by randomised search rather than argument.

This is the test the whole project rests on. Every other component — the Redis
fan-out, the stateless gateway, the offline buffer in the browser — is only
allowed to be careless about ordering and duplication because of the property
asserted here:

    replicas that have received the same set of operations hold the same
    document, no matter what order those operations arrived in, how many times
    they were repeated, or which replica issued them.

Hand-written cases cannot establish that; there are too many interleavings.
So the tests below generate the interleavings instead.
"""

from __future__ import annotations

import random

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from syncpad.crdt import RGA, Op

ALPHABET = "ab ñ🌍\n"
MARK_KEYS = ("bold", "italic", "code")
MARK_VALUES = (True, None, "on")


def _spans_key(doc: RGA) -> tuple:
    """A hashable, comparable rendering of a document's text AND formatting."""
    return tuple((s.text, tuple(sorted(s.marks.items()))) for s in doc.spans())


def _deliver_everything(replicas: list[RGA], ops: list[Op], rnd: random.Random) -> None:
    """Flood every replica with every op, shuffled and duplicated.

    Repeated passes let causally buffered operations drain: an insert whose left
    origin arrives later is held back, so one pass is not enough to conclude that
    it was dropped.
    """
    for replica in replicas:
        order = ops + ops
        rnd.shuffle(order)
        for _ in range(3):
            for op in order:
                replica.apply(op)


@st.composite
def _edit_script(draw: st.DrawFn) -> tuple[int, list[tuple], int]:
    n_sites = draw(st.integers(min_value=2, max_value=4))
    steps = draw(
        st.lists(
            st.tuples(
                st.integers(min_value=0, max_value=3),  # which site
                st.sampled_from(("ins", "del", "mark")),
                st.integers(min_value=0, max_value=10_000),  # position seed
                st.integers(min_value=1, max_value=6),  # length seed
                st.text(alphabet=ALPHABET, min_size=1, max_size=5),
                st.sampled_from(MARK_KEYS),
                st.sampled_from(MARK_VALUES),
                st.booleans(),  # mark expand
                st.booleans(),  # gossip to a peer immediately
                st.integers(min_value=0, max_value=10_000),  # gossip partner seed
            ),
            min_size=1,
            max_size=40,
        )
    )
    return n_sites, steps, draw(st.integers(min_value=0, max_value=2**31))


@given(_edit_script())
@settings(
    max_examples=250,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
def test_replicas_converge_under_arbitrary_interleaving(script: tuple) -> None:
    n_sites, steps, seed = script
    rnd = random.Random(seed)
    replicas = [RGA(f"site{i}") for i in range(n_sites)]
    every_op: list[Op] = []

    for site_i, kind, pos_seed, length_seed, text, key, value, expand, gossip, partner in steps:
        replica = replicas[site_i % n_sites]
        visible = len(replica)
        ops: list[Op] = []

        if kind == "ins" or visible == 0:
            ops = replica.local_insert(pos_seed % (visible + 1), text)
        elif kind == "del":
            start = pos_seed % visible
            ops = replica.local_delete(start, min(length_seed, visible - start))
        else:
            start = pos_seed % visible
            ops = replica.local_mark(start, min(length_seed, visible - start), key, value, expand)

        every_op.extend(ops)
        # Partial, out-of-order gossip while editing continues — this is what
        # produces genuinely concurrent states rather than a tidy final merge.
        if gossip and n_sites > 1:
            peer = replicas[partner % n_sites]
            if peer is not replica:
                peer.apply_many(ops)

    _deliver_everything(replicas, every_op, rnd)

    texts = {r.text() for r in replicas}
    assert len(texts) == 1, f"replicas diverged on text: {texts}"
    spans = {_spans_key(r) for r in replicas}
    assert len(spans) == 1, f"replicas diverged on formatting: {spans}"

    for replica in replicas:
        replica.check_invariants()


@given(_edit_script())
@settings(max_examples=120, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_replaying_the_whole_log_changes_nothing(script: tuple) -> None:
    """Idempotence at the log level: re-delivery is always safe.

    The gateway relies on this. Redis Pub/Sub is at-most-once, reconnecting
    clients re-send their unacknowledged buffer, and the room applies ops it may
    have already seen — none of which needs deduplication if this holds.
    """
    n_sites, steps, seed = script
    replicas = [RGA(f"site{i}") for i in range(n_sites)]
    every_op: list[Op] = []

    for site_i, kind, pos_seed, length_seed, text, key, value, expand, _g, _p in steps:
        replica = replicas[site_i % n_sites]
        visible = len(replica)
        if kind == "ins" or visible == 0:
            every_op.extend(replica.local_insert(pos_seed % (visible + 1), text))
        elif kind == "del":
            start = pos_seed % visible
            every_op.extend(replica.local_delete(start, min(length_seed, visible - start)))
        else:
            start = pos_seed % visible
            every_op.extend(
                replica.local_mark(start, min(length_seed, visible - start), key, value, expand)
            )

    target = RGA("fresh")
    _deliver_everything([target], every_op, random.Random(seed))
    settled_text, settled_spans = target.text(), _spans_key(target)

    assert target.apply_many(every_op) == 0, "a settled replica re-applied an op"
    assert target.text() == settled_text
    assert _spans_key(target) == settled_spans


@given(_edit_script())
@settings(max_examples=120, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_causal_order_and_shuffled_order_agree(script: tuple) -> None:
    """Delivery order must be irrelevant, not merely usually harmless."""
    n_sites, steps, seed = script
    source = [RGA(f"site{i}") for i in range(n_sites)]
    every_op: list[Op] = []

    for site_i, kind, pos_seed, length_seed, text, key, value, expand, _g, _p in steps:
        replica = source[site_i % n_sites]
        visible = len(replica)
        if kind == "ins" or visible == 0:
            every_op.extend(replica.local_insert(pos_seed % (visible + 1), text))
        elif kind == "del":
            start = pos_seed % visible
            every_op.extend(replica.local_delete(start, min(length_seed, visible - start)))
        else:
            start = pos_seed % visible
            every_op.extend(
                replica.local_mark(start, min(length_seed, visible - start), key, value, expand)
            )

    in_order = RGA("ordered")
    for _ in range(3):
        in_order.apply_many(every_op)

    shuffled = RGA("shuffled")
    _deliver_everything([shuffled], every_op, random.Random(seed ^ 0x5EED))

    assert in_order.text() == shuffled.text()
    assert _spans_key(in_order) == _spans_key(shuffled)


@given(_edit_script())
@settings(max_examples=100, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_snapshot_restored_replica_stays_convergent(script: tuple) -> None:
    """A replica rebuilt from a snapshot must be indistinguishable from one that
    never restarted — including for operations that arrive afterwards."""
    n_sites, steps, seed = script
    replicas = [RGA(f"site{i}") for i in range(n_sites)]
    first_half: list[Op] = []
    second_half: list[Op] = []

    for idx, step in enumerate(steps):
        site_i, kind, pos_seed, length_seed, text, key, value, expand, _g, _p = step
        replica = replicas[site_i % n_sites]
        visible = len(replica)
        if kind == "ins" or visible == 0:
            ops = replica.local_insert(pos_seed % (visible + 1), text)
        elif kind == "del":
            start = pos_seed % visible
            ops = replica.local_delete(start, min(length_seed, visible - start))
        else:
            start = pos_seed % visible
            ops = replica.local_mark(start, min(length_seed, visible - start), key, value, expand)
        (first_half if idx < len(steps) // 2 else second_half).extend(ops)

    rnd = random.Random(seed)
    live = RGA("live")
    _deliver_everything([live], first_half, rnd)

    restarted = RGA.load("restarted", live.snapshot())
    _deliver_everything([live, restarted], second_half, rnd)

    assert live.text() == restarted.text()
    assert _spans_key(live) == _spans_key(restarted)
    restarted.check_invariants()


class ConvergenceMachine(RuleBasedStateMachine):
    """Stateful search for an interleaving that breaks convergence.

    Hypothesis drives three replicas through interleaved edits and partial
    syncs, shrinking any counterexample to a minimal reproduction — which is
    far more useful for debugging a CRDT than a 40-step random trace.
    """

    def __init__(self) -> None:
        super().__init__()
        self.replicas = [RGA(f"m{i}") for i in range(3)]
        self.log: list[Op] = []

    @rule(
        which=st.integers(0, 2),
        pos=st.integers(0, 9999),
        text=st.text(alphabet=ALPHABET, min_size=1, max_size=4),
    )
    def insert(self, which: int, pos: int, text: str) -> None:
        replica = self.replicas[which]
        self.log.extend(replica.local_insert(pos % (len(replica) + 1), text))

    @precondition(lambda self: any(len(r) for r in self.replicas))
    @rule(which=st.integers(0, 2), pos=st.integers(0, 9999), length=st.integers(1, 4))
    def delete(self, which: int, pos: int, length: int) -> None:
        replica = self.replicas[which]
        if not len(replica):
            return
        start = pos % len(replica)
        self.log.extend(replica.local_delete(start, min(length, len(replica) - start)))

    @precondition(lambda self: any(len(r) for r in self.replicas))
    @rule(
        which=st.integers(0, 2),
        pos=st.integers(0, 9999),
        length=st.integers(1, 4),
        key=st.sampled_from(MARK_KEYS),
        value=st.sampled_from(MARK_VALUES),
        expand=st.booleans(),
    )
    def mark(self, which: int, pos: int, length: int, key: str, value, expand: bool) -> None:
        replica = self.replicas[which]
        if not len(replica):
            return
        start = pos % len(replica)
        self.log.extend(
            replica.local_mark(start, min(length, len(replica) - start), key, value, expand)
        )

    @rule(which=st.integers(0, 2))
    def partial_sync(self, which: int) -> None:
        """Deliver a prefix of the log — simulating a slow or lagging replica."""
        self.replicas[which].apply_many(self.log[: len(self.log) // 2 + 1])

    @invariant()
    def structure_holds(self) -> None:
        for replica in self.replicas:
            replica.check_invariants()

    def teardown(self) -> None:
        for _ in range(3):
            for replica in self.replicas:
                replica.apply_many(self.log)
        texts = {r.text() for r in self.replicas}
        assert len(texts) == 1, f"diverged: {texts}"
        assert len({_spans_key(r) for r in self.replicas}) == 1, "formatting diverged"


TestConvergenceMachine = ConvergenceMachine.TestCase
TestConvergenceMachine.settings = settings(
    max_examples=60,
    stateful_step_count=40,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)

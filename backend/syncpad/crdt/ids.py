"""Identity and causality primitives for the CRDT layer.

Every character SyncPad has ever contained is named by an :class:`OpId`. That name
is globally unique, immutable, and totally ordered — which is what lets replicas
that have never spoken to each other agree on document order without a lock, a
server round-trip, or a transform matrix.

The total order is ``(counter, site)``. ``counter`` is a Lamport clock, so it
encodes causality: if site A saw element X before creating element Y, then
``Y.counter > X.counter``. ``site`` only ever breaks ties between genuinely
concurrent operations, and it does so the same way on every replica.

That single invariant — *a child's id always sorts above its parent's* — is what
makes the RGA integration rule in :mod:`syncpad.crdt.rga` provably convergent.
"""

from __future__ import annotations

import base64
import uuid
from dataclasses import dataclass

__all__ = [
    "LamportClock",
    "OpId",
    "VersionVector",
    "new_site_id",
    "vv_covers",
    "vv_merge",
    "vv_min",
]

#: Site -> highest counter that site has been observed to produce.
VersionVector = dict[str, int]


def new_site_id() -> str:
    """Return a fresh 22-character urlsafe site identifier.

    Sites are per-connection, not per-user: one human with two browser tabs is two
    sites, and that is correct — the tabs edit concurrently and must not share a
    Lamport counter.
    """
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b"=").decode("ascii")


@dataclass(frozen=True, slots=True, order=False)
class OpId:
    """A globally unique, totally ordered operation identifier.

    Ordering is ``(counter, site)`` ascending. Equality and hashing come from the
    dataclass, so an ``OpId`` is usable as a dict key and set member — which the
    RGA relies on heavily for O(1) element lookup.
    """

    site: str
    counter: int

    def __post_init__(self) -> None:
        if ":" in self.site:
            raise ValueError(f"site id may not contain ':' (got {self.site!r})")
        if not self.site:
            raise ValueError("site id may not be empty")
        if self.counter < 1:
            raise ValueError(f"counter must be >= 1 (got {self.counter})")

    # Ordering is defined explicitly rather than via order=True because the field
    # declaration order (site, counter) is deliberately NOT the sort order.
    def __lt__(self, other: OpId) -> bool:
        if not isinstance(other, OpId):
            return NotImplemented
        return (self.counter, self.site) < (other.counter, other.site)

    def __le__(self, other: OpId) -> bool:
        if not isinstance(other, OpId):
            return NotImplemented
        return (self.counter, self.site) <= (other.counter, other.site)

    def __gt__(self, other: OpId) -> bool:
        if not isinstance(other, OpId):
            return NotImplemented
        return (self.counter, self.site) > (other.counter, other.site)

    def __ge__(self, other: OpId) -> bool:
        if not isinstance(other, OpId):
            return NotImplemented
        return (self.counter, self.site) >= (other.counter, other.site)

    def offset(self, n: int) -> OpId:
        """Return the id ``n`` places along this id's run.

        An insert of ``"abc"`` allocates one op id and three consecutive element
        ids; this is how the run expands.
        """
        return OpId(self.site, self.counter + n)

    def encode(self) -> str:
        return f"{self.site}:{self.counter}"

    @classmethod
    def decode(cls, s: str) -> OpId:
        site, sep, counter = s.rpartition(":")
        if not sep or not site:
            raise ValueError(f"malformed OpId {s!r}")
        try:
            return cls(site, int(counter))
        except ValueError as exc:
            raise ValueError(f"malformed OpId {s!r}") from exc

    def __repr__(self) -> str:  # pragma: no cover - debugging affordance
        return f"OpId({self.site[:6]}…:{self.counter})"


class LamportClock:
    """A per-site Lamport clock that hands out monotonically increasing ids.

    ``observe`` must be called for every remote operation applied. Skipping it
    breaks the "child sorts above parent" invariant and, with it, convergence —
    so the RGA calls it on every single apply rather than trusting callers.
    """

    __slots__ = ("_counter", "_site")

    def __init__(self, site: str, counter: int = 0) -> None:
        if ":" in site or not site:
            raise ValueError(f"invalid site id {site!r}")
        if counter < 0:
            raise ValueError("counter must be >= 0")
        self._site = site
        self._counter = counter

    @property
    def site(self) -> str:
        return self._site

    @property
    def counter(self) -> int:
        return self._counter

    def tick(self) -> OpId:
        """Advance by one and return the new id."""
        self._counter += 1
        return OpId(self._site, self._counter)

    def tick_run(self, n: int) -> OpId:
        """Reserve ``n`` consecutive ids and return the FIRST one.

        The caller owns ids ``[id, id.offset(n - 1)]`` inclusive.
        """
        if n < 1:
            raise ValueError("run length must be >= 1")
        start = self._counter + 1
        self._counter += n
        return OpId(self._site, start)

    def observe(self, other: OpId) -> None:
        """Absorb a remote id so locally generated ids sort above it."""
        if other.counter > self._counter:
            self._counter = other.counter

    def __repr__(self) -> str:  # pragma: no cover - debugging affordance
        return f"LamportClock({self._site[:6]}…, {self._counter})"


def vv_merge(a: VersionVector, b: VersionVector) -> VersionVector:
    """Pointwise maximum of two version vectors."""
    out = dict(a)
    for site, counter in b.items():
        if counter > out.get(site, 0):
            out[site] = counter
    return out


def vv_covers(vv: VersionVector, oid: OpId) -> bool:
    """True when ``vv`` implies ``oid`` has already been observed."""
    return vv.get(oid.site, 0) >= oid.counter


def vv_min(vectors: list[VersionVector]) -> VersionVector:
    """Pointwise minimum across every vector, over the union of all sites.

    A site missing from any vector contributes 0, because "I have never heard of
    this site" is strictly weaker than any counter it may have reached. This is
    the safety boundary for tombstone GC: only operations below the minimum are
    known to every participant and therefore safe to forget.
    """
    if not vectors:
        return {}
    sites: set[str] = set()
    for vv in vectors:
        sites.update(vv)
    return {site: min(vv.get(site, 0) for vv in vectors) for site in sites}

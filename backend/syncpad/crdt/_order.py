"""An order-statistic treap: the index that makes the RGA fast.

The RGA needs four things from its sequence structure, and a Python ``list``
gives good numbers for only three of them:

==========================  ============  ===========
operation                   list          this treap
==========================  ============  ===========
insert after a known point  O(n) memmove  O(log n)
visible-index -> element    O(n) scan     O(log n)
element -> visible-index    O(n) scan     O(log n)
tombstone an element        O(1)          O(log n)
==========================  ============  ===========

The expensive one for a list is *element -> index*: a document with fifty
editors resolves cursor and mark anchors constantly, and an O(n) scan per
resolution is what turns a large document into a slideshow. A treap with parent
pointers answers it by walking up to the root, summing the visible counts of the
left subtrees it emerges from.

Each node caches two subtree aggregates: ``size`` (all nodes, tombstones
included, because tombstones remain addressable as insertion anchors) and
``vis`` (non-tombstoned nodes only, which is what maps to user-visible text).

Priorities are derived deterministically from the element id rather than drawn
at random. The tree is a purely local index — replicas need not agree on its
shape — but a deterministic shape makes a failing test reproduce on the first
try instead of the fortieth.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Generic, TypeVar

__all__ = ["OrderTreap", "TreapNode"]

T = TypeVar("T")

_MASK64 = (1 << 64) - 1


def _priority(site: str, counter: int) -> int:
    """A stable, well-spread priority from an element's identity.

    SplitMix64's finalizer: cheap, and good enough that the treap stays within a
    small constant of ``log2(n)`` depth in practice.
    """
    x = (counter * 0x9E3779B97F4A7C15 + (hash_str := _stable_hash(site))) & _MASK64
    x ^= x >> 30
    x = (x * 0xBF58476D1CE4E5B9) & _MASK64
    x ^= x >> 27
    x = (x * 0x94D049BB133111EB) & _MASK64
    x ^= x >> 31
    return x ^ (hash_str & 0xFFFF)


def _stable_hash(s: str) -> int:
    """FNV-1a over the site id.

    Python's built-in ``hash`` for ``str`` is salted per process, which would
    make treap shape — and therefore any shape-sensitive test failure — vary run
    to run. FNV-1a does not.
    """
    h = 0xCBF29CE484222325
    for byte in s.encode("utf-8"):
        h ^= byte
        h = (h * 0x100000001B3) & _MASK64
    return h


class TreapNode(Generic[T]):
    """A single position in the sequence.

    ``value`` is the caller's payload (an RGA ``Element``); ``visible`` is the
    only mutable aggregate input, flipped when a character is tombstoned.
    """

    __slots__ = ("left", "parent", "prio", "right", "size", "value", "vis", "visible")

    def __init__(self, value: T, prio: int) -> None:
        self.value = value
        self.prio = prio
        self.left: TreapNode[T] | None = None
        self.right: TreapNode[T] | None = None
        self.parent: TreapNode[T] | None = None
        self.size = 1
        self.vis = 1
        self.visible = True


def _size(n: TreapNode[T] | None) -> int:
    return n.size if n is not None else 0


def _vis(n: TreapNode[T] | None) -> int:
    return n.vis if n is not None else 0


def _pull(n: TreapNode[T]) -> None:
    """Recompute a node's aggregates from its children."""
    n.size = 1 + _size(n.left) + _size(n.right)
    n.vis = (1 if n.visible else 0) + _vis(n.left) + _vis(n.right)


class OrderTreap(Generic[T]):
    """A sequence supporting insert-after, rank, and select in O(log n)."""

    __slots__ = ("_root",)

    def __init__(self) -> None:
        self._root: TreapNode[T] | None = None

    # ------------------------------------------------------------------ size
    def __len__(self) -> int:
        """Total positions, tombstones included."""
        return _size(self._root)

    @property
    def visible_count(self) -> int:
        return _vis(self._root)

    # ---------------------------------------------------------------- rotate
    def _rotate_up(self, n: TreapNode[T]) -> None:
        """Rotate ``n`` above its parent, fixing aggregates and parent links."""
        p = n.parent
        if p is None:
            return
        g = p.parent
        if p.left is n:
            p.left = n.right
            if n.right is not None:
                n.right.parent = p
            n.right = p
        else:
            p.right = n.left
            if n.left is not None:
                n.left.parent = p
            n.left = p
        p.parent = n
        n.parent = g
        if g is None:
            self._root = n
        elif g.left is p:
            g.left = n
        else:
            g.right = n
        _pull(p)
        _pull(n)

    def _bubble_up(self, n: TreapNode[T]) -> None:
        """Restore the heap property, then refresh aggregates to the root."""
        while n.parent is not None and n.prio > n.parent.prio:
            self._rotate_up(n)
        cur = n
        while cur is not None:
            _pull(cur)
            cur = cur.parent

    def _fix_up(self, n: TreapNode[T] | None) -> None:
        while n is not None:
            _pull(n)
            n = n.parent

    # ---------------------------------------------------------------- insert
    def insert_after(
        self, ref: TreapNode[T] | None, value: T, *, site: str, counter: int
    ) -> TreapNode[T]:
        """Insert ``value`` immediately after ``ref`` (or at the head if None)."""
        node: TreapNode[T] = TreapNode(value, _priority(site, counter))
        if self._root is None:
            self._root = node
            return node

        if ref is None:
            # Head insert: become the left child of the leftmost node.
            cur = self._root
            while cur.left is not None:
                cur = cur.left
            cur.left = node
            node.parent = cur
        elif ref.right is None:
            ref.right = node
            node.parent = ref
        else:
            cur = ref.right
            while cur.left is not None:
                cur = cur.left
            cur.left = node
            node.parent = cur

        self._bubble_up(node)
        return node

    # ------------------------------------------------------------- tombstone
    def set_visible(self, node: TreapNode[T], visible: bool) -> bool:
        """Flip a node's visibility. Returns False when it was already so."""
        if node.visible == visible:
            return False
        node.visible = visible
        self._fix_up(node)
        return True

    # -------------------------------------------------------------- ordering
    def rank(self, node: TreapNode[T]) -> int:
        """Absolute position of ``node``, tombstones counted."""
        idx = _size(node.left)
        cur = node
        p = cur.parent
        while p is not None:
            if p.right is cur:
                idx += _size(p.left) + 1
            cur = p
            p = cur.parent
        return idx

    def visible_rank(self, node: TreapNode[T]) -> int:
        """Visible position of ``node``: how much user-visible text precedes it.

        Defined for tombstoned nodes too, where it reports the position the
        character *would* occupy — which is exactly what a cursor anchored to
        deleted text needs in order to land somewhere sensible.
        """
        idx = _vis(node.left)
        cur = node
        p = cur.parent
        while p is not None:
            if p.right is cur:
                idx += _vis(p.left) + (1 if p.visible else 0)
            cur = p
            p = cur.parent
        return idx

    def select_visible(self, k: int) -> TreapNode[T] | None:
        """The ``k``-th visible node (0-based), or None if out of range."""
        if k < 0:
            return None
        cur = self._root
        while cur is not None:
            left_vis = _vis(cur.left)
            if k < left_vis:
                cur = cur.left
            elif cur.visible and k == left_vis:
                return cur
            else:
                k -= left_vis + (1 if cur.visible else 0)
                cur = cur.right
        return None

    def node_at(self, index: int) -> TreapNode[T] | None:
        """The node at absolute position ``index``, tombstones counted."""
        if index < 0:
            return None
        cur = self._root
        while cur is not None:
            left_size = _size(cur.left)
            if index < left_size:
                cur = cur.left
            elif index == left_size:
                return cur
            else:
                index -= left_size + 1
                cur = cur.right
        return None

    # ------------------------------------------------------------- traversal
    def successor(self, node: TreapNode[T]) -> TreapNode[T] | None:
        """The next node in sequence order, tombstones included."""
        if node.right is not None:
            cur = node.right
            while cur.left is not None:
                cur = cur.left
            return cur
        cur = node
        p = cur.parent
        while p is not None and p.right is cur:
            cur = p
            p = cur.parent
        return p

    def first(self) -> TreapNode[T] | None:
        cur = self._root
        if cur is None:
            return None
        while cur.left is not None:
            cur = cur.left
        return cur

    def __iter__(self) -> Iterator[TreapNode[T]]:
        """In-order walk. Iterative, so a degenerate tree cannot blow the stack."""
        stack: list[TreapNode[T]] = []
        cur = self._root
        while stack or cur is not None:
            while cur is not None:
                stack.append(cur)
                cur = cur.left
            cur = stack.pop()
            yield cur
            cur = cur.right

    def values(self) -> Iterator[T]:
        for node in self:
            yield node.value

    # ------------------------------------------------------------ rebuilding
    def rebuild(self, nodes: list[TreapNode[T]]) -> None:
        """Rebuild the tree from an ordered node list, reusing the node objects.

        Used by tombstone GC, which drops nodes wholesale; rebuilding from the
        survivors is both simpler and faster than n individual deletions, and it
        keeps the ``OpId -> node`` map valid because the objects are reused.
        """
        for n in nodes:
            n.left = n.right = n.parent = None
            n.size = 1
            n.vis = 1 if n.visible else 0
        self._root = self._build(nodes, 0, len(nodes))

    def _build(self, nodes: list[TreapNode[T]], lo: int, hi: int) -> TreapNode[T] | None:
        """Build a balanced tree over ``nodes[lo:hi]``, ignoring priorities.

        Priorities exist to randomise incremental inserts; a bulk rebuild can do
        better by simply balancing, so the result is a perfect BST by position.
        """
        if lo >= hi:
            return None
        mid = (lo + hi) // 2
        node = nodes[mid]
        node.left = self._build(nodes, lo, mid)
        node.right = self._build(nodes, mid + 1, hi)
        if node.left is not None:
            node.left.parent = node
        if node.right is not None:
            node.right.parent = node
        # Keep the heap property consistent with the new shape so later
        # incremental inserts cannot rotate the balanced tree apart.
        node.prio = (
            max(
                node.prio,
                (node.left.prio if node.left else 0),
                (node.right.prio if node.right else 0),
            )
            + 1
        )
        _pull(node)
        return node

    # ------------------------------------------------------------ invariants
    def check(self) -> None:
        """Assert every structural invariant. Test-only; O(n)."""

        def walk(n: TreapNode[T] | None, parent: TreapNode[T] | None) -> tuple[int, int]:
            if n is None:
                return 0, 0
            assert n.parent is parent, "parent pointer mismatch"
            if parent is not None:
                assert n.prio <= parent.prio, "heap property violated"
            ls, lv = walk(n.left, n)
            rs, rv = walk(n.right, n)
            assert n.size == ls + rs + 1, f"size cache wrong at {n.size}"
            assert n.vis == lv + rv + (1 if n.visible else 0), "vis cache wrong"
            return n.size, n.vis

        walk(self._root, None)

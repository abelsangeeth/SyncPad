"""A token bucket, per connection.

Rate limiting a collaborative editor is a balancing act. Real typing is bursty —
a fast typist produces a flurry, then pauses — so a flat per-second cap either
throttles legitimate users or is set so high it stops being a limit. A token
bucket handles both: sustained rate is bounded by the refill rate, while the
bucket size absorbs bursts.

The clock is injectable so the tests can assert refill behaviour precisely
instead of sleeping and hoping.
"""

from __future__ import annotations

import time
from collections.abc import Callable

__all__ = ["TokenBucket"]


class TokenBucket:
    """Classic token bucket over a monotonic clock.

    ``capacity`` tokens accumulate at ``rate`` per second. Each operation costs
    one token; when the bucket is empty, requests are refused rather than queued
    — a queue would just convert a flood into unbounded latency and memory.
    """

    __slots__ = ("_capacity", "_clock", "_last", "_rate", "_tokens")

    def __init__(
        self,
        rate: float,
        capacity: int,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        self._rate = rate
        self._capacity = float(capacity)
        self._tokens = float(capacity)
        self._clock = clock
        self._last = clock()

    @property
    def tokens(self) -> float:
        return self._tokens

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last = now

    def allow(self, cost: int = 1) -> bool:
        """Consume ``cost`` tokens if available. Returns False when throttled."""
        if cost <= 0:
            return True
        self._refill()
        if self._tokens >= cost:
            self._tokens -= cost
            return True
        return False

    def retry_after(self, cost: int = 1) -> float:
        """Seconds until ``cost`` tokens would be available, for client backoff."""
        self._refill()
        shortfall = cost - self._tokens
        return max(0.0, shortfall / self._rate)

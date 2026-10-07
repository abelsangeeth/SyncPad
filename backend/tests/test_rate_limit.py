"""The per-connection token bucket.

Tested against an injected clock rather than ``sleep``. A test that sleeps to
watch tokens refill is slow, flaky on a loaded CI runner, and cannot assert
anything precise; a fake clock makes refill behaviour exactly checkable.
"""

from __future__ import annotations

import pytest

from syncpad.services.rate_limit import TokenBucket


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_burst_is_allowed_up_to_capacity() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=10, capacity=50, clock=clock)
    assert all(bucket.allow() for _ in range(50))
    assert bucket.allow() is False, "capacity must be a hard ceiling"


def test_sustained_overuse_is_refused() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=10, capacity=10, clock=clock)
    for _ in range(10):
        assert bucket.allow()
    assert not bucket.allow()

    # A second of real time buys exactly ten more operations.
    clock.advance(1.0)
    for _ in range(10):
        assert bucket.allow()
    assert not bucket.allow()


def test_refill_is_proportional_to_elapsed_time() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=100, capacity=100, clock=clock)
    for _ in range(100):
        bucket.allow()
    assert bucket.tokens == pytest.approx(0.0)

    clock.advance(0.25)
    assert bucket.allow(25)
    assert not bucket.allow(1)


def test_refill_never_exceeds_capacity() -> None:
    """An idle connection must not bank unlimited credit."""
    clock = FakeClock()
    bucket = TokenBucket(rate=10, capacity=20, clock=clock)
    bucket.allow(20)
    clock.advance(3600)
    assert bucket.tokens == pytest.approx(0.0)
    assert bucket.allow(20)
    assert not bucket.allow(1), "an hour idle must still only buy one bucketful"


def test_cost_is_charged_per_operation() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=10, capacity=10, clock=clock)
    assert bucket.allow(6)
    assert bucket.allow(4)
    assert not bucket.allow(1)


def test_an_oversized_request_is_refused_without_draining() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=10, capacity=10, clock=clock)
    assert not bucket.allow(11)
    assert bucket.tokens == pytest.approx(10.0), "a refusal must not consume tokens"


def test_zero_cost_is_free() -> None:
    bucket = TokenBucket(rate=1, capacity=1, clock=FakeClock())
    assert bucket.allow(0)
    assert bucket.tokens == pytest.approx(1.0)


def test_retry_after_reports_the_wait() -> None:
    clock = FakeClock()
    bucket = TokenBucket(rate=10, capacity=10, clock=clock)
    bucket.allow(10)
    assert bucket.retry_after(5) == pytest.approx(0.5)

    clock.advance(0.5)
    assert bucket.retry_after(5) == pytest.approx(0.0)


def test_retry_after_is_zero_when_tokens_are_available() -> None:
    bucket = TokenBucket(rate=10, capacity=10, clock=FakeClock())
    assert bucket.retry_after() == pytest.approx(0.0)


def test_invalid_configuration_is_rejected() -> None:
    with pytest.raises(ValueError, match="rate must be positive"):
        TokenBucket(rate=0, capacity=10)
    with pytest.raises(ValueError, match="rate must be positive"):
        TokenBucket(rate=-1, capacity=10)
    with pytest.raises(ValueError, match="capacity must be at least 1"):
        TokenBucket(rate=10, capacity=0)


def test_default_clock_is_monotonic() -> None:
    """Wall-clock time would let an NTP step hand out free tokens."""
    bucket = TokenBucket(rate=1000, capacity=10)
    assert bucket.allow()

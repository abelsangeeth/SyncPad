"""Cross-replica fan-out.

The test that matters here uses *two separate hub instances* sharing one broker.
That is not a stylistic choice — a single-instance test would pass even if the
hub were a plain local dict, which is exactly the bug that would silently split
a document across two replicas in production. Two instances make the test
meaningful: a publish on "replica A" must reach a subscriber on "replica B".
"""

from __future__ import annotations

import asyncio

import pytest

from syncpad.config import Settings
from syncpad.gateway.hub import InMemoryHub, RedisHub, channel_for, make_hub

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def clean_brokers():
    InMemoryHub.reset_brokers()
    yield
    InMemoryHub.reset_brokers()


async def test_publish_reaches_a_local_subscriber() -> None:
    hub = InMemoryHub("t1")
    await hub.start()
    received: list[bytes] = []

    await hub.subscribe("doc-1", lambda payload: _collect(received, payload))
    await hub.publish("doc-1", b"hello")

    assert received == [b"hello"]
    await hub.close()


async def test_publish_crosses_replicas() -> None:
    """The load-bearing test: two hubs, one broker, one message."""
    replica_a, replica_b = InMemoryHub("shared"), InMemoryHub("shared")
    await replica_a.start()
    await replica_b.start()
    assert replica_a.gateway_id != replica_b.gateway_id

    on_b: list[bytes] = []
    await replica_b.subscribe("doc-1", lambda p: _collect(on_b, p))
    await replica_a.publish("doc-1", b"from-a")

    assert on_b == [b"from-a"], "a publish on one replica must reach the other"

    await replica_a.close()
    await replica_b.close()


async def test_separate_brokers_are_isolated() -> None:
    """Guards the test harness itself: different brokers must not leak."""
    one, two = InMemoryHub("broker-1"), InMemoryHub("broker-2")
    await one.start()
    await two.start()

    seen: list[bytes] = []
    await two.subscribe("doc-1", lambda p: _collect(seen, p))
    await one.publish("doc-1", b"should-not-arrive")

    assert seen == []
    await one.close()
    await two.close()


async def test_subscriptions_are_per_document() -> None:
    hub = InMemoryHub("t2")
    await hub.start()
    doc_a: list[bytes] = []
    doc_b: list[bytes] = []

    await hub.subscribe("a", lambda p: _collect(doc_a, p))
    await hub.subscribe("b", lambda p: _collect(doc_b, p))
    await hub.publish("a", b"for-a")

    assert doc_a == [b"for-a"]
    assert doc_b == []
    await hub.close()


async def test_every_subscriber_of_a_document_receives() -> None:
    replica_a, replica_b = InMemoryHub("fanout"), InMemoryHub("fanout")
    await replica_a.start()
    await replica_b.start()

    first: list[bytes] = []
    second: list[bytes] = []
    third: list[bytes] = []
    await replica_a.subscribe("doc", lambda p: _collect(first, p))
    await replica_b.subscribe("doc", lambda p: _collect(second, p))
    await replica_b.subscribe("doc", lambda p: _collect(third, p))

    await replica_a.publish("doc", b"broadcast")

    assert first == second == third == [b"broadcast"]
    await replica_a.close()
    await replica_b.close()


async def test_unsubscribe_stops_delivery() -> None:
    hub = InMemoryHub("t3")
    await hub.start()
    received: list[bytes] = []

    token = await hub.subscribe("doc", lambda p: _collect(received, p))
    await hub.publish("doc", b"one")
    await hub.unsubscribe(token)
    await hub.publish("doc", b"two")

    assert received == [b"one"]
    await hub.close()


async def test_unsubscribing_twice_is_harmless() -> None:
    hub = InMemoryHub("t4")
    await hub.start()
    token = await hub.subscribe("doc", lambda p: _collect([], p))
    await hub.unsubscribe(token)
    await hub.unsubscribe(token)
    await hub.close()


async def test_a_failing_subscriber_does_not_stop_the_others() -> None:
    """One broken socket must not silence a document for everyone else."""
    hub = InMemoryHub("t5")
    await hub.start()
    survived: list[bytes] = []

    async def explode(_payload: bytes) -> None:
        raise RuntimeError("this subscriber is broken")

    await hub.subscribe("doc", explode)
    await hub.subscribe("doc", lambda p: _collect(survived, p))
    await hub.publish("doc", b"still-delivered")

    assert survived == [b"still-delivered"]
    await hub.close()


async def test_close_drops_subscriptions_and_stops_publishing() -> None:
    hub = InMemoryHub("t6")
    await hub.start()
    received: list[bytes] = []
    await hub.subscribe("doc", lambda p: _collect(received, p))

    await hub.close()
    await hub.publish("doc", b"after-close")
    assert received == []


async def test_publish_tolerates_no_subscribers() -> None:
    hub = InMemoryHub("t7")
    await hub.start()
    await hub.publish("nobody-listening", b"payload")
    await hub.close()


async def test_channel_naming_is_document_scoped() -> None:
    assert channel_for("abc") == "syncpad:doc:abc"
    assert channel_for("abc") != channel_for("abd")


async def test_make_hub_selects_by_configuration() -> None:
    """The adapter seam: Redis only when a URL is configured."""
    in_process = make_hub(Settings(env="test", redis_url=None))
    assert isinstance(in_process, InMemoryHub)

    with_redis = make_hub(Settings(env="test", redis_url="redis://localhost:6379/0"))
    assert isinstance(with_redis, RedisHub)


async def test_redis_hub_refuses_to_publish_before_start() -> None:
    hub = RedisHub("redis://localhost:6379/0")
    with pytest.raises(RuntimeError, match="not started"):
        await hub.publish("doc", b"payload")


# --------------------------------------------------------------- integration


@pytest.mark.integration
async def test_redis_hub_crosses_replicas_for_real() -> None:
    """The same cross-replica guarantee, over a live Redis."""
    import os

    url = os.environ.get("SYNCPAD_REDIS_URL", "redis://localhost:6379/0")
    replica_a, replica_b = RedisHub(url), RedisHub(url)
    await replica_a.start()
    await replica_b.start()

    received: asyncio.Queue[bytes] = asyncio.Queue()
    await replica_b.subscribe("doc-int", received.put)
    await asyncio.sleep(0.2)  # let the subscription register

    await replica_a.publish("doc-int", b"over-the-wire")
    payload = await asyncio.wait_for(received.get(), timeout=5)
    assert payload == b"over-the-wire"

    await replica_a.close()
    await replica_b.close()


async def _collect(sink: list[bytes], payload: bytes) -> None:
    sink.append(payload)

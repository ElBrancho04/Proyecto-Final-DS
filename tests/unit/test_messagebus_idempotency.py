import asyncio

from common.errors import Reject
from sdk.messaging import MessageBus


class InMemoryBroker:
    """Broker mínimo en memoria con la misma interfaz que BrokerClient (sin Ray)."""

    def __init__(self):
        self.queues = {}
        self.acks = []
        self.nacks = []
        self._n = 0

    async def publish(self, queue, envelope):
        self.queues.setdefault(queue, []).append(envelope)
        return envelope["message_id"]

    async def consume(self, queue):
        items = self.queues.get(queue, [])
        if not items:
            return None
        self._n += 1
        return {"delivery_id": f"d{self._n}", "queue": queue, "attempts": 0, "envelope": items.pop(0)}

    async def ack(self, queue, delivery_id):
        self.acks.append(delivery_id)

    async def nack(self, queue, delivery_id):
        self.nacks.append(delivery_id)


def _request(bus, idem_key, payload=None):
    return bus._build_envelope(
        "request",
        payload or {"a": 1},
        idempotency_key=idem_key,
        from_agent_id="client",
        to_capability="math.add",
        reply_to="reply.client",
        timeout_ms=1000,
    )


def test_handler_exception_runs_once_and_returns_internal_error():
    async def scenario():
        broker = InMemoryBroker()
        bus = MessageBus("agent", broker)
        calls = []

        async def handler(cap, payload, env):
            calls.append(1)
            raise ValueError("boom")

        await bus._process_item("cap.math.add", "math.add", {"delivery_id": "d0", "envelope": _request(bus, "k1")}, handler, 10)
        reply = broker.queues["reply.client"][0]
        return calls, reply, broker

    calls, reply, broker = asyncio.run(scenario())
    assert len(calls) == 1
    assert reply["type"] == "reject"
    assert reply["payload"]["code"] == "INTERNAL_ERROR" and reply["payload"]["retryable"] is False
    assert broker.acks == ["d0"] and broker.nacks == []  # sin reentrega


def test_concurrent_same_idempotency_key_executes_once():
    async def scenario():
        broker = InMemoryBroker()
        bus = MessageBus("agent", broker)
        calls = []

        async def handler(cap, payload, env):
            calls.append(1)
            await asyncio.sleep(0.1)
            return {"result": 3}

        first = {"delivery_id": "d1", "envelope": _request(bus, "same")}
        second = {"delivery_id": "d2", "envelope": _request(bus, "same")}
        await asyncio.gather(
            bus._process_item("cap.math.add", "math.add", first, handler, 10),
            bus._process_item("cap.math.add", "math.add", second, handler, 10),
        )
        return calls, broker

    calls, broker = asyncio.run(scenario())
    assert len(calls) == 1
    replies = broker.queues["reply.client"]
    assert len(replies) == 2 and all(r["payload"] == {"result": 3} for r in replies)
    assert sorted(broker.acks) == ["d1", "d2"]


def test_retryable_reject_is_not_cached_but_fatal_one_is():
    async def scenario():
        broker = InMemoryBroker()
        bus = MessageBus("agent", broker)
        calls = []

        async def retryable(cap, payload, env):
            calls.append("r")
            raise Reject("OVERLOADED", "busy")  # retryable por defecto

        async def fatal(cap, payload, env):
            calls.append("f")
            raise Reject("VALIDATION_ERROR", "bad")  # no reintentable por defecto

        for i in range(2):
            await bus._process_item("cap.x", "x", {"delivery_id": f"r{i}", "envelope": _request(bus, "kr")}, retryable, 10)
        for i in range(2):
            await bus._process_item("cap.x", "x", {"delivery_id": f"f{i}", "envelope": _request(bus, "kf")}, fatal, 10)
        return calls

    assert asyncio.run(scenario()) == ["r", "r", "f"]


def test_send_request_max_retries_zero_does_not_retry():
    async def scenario():
        broker = InMemoryBroker()
        bus = MessageBus("client", broker)
        try:
            await bus.send_request("math.add", {"a": 1}, timeout_s=0.2, max_retries=0)
        except Exception as exc:
            return exc, len(broker.queues["cap.math.add"])

    exc, published = asyncio.run(scenario())
    assert exc.code == "REQUEST_FAILED"
    assert published == 1


def test_unclaimed_replies_expire():
    async def scenario():
        bus = MessageBus("client", InMemoryBroker())
        bus.PENDING_REPLY_TTL_S = 0.0
        bus._pending_replies["orphan"] = (0.0, {"type": "response"})
        bus._cleanup_pending_replies()
        return bus._pending_replies

    assert asyncio.run(scenario()) == {}

import asyncio
import pytest
from broker.store import BrokerStore
from common.errors import Reject
from common.messages import RejectCode
from sdk.messaging import MessageBus


class InMemoryAsyncBroker:
    """Adaptador asíncrono sobre BrokerStore.

    Con db_path=':memory:' el store mantiene internamente una conexión persistente,
    por lo que todas las operaciones ven la misma base de datos.
    """

    def __init__(self, visibility_timeout: int = 30, max_attempts: int = 3) -> None:
        self._store = BrokerStore(db_path=":memory:")
        self._visibility_timeout = visibility_timeout
        self._max_attempts = max_attempts

    async def publish(self, queue: str, envelope: dict) -> str:
        return self._store.publish(queue, envelope)

    async def consume(self, queue: str):
        return self._store.consume(queue, visibility_timeout=self._visibility_timeout)

    async def ack(self, queue: str, delivery_id: str) -> None:
        self._store.ack(queue, delivery_id)

    async def nack(self, queue: str, delivery_id: str) -> None:
        self._store.nack(queue, delivery_id)


@pytest.mark.asyncio
async def test_message_bus_echo_request_response():
    broker = InMemoryAsyncBroker()
    client_bus = MessageBus("agent-client", broker)
    server_bus = MessageBus("agent-echo", broker)
    server_bus.POLL_INTERVAL_S = 0.02

    async def echo_handler(capability_id: str, payload: dict, envelope: dict) -> dict:
        assert capability_id == "echo.text"
        return {"echo": payload.get("text"), "received_by": "agent-echo"}

    server_task = asyncio.create_task(server_bus.serve("echo.text", echo_handler, max_concurrency=5))
    try:
        reply = await client_bus.send_request("echo.text", {"text": "hola mundo"}, timeout_s=3.0)
        assert reply["type"] == "response"
        assert reply["from_agent_id"] == "agent-echo"
        assert reply["payload"] == {"echo": "hola mundo", "received_by": "agent-echo"}
        assert reply["correlation_id"] is not None
    finally:
        server_task.cancel()
        await asyncio.gather(server_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_message_bus_retries_with_same_idempotency_key():
    broker = InMemoryAsyncBroker()
    client_bus = MessageBus("agent-caller", broker)
    client_bus.RETRY_BACKOFFS_S = (0.01, 0.02, 0.03)
    server_bus = MessageBus("agent-flaky", broker)
    server_bus.POLL_INTERVAL_S = 0.01

    seen_keys = []
    calls = 0

    async def flaky_handler(capability_id: str, payload: dict, envelope: dict) -> dict:
        nonlocal calls
        calls += 1
        seen_keys.append(envelope["idempotency_key"])
        if calls < 2:
            raise Reject(code=RejectCode.OVERLOADED.value, details="Busy once", retryable=True)
        return {"ok": True, "attempt": calls}

    server_task = asyncio.create_task(server_bus.serve("work.flaky", flaky_handler, max_concurrency=5))
    try:
        fixed_idem = "idem-fixed-123"
        # El reject (aun siendo retryable) se retorna tal cual al llamador con la misma
        # idempotency_key: el handler NO se re-ejecuta; quien decide reintentar es la capa superior.
        reply = await client_bus.send_request(
            "work.flaky",
            {"value": 42},
            timeout_s=2.0,
            idempotency_key=fixed_idem,
        )
        assert reply["type"] == "reject"
        assert reply["idempotency_key"] == fixed_idem
        assert reply["payload"]["code"] == RejectCode.OVERLOADED.value
        assert calls == 1
        assert seen_keys == [fixed_idem]
    finally:
        server_task.cancel()
        await asyncio.gather(server_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_message_bus_idempotency_cache_prevents_duplicate_execution():
    broker = InMemoryAsyncBroker()
    server_bus = MessageBus("agent-idem", broker)

    executions = 0

    async def counting_handler(capability_id: str, payload: dict, envelope: dict) -> dict:
        nonlocal executions
        executions += 1
        return {"count": executions, "input": payload}

    req1 = server_bus._build_envelope(
        "request",
        {"x": 1},
        idempotency_key="same-key-abc",
        from_agent_id="caller-1",
        to_capability="math.inc",
        reply_to="reply.caller-1",
    )
    req2 = server_bus._build_envelope(
        "request",
        {"x": 1},
        idempotency_key="same-key-abc",
        from_agent_id="caller-1",
        to_capability="math.inc",
        reply_to="reply.caller-1",
    )

    await server_bus._process_item(
        "cap.math.inc",
        "math.inc",
        {"delivery_id": "d-1", "queue": "cap.math.inc", "attempts": 0, "envelope": req1},
        counting_handler,
        max_concurrency=5,
    )
    await server_bus._process_item(
        "cap.math.inc",
        "math.inc",
        {"delivery_id": "d-2", "queue": "cap.math.inc", "attempts": 0, "envelope": req2},
        counting_handler,
        max_concurrency=5,
    )

    assert executions == 1

    reply = await broker.consume("reply.caller-1")
    assert reply is not None
    await broker.ack("reply.caller-1", reply["delivery_id"])
    assert reply["envelope"]["payload"] == {"count": 1, "input": {"x": 1}}


@pytest.mark.asyncio
async def test_message_bus_rejects_overloaded_when_max_concurrency_reached():
    broker = InMemoryAsyncBroker()
    server_bus = MessageBus("agent-busy", broker)
    server_bus._in_flight = 1

    async def never_called(capability_id: str, payload: dict, envelope: dict) -> dict:
        raise AssertionError("No debería ejecutarse el handler estando OVERLOADED")

    req = server_bus._build_envelope(
        "request",
        {"ping": True},
        from_agent_id="caller-overload",
        to_capability="ping.cap",
        reply_to="reply.caller-overload",
    )

    await server_bus._process_item(
        "cap.ping.cap",
        "ping.cap",
        {"delivery_id": "d-ov", "queue": "cap.ping.cap", "attempts": 0, "envelope": req},
        never_called,
        max_concurrency=1,
    )

    consumed_reply = await broker.consume("reply.caller-overload")
    assert consumed_reply is not None
    env = consumed_reply["envelope"]
    assert env["type"] == "reject"
    assert env["correlation_id"] == req["correlation_id"]
    assert env["payload"]["code"] == RejectCode.OVERLOADED.value
    assert env["payload"]["retryable"] is True
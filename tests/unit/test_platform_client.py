import asyncio
from pathlib import Path

import pytest

import sdk.platform_client as platform_client_module
from common.errors import PlatformError, Reject
from common.messages import RejectCode
from sdk.messaging import MessageBus
from sdk.platform_client import PlatformClient


class FakeOrchestrator:
    """Sustituto de OrchestratorClient con estado en memoria."""

    def __init__(self):
        self.tasks = {}
        self.created = []
        self.cancelled = []

    async def create_task(self, steps=None, workflow=None, input_data=None, flow_id=None) -> str:
        task_id = f"task-{len(self.created) + 1}"
        self.created.append(
            {"steps": steps, "workflow": workflow, "input_data": input_data, "flow_id": flow_id}
        )
        self.tasks[task_id] = {
            "task_id": task_id,
            "flow_id": flow_id,
            "state": "CREATED",
            "input": dict(input_data or {}),
            "output": None,
            "steps": [],
            "task_events": [],
        }
        return task_id

    async def get_task(self, task_id: str):
        if task_id not in self.tasks:
            raise PlatformError("NOT_FOUND", f"Tarea '{task_id}' no encontrada")
        return dict(self.tasks[task_id])

    async def list_tasks(self, state=None):
        tasks = [dict(t) for t in self.tasks.values()]
        if state is not None:
            tasks = [t for t in tasks if t["state"] == state]
        return tasks

    async def cancel_task(self, task_id: str) -> None:
        if task_id not in self.tasks:
            raise PlatformError("NOT_FOUND", f"Tarea '{task_id}' no encontrada")
        self.cancelled.append(task_id)
        self.tasks[task_id]["state"] = "CANCELLED"

    def set_state(self, task_id: str, state: str) -> None:
        self.tasks[task_id]["state"] = state


class FakeBroker:
    """Broker en memoria con la interfaz de BrokerClient."""

    def __init__(self):
        self.queues = {}
        self._n = 0

    async def publish(self, queue, envelope):
        self.queues.setdefault(queue, []).append(envelope)
        return envelope.get("message_id", f"msg-{self._n}")

    async def consume(self, queue):
        items = self.queues.get(queue)
        if not items:
            return None
        self._n += 1
        return {"delivery_id": f"d{self._n}", "queue": queue, "attempts": 0, "envelope": items.pop(0)}

    async def ack(self, queue, delivery_id):
        pass

    async def nack(self, queue, delivery_id):
        pass


def make_client(orchestrator=None, broker=None, **kwargs):
    kwargs.setdefault("poll_interval_s", 0.01)
    return PlatformClient(orchestrator=orchestrator, broker=broker, **kwargs)


def test_client_id_unique_per_instance():
    c1 = PlatformClient(orchestrator=FakeOrchestrator(), broker=FakeBroker())
    c2 = PlatformClient(orchestrator=FakeOrchestrator(), broker=FakeBroker())
    assert c1.client_id != c2.client_id
    assert c1.client_id.startswith("client-")


def test_platform_client_has_no_direct_runtime_imports():
    """El cliente solo debe depender de los clientes de servicios y del SDK."""
    source = Path(platform_client_module.__file__).read_text(encoding="utf-8")
    assert "import ray" not in source
    assert "ray_support" not in source


@pytest.mark.asyncio
async def test_create_task_delegates_and_returns_task_id():
    orch = FakeOrchestrator()
    client = make_client(orchestrator=orch)
    task_id = await client.create_task(
        workflow="workflow.echo_math",
        input_data={"a": 1, "b": 2},
        flow_id="flow-1",
    )
    assert task_id in orch.tasks
    assert orch.created[0]["workflow"] == "workflow.echo_math"
    assert orch.created[0]["input_data"] == {"a": 1, "b": 2}
    assert orch.created[0]["flow_id"] == "flow-1"


@pytest.mark.asyncio
async def test_get_task_not_found_propagates():
    client = make_client(orchestrator=FakeOrchestrator())
    with pytest.raises(PlatformError) as exc:
        await client.get_task("no-existe")
    assert exc.value.code == "NOT_FOUND"


@pytest.mark.asyncio
async def test_list_tasks_filters_by_state():
    orch = FakeOrchestrator()
    client = make_client(orchestrator=orch)
    t1 = await client.create_task(steps=["echo.text"])
    t2 = await client.create_task(steps=["echo.text"])
    orch.set_state(t2, "COMPLETED")

    assert len(await client.list_tasks()) == 2
    created = await client.list_tasks(state="CREATED")
    assert [t["task_id"] for t in created] == [t1]
    completed = await client.list_tasks(state="COMPLETED")
    assert [t["task_id"] for t in completed] == [t2]


@pytest.mark.asyncio
async def test_cancel_task_marks_cancelled():
    orch = FakeOrchestrator()
    client = make_client(orchestrator=orch)
    task_id = await client.create_task(steps=["echo.text"])

    await client.cancel_task(task_id)

    assert task_id in orch.cancelled
    task = await client.get_task(task_id)
    assert task["state"] == "CANCELLED"


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_state", ["COMPLETED", "FAILED", "CANCELLED"])
async def test_wait_task_returns_on_each_terminal_state(terminal_state):
    orch = FakeOrchestrator()
    client = make_client(orchestrator=orch)
    task_id = await client.create_task(steps=["echo.text"])
    orch.set_state(task_id, terminal_state)

    task = await client.wait_task(task_id, timeout_s=1.0)

    assert task["state"] == terminal_state
    assert task["task_id"] == task_id


@pytest.mark.asyncio
async def test_wait_task_times_out_with_request_timeout():
    orch = FakeOrchestrator()
    client = make_client(orchestrator=orch)
    task_id = await client.create_task(steps=["echo.text"])
    orch.set_state(task_id, "RUNNING")  # nunca llega a estado terminal

    with pytest.raises(PlatformError) as exc:
        await client.wait_task(task_id, timeout_s=0.1)

    assert exc.value.code == "REQUEST_TIMEOUT"


@pytest.mark.asyncio
async def test_wait_task_propagates_not_found():
    client = make_client(orchestrator=FakeOrchestrator())
    with pytest.raises(PlatformError) as exc:
        await client.wait_task("no-existe", timeout_s=1.0)
    assert exc.value.code == "NOT_FOUND"


@pytest.mark.asyncio
async def test_wait_task_returns_full_task_with_history():
    orch = FakeOrchestrator()
    client = make_client(orchestrator=orch)
    task_id = await client.create_task(steps=["echo.text"])
    orch.tasks[task_id]["steps"] = [{"step_id": "s1", "state": "DONE"}]
    orch.tasks[task_id]["task_events"] = [{"type": "TASK_CREATED"}]
    orch.set_state(task_id, "COMPLETED")

    task = await client.wait_task(task_id, timeout_s=1.0)

    assert task["steps"][0]["step_id"] == "s1"
    assert task["task_events"][0]["type"] == "TASK_CREATED"


@pytest.mark.asyncio
async def test_request_returns_response_envelope():
    broker = FakeBroker()
    client = make_client(broker=broker)
    server_bus = MessageBus("agent-echo", broker)
    server_bus.POLL_INTERVAL_S = 0.01

    async def echo_handler(capability_id, payload, envelope):
        return {"echo": payload.get("text")}

    server_task = asyncio.create_task(server_bus.serve("echo.text", echo_handler, max_concurrency=5))
    try:
        reply = await client.request("echo.text", {"text": "hola"}, timeout_s=2.0)
        assert reply["type"] == "response"
        assert reply["payload"] == {"echo": "hola"}
        assert reply["correlation_id"]
    finally:
        server_task.cancel()
        await asyncio.gather(server_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_request_returns_last_reject_after_retries_exhausted():
    """Un reject retryable se reintenta hasta agotar y se devuelve el último recibido, sin lanzar."""
    broker = FakeBroker()
    client = make_client(broker=broker)
    client.bus.RETRY_BACKOFFS_S = (0.01, 0.01, 0.01)
    server_bus = MessageBus("agent-busy", broker)
    server_bus.POLL_INTERVAL_S = 0.01

    calls = 0

    async def overloaded_handler(capability_id, payload, envelope):
        nonlocal calls
        calls += 1
        raise Reject(code=RejectCode.OVERLOADED.value, details="busy", retryable=True)

    server_task = asyncio.create_task(server_bus.serve("work.busy", overloaded_handler, max_concurrency=5))
    try:
        reply = await client.request("work.busy", {"x": 1}, timeout_s=2.0)
        assert reply["type"] == "reject"
        assert reply["payload"]["code"] == RejectCode.OVERLOADED.value
        assert reply["payload"]["retryable"] is True
        # 1 intento original + 3 reintentos del bus
        assert calls == 4
    finally:
        server_task.cancel()
        await asyncio.gather(server_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_request_non_retryable_reject_returns_immediately():
    broker = FakeBroker()
    client = make_client(broker=broker)
    server_bus = MessageBus("agent-invalid", broker)
    server_bus.POLL_INTERVAL_S = 0.01

    calls = 0

    async def invalid_handler(capability_id, payload, envelope):
        nonlocal calls
        calls += 1
        raise Reject(code=RejectCode.VALIDATION_ERROR.value, details="bad", retryable=False)

    server_task = asyncio.create_task(server_bus.serve("work.invalid", invalid_handler, max_concurrency=5))
    try:
        reply = await client.request("work.invalid", {"x": 1}, timeout_s=2.0)
        assert reply["type"] == "reject"
        assert reply["payload"]["code"] == "VALIDATION_ERROR"
        assert reply["payload"]["retryable"] is False
        assert calls == 1  # sin reenvío
    finally:
        server_task.cancel()
        await asyncio.gather(server_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_request_idempotency_key_reaches_capability():
    broker = FakeBroker()
    client = make_client(broker=broker)
    server_bus = MessageBus("agent-idem", broker)
    server_bus.POLL_INTERVAL_S = 0.01

    seen_keys = []

    async def handler(capability_id, payload, envelope):
        seen_keys.append(envelope["idempotency_key"])
        return {"ok": True}

    server_task = asyncio.create_task(server_bus.serve("work.idem", handler, max_concurrency=5))
    try:
        reply = await client.request("work.idem", {"x": 1}, timeout_s=2.0, idempotency_key="key-123")
        assert reply["type"] == "response"
        assert seen_keys == ["key-123"]
    finally:
        server_task.cancel()
        await asyncio.gather(server_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_request_failed_when_no_attempt_replies():
    broker = FakeBroker()
    client = make_client(broker=broker)
    client.bus.RETRY_BACKOFFS_S = (0.01, 0.01, 0.01)

    with pytest.raises(PlatformError) as exc:
        await client.request("nobody.home", {"x": 1}, timeout_s=0.05)

    assert exc.value.code == "REQUEST_FAILED"
    # 1 intento original + 3 reintentos del bus = 4 mensajes publicados
    assert len(broker.queues["cap.nobody.home"]) == 4
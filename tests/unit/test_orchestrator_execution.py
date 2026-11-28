import asyncio
from datetime import timedelta

import pytest

from common.errors import Reject
from common.messages import RejectCode
from common.models import CapabilitySpec
from orchestrator.service import OrchestratorService
from orchestrator.state_machine import TERMINAL_TASK_STATES
from sdk.messaging import MessageBus
from tests.fakes import FakeRegistry, InMemoryAsyncBroker


async def register(registry: FakeRegistry, agent_id: str, capability_id: str) -> None:
    await registry.publish(
        token="test-token",
        agent_id=agent_id,
        capability=CapabilitySpec(capability_id=capability_id, name=capability_id),
        instance_id=f"{agent_id}-inst",
    )


def make_service(tmp_path, registry, broker, step_timeout_s=2.0):
    service = OrchestratorService(
        db_path=str(tmp_path / "orchestrator.db"),
        registry=registry,
        broker=broker,
        step_timeout_s=step_timeout_s,
    )
    service.default_executor.bus.POLL_INTERVAL_S = 0.01
    return service


def start_server(broker, agent_id, capability_id, handler):
    bus = MessageBus(agent_id, broker)
    bus.POLL_INTERVAL_S = 0.01
    return asyncio.create_task(bus.serve(capability_id, handler, max_concurrency=5))


async def stop_servers(servers):
    for task in servers:
        task.cancel()
    await asyncio.gather(*servers, return_exceptions=True)


async def run_until_terminal(service, task_id, max_passes=60):
    # Cada pasada de run_pending_tasks ejecuta un intento por paso; se repite hasta estado terminal.
    for _ in range(max_passes):
        task = await service.get_task(task_id)
        if task["state"] in TERMINAL_TASK_STATES:
            return task
        await service.run_pending_tasks()
    return await service.get_task(task_id)


@pytest.mark.asyncio
async def test_three_step_task_completes_via_broker(tmp_path):
    registry = FakeRegistry()
    broker = InMemoryAsyncBroker()
    for agent_id, capability_id in [
        ("agent-echo", "echo.text"),
        ("agent-add", "math.add"),
        ("agent-mul", "math.mul"),
    ]:
        await register(registry, agent_id, capability_id)

    async def echo(capability_id, payload, envelope):
        return {"text": payload["text"]}

    async def add(capability_id, payload, envelope):
        return {"result": payload["a"] + payload["b"]}

    async def mul(capability_id, payload, envelope):
        return {"result": payload["a"] * payload["b"]}

    servers = [
        start_server(broker, "agent-echo", "echo.text", echo),
        start_server(broker, "agent-add", "math.add", add),
        start_server(broker, "agent-mul", "math.mul", mul),
    ]
    try:
        service = make_service(tmp_path, registry, broker)
        task_id = await service.create_task(
            workflow="workflow.echo_math",
            input_data={"text": "hola", "a": 2, "b": 5, "k": 3},
        )

        task = await run_until_terminal(service, task_id)
    finally:
        await stop_servers(servers)

    assert task["state"] == "COMPLETED"
    assert task["output"] == {"result": 21}
    assert all(s["state"] == "DONE" for s in task["steps"])

    types = [e["type"] for e in task["task_events"]]
    assert types[0] == "TASK_CREATED"
    assert types.count("STEP_STARTED") == 3
    assert types.count("STEP_DONE") == 3
    assert types.count("STEP_RETRY") == 0
    assert types[-1] == "TASK_COMPLETED"

    step_done = [e for e in task["task_events"] if e["type"] == "STEP_DONE"]
    assert step_done[0]["payload"]["candidate_agent_id"] == "agent-echo"


@pytest.mark.asyncio
async def test_retryable_reject_retries_with_same_idempotency_key(tmp_path):
    """El reintento del paso reenvía con la misma idempotency_key hasta agotar los 3 intentos."""
    registry = FakeRegistry()
    broker = InMemoryAsyncBroker()
    await register(registry, "agent-flaky", "work.flaky")

    calls = 0
    seen_keys = []

    async def flaky(capability_id, payload, envelope):
        nonlocal calls
        calls += 1
        seen_keys.append(envelope["idempotency_key"])
        if calls < 3:
            raise Reject(code=RejectCode.OVERLOADED.value, details="busy", retryable=True)
        return {"ok": True}

    server = start_server(broker, "agent-flaky", "work.flaky", flaky)
    try:
        service = make_service(tmp_path, registry, broker)
        service.STEP_RETRY_BACKOFFS_S = (0.01, 0.01, 0.01)
        task_id = await service.create_task(steps=["work.flaky"], input_data={})

        task = await run_until_terminal(service, task_id)
    finally:
        await stop_servers([server])

    assert task["state"] == "COMPLETED"
    assert task["output"] == {"ok": True}
    assert task["steps"][0]["attempt"] == 3

    types = [e["type"] for e in task["task_events"]]
    assert types.count("STEP_RETRY") == 2
    assert types.count("STEP_DONE") == 1

    assert calls == 3
    assert len(seen_keys) == 3
    assert seen_keys[0] == seen_keys[1] == seen_keys[2] == task["steps"][0]["idempotency_key"]


@pytest.mark.asyncio
async def test_non_retryable_reject_fails_without_retries(tmp_path):
    registry = FakeRegistry()
    broker = InMemoryAsyncBroker()
    await register(registry, "agent-invalid", "work.invalid")

    calls = 0

    async def invalid(capability_id, payload, envelope):
        nonlocal calls
        calls += 1
        raise Reject(code=RejectCode.VALIDATION_ERROR.value, details="bad input")

    server = start_server(broker, "agent-invalid", "work.invalid", invalid)
    try:
        service = make_service(tmp_path, registry, broker)
        task_id = await service.create_task(steps=["work.invalid"], input_data={})

        task = await run_until_terminal(service, task_id)
    finally:
        await stop_servers([server])

    assert task["state"] == "FAILED"
    assert task["steps"][0]["attempt"] == 1
    assert calls == 1

    types = [e["type"] for e in task["task_events"]]
    assert types.count("STEP_RETRY") == 0
    assert types[-1] == "TASK_FAILED"


@pytest.mark.asyncio
async def test_no_live_instance_fails_without_publishing(tmp_path):
    """Sin instancias vivas el paso reintenta por UNAVAILABLE sin publicar nada en cap.*."""
    registry = FakeRegistry()
    broker = InMemoryAsyncBroker()
    service = make_service(tmp_path, registry, broker)
    service.STEP_RETRY_BACKOFFS_S = (0.01, 0.01, 0.01)

    task_id = await service.create_task(steps=["math.add"], input_data={"a": 1, "b": 2})
    task = await run_until_terminal(service, task_id)

    assert task["state"] == "FAILED"
    assert task["steps"][0]["attempt"] == 3

    retries = [e for e in task["task_events"] if e["type"] == "STEP_RETRY"]
    assert len(retries) == 2
    assert all("UNAVAILABLE" in e["payload"]["error"] for e in retries)

    published = broker.store.list_messages(limit=100)
    assert all(not m["queue"].startswith("cap.") for m in published)


@pytest.mark.asyncio
async def test_unreachable_agent_fails_after_timeouts(tmp_path):
    """Con instancia registrada pero sin consumidor, cada intento agota su timeout y el paso falla."""
    registry = FakeRegistry()
    broker = InMemoryAsyncBroker()
    await register(registry, "agent-ghost", "work.ghost")

    service = make_service(tmp_path, registry, broker, step_timeout_s=0.2)
    service.STEP_RETRY_BACKOFFS_S = (0.01, 0.01, 0.01)

    task_id = await service.create_task(steps=["work.ghost"], input_data={"x": 1})
    task = await run_until_terminal(service, task_id)

    assert task["state"] == "FAILED"
    assert task["steps"][0]["attempt"] == 3
    assert "REQUEST_FAILED" in task["steps"][0]["error"]["error"]

    published = broker.store.list_messages(limit=10, queue="cap.work.ghost")
    assert len(published) == 3


@pytest.mark.asyncio
async def test_recovery_resumes_task_after_restart(tmp_path):
    registry = FakeRegistry()
    broker = InMemoryAsyncBroker()
    await register(registry, "agent-echo", "echo.text")

    async def echo(capability_id, payload, envelope):
        return {"text": payload["text"]}

    server = start_server(broker, "agent-echo", "echo.text", echo)
    try:
        service = make_service(tmp_path, registry, broker)
        task_id = await service.create_task(steps=["echo.text"], input_data={"text": "hola"})
        step_id = (await service.get_task(task_id))["steps"][0]["step_id"]

        # Simula una caída con el paso a mitad de ejecución
        service.store.update_step(step_id, state="RUNNING", attempt=1)

        resumable = await service.recover_pending_tasks()
        recovered = await service.get_task(task_id)
        assert resumable == [task_id]
        assert recovered["steps"][0]["state"] == "PENDING"
        assert recovered["task_events"][-1]["type"] == "STEP_REASSIGNED"

        # El paso reanudado espera el backoff de su último intento; se adelanta el reloj
        # para verificar la reanudación de forma determinista.
        real_now = service._utcnow
        service._utcnow = lambda: real_now() + timedelta(seconds=60)

        task = await run_until_terminal(service, task_id)
    finally:
        await stop_servers([server])

    assert task["state"] == "COMPLETED"
    assert task["steps"][0]["attempt"] == 2
    assert task["output"] == {"text": "hola"}


@pytest.mark.asyncio
async def test_backoff_defers_retry_until_elapsed(tmp_path):
    registry = FakeRegistry()
    broker = InMemoryAsyncBroker()
    await register(registry, "agent-flaky", "work.once")

    calls = 0

    async def flaky_once(capability_id, payload, envelope):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise Reject(code=RejectCode.OVERLOADED.value, details="busy", retryable=True)
        return {"ok": True}

    server = start_server(broker, "agent-flaky", "work.once", flaky_once)
    try:
        service = make_service(tmp_path, registry, broker)
        service.STEP_RETRY_BACKOFFS_S = (5.0, 5.0, 5.0)
        task_id = await service.create_task(steps=["work.once"], input_data={})

        # Intento 1: falla y el paso queda a la espera del backoff
        assert await service.run_pending_tasks() == {"executed": 1, "deferred": 0}
        # Con backoff de 5 s, la pasada inmediata no puede ejecutar el reintento
        assert await service.run_pending_tasks() == {"executed": 0, "deferred": 1}

        # Se adelanta el reloj: el paso se ejecuta como segundo intento y la tarea completa
        real_now = service._utcnow
        service._utcnow = lambda: real_now() + timedelta(seconds=10)
        assert await service.run_pending_tasks() == {"executed": 1, "deferred": 0}

        task = await service.get_task(task_id)
    finally:
        await stop_servers([server])

    assert task["state"] == "COMPLETED"
    assert task["steps"][0]["attempt"] == 2
    assert calls == 2
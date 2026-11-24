import pytest

from common.models import CapabilitySpec
from orchestrator.service import OrchestratorService
from orchestrator.state_machine import StepState, TaskState
from tests.fakes import FakeBroker, FakeRegistry


@pytest.mark.asyncio
async def test_create_get_and_list_tasks_with_workflow(tmp_path):
    db_file = str(tmp_path / "orch_core.db")
    registry = FakeRegistry()
    broker = FakeBroker()
    service = OrchestratorService(db_path=db_file, registry=registry, broker=broker)

    task_id = await service.create_task(
        workflow="workflow.echo_math",
        input_data={"text": "hola", "a": 3, "b": 4, "factor": 2},
        flow_id="flow-001",
    )

    task = await service.get_task(task_id)
    assert task["task_id"] == task_id
    assert task["flow_id"] == "flow-001"
    assert task["state"] == TaskState.CREATED.value
    assert len(task["steps"]) == 3
    assert [s["capability_id"] for s in task["steps"]] == [
        "echo.text",
        "math.add",
        "math.mul",
    ]
    assert all(s["state"] == StepState.PENDING.value for s in task["steps"])

    # Verificar historial append-only inicial
    event_types = [e["type"] for e in task["task_events"]]
    assert event_types == ["TASK_CREATED"]

    all_tasks = await service.list_tasks()
    assert len(all_tasks) == 1
    assert all_tasks[0]["task_id"] == task_id

    created_only = await service.list_tasks(state=TaskState.CREATED.value)
    assert len(created_only) == 1
    assert await service.list_tasks(state=TaskState.COMPLETED.value) == []


@pytest.mark.asyncio
async def test_three_step_task_advances_to_completed_with_fake_registry(tmp_path):
    db_file = str(tmp_path / "orch_3steps.db")
    registry = FakeRegistry()

    for cap_id, agent_id in [
        ("echo.text", "agent-echo-1"),
        ("math.add", "agent-math-1"),
        ("math.mul", "agent-mul-1"),
    ]:
        await registry.publish(
            token="test-token",
            agent_id=agent_id,
            capability=CapabilitySpec(capability_id=cap_id, name=cap_id),
            instance_id=f"{agent_id}-inst",
        )

    service = OrchestratorService(db_path=db_file, registry=registry)
    task_id = await service.create_task(
        workflow="workflow.echo_math",
        # El workflow encadena: math.mul ← {"a": "$steps.1.result", "b": "$input.k"}
        # por eso el input debe exponer 'k' (7 * 3 = 21)
        input_data={"text": "ds", "a": 2, "b": 5, "k": 3},
    )

    def fake_executor(capability_id: str, step_input: dict, step: dict) -> dict:
        if capability_id == "echo.text":
            return {"echoed": step_input.get("text", "")}
        if capability_id == "math.add":
            return {"result": step_input.get("a", 0) + step_input.get("b", 0)}
        if capability_id == "math.mul":
            return {"result": step_input.get("a", 0) * step_input.get("b", 0)}
        raise ValueError(f"Capacidad desconocida: {capability_id}")

    final_task = await service.run_task_with_fakes(task_id, step_executor=fake_executor)

    assert final_task["state"] == TaskState.COMPLETED.value
    assert final_task["output"] == {"result": 21}
    assert all(s["state"] == StepState.DONE.value for s in final_task["steps"])

    # Nueva instancia apuntando al mismo SQLite confirma que todo el estado vive en SQLite
    reloaded = OrchestratorService(db_path=db_file, registry=registry)
    persisted_task = await reloaded.get_task(task_id)
    assert persisted_task["state"] == TaskState.COMPLETED.value
    assert persisted_task["output"] == {"result": 21}

    event_types = [e["type"] for e in persisted_task["task_events"]]
    assert "TASK_CREATED" in event_types
    assert event_types.count("STEP_STARTED") == 3
    assert event_types.count("STEP_DONE") == 3
    assert event_types[-1] == "TASK_COMPLETED"


@pytest.mark.asyncio
async def test_cancel_task_best_effort_and_retries_before_failure(tmp_path):
    db_file = str(tmp_path / "orch_cancel_retry.db")
    service = OrchestratorService(db_path=db_file)

    # 1. Cancelación de tarea activa
    task_to_cancel = await service.create_task(
        steps=[{"capability_id": "echo.text", "input": {"msg": "cancel me"}}],
    )
    await service.cancel_task(task_to_cancel)
    cancelled = await service.get_task(task_to_cancel)
    assert cancelled["state"] == TaskState.CANCELLED.value
    assert any(e["type"] == "TASK_CANCELLED" for e in cancelled["task_events"])

    # 2. Fallos hasta agotar 3 intentos -> TASK_FAILED
    flaky_task_id = await service.create_task(
        steps=[{"capability_id": "math.add", "input": {"a": 1, "b": 2}}],
    )

    def always_fail(capability_id: str, step_input: dict, step: dict):
        raise RuntimeError("fallo simulado")

    failed_task = await service.run_task_with_fakes(flaky_task_id, step_executor=always_fail)
    assert failed_task["state"] == TaskState.FAILED.value
    assert failed_task["steps"][0]["attempt"] == 3
    assert failed_task["steps"][0]["state"] == StepState.FAILED.value
    event_types = [e["type"] for e in failed_task["task_events"]]
    assert event_types.count("STEP_RETRY") == 2
    assert event_types[-1] == "TASK_FAILED"
import asyncio

import pytest

from common.errors import PlatformError, Reject
from common.messages import DEFAULT_RETRYABLE, ENVELOPE_FIELDS, create_envelope, validate_envelope
from orchestrator.service import OrchestratorService
from orchestrator.workflows import resolve_input_map


def test_resolve_input_map_chains_outputs():
    task_input = {"text": "hola", "a": 2, "b": 3, "k": 4}
    outputs = [{"text": "hola"}, {"result": 5}, None]
    assert resolve_input_map({"a": "$steps.1.result", "b": "$input.k"}, task_input, outputs) == {"a": 5, "b": 4}
    assert resolve_input_map({"x": 7}, task_input, outputs) == {"x": 7}


@pytest.mark.parametrize("ref", ["$input.missing", "$steps.2.result", "$steps.9.result", "$steps.-1.result", "$foo.bar"])
def test_resolve_input_map_invalid_reference(ref):
    with pytest.raises(PlatformError) as exc:
        resolve_input_map({"a": ref}, {"a": 1}, [{"result": 1}, None, None])
    assert exc.value.code == "VALIDATION_ERROR"


def _executor(capability_id, step_input, step):
    if capability_id == "echo.text":
        return {"text": step_input["text"]}
    if capability_id == "math.add":
        return {"result": step_input["a"] + step_input["b"]}
    return {"result": step_input["a"] * step_input["b"]}


def test_workflow_echo_math_completes_with_input_map(tmp_path):
    async def scenario():
        svc = OrchestratorService(db_path=str(tmp_path / "o.db"))
        task_id = await svc.create_task(workflow="workflow.echo_math", input_data={"text": "hola", "a": 2, "b": 3, "k": 4})
        return await svc.run_task_with_fakes(task_id, step_executor=_executor)

    task = asyncio.run(scenario())
    assert task["state"] == "COMPLETED"
    assert task["output"] == {"result": 20}
    assert task["steps"][2]["input"] == {"a": 5, "b": 4}


def test_missing_reference_fails_task_without_retries(tmp_path):
    async def scenario():
        svc = OrchestratorService(db_path=str(tmp_path / "o.db"))
        task_id = await svc.create_task(workflow="workflow.echo_math", input_data={"text": "hola"})
        return await svc.run_task_with_fakes(task_id, step_executor=_executor)

    task = asyncio.run(scenario())
    assert task["state"] == "FAILED"
    assert task["steps"][1]["attempt"] == 1  # VALIDATION_ERROR no se reintenta


def test_without_executor_step_is_never_marked_done(tmp_path):
    async def scenario():
        svc = OrchestratorService(db_path=str(tmp_path / "o.db"))
        task_id = await svc.create_task(steps=["echo.text"], input_data={"text": "hola"})
        return await svc.run_task_with_fakes(task_id)

    task = asyncio.run(scenario())
    assert task["state"] == "FAILED"
    assert all(s["state"] != "DONE" for s in task["steps"])


def test_recover_pending_tasks_resets_running_steps(tmp_path):
    async def scenario():
        svc = OrchestratorService(db_path=str(tmp_path / "o.db"))
        task_id = await svc.create_task(steps=["echo.text", "echo.text"], input_data={"text": "hola"})
        step = svc.store.get_task(task_id)["steps"][0]
        svc.store.update_step(step["step_id"], state="RUNNING", attempt=1)
        resumable = await svc.recover_pending_tasks()
        task = svc.store.get_task(task_id)
        return task_id, resumable, task

    task_id, resumable, task = asyncio.run(scenario())
    assert resumable == [task_id]
    assert task["steps"][0]["state"] == "PENDING"
    assert task["task_events"][-1]["type"] == "STEP_REASSIGNED"


def test_envelope_has_only_contract_fields_and_validates():
    env = create_envelope(envelope_type="request", payload={"a": 1}, from_agent_id="x", to_capability="math.add")
    assert set(env) == set(ENVELOPE_FIELDS)
    validate_envelope(env)
    env["sender_id"] = "x"
    with pytest.raises(PlatformError):
        validate_envelope(env)


def test_reject_retryable_defaults_follow_table():
    for code, expected in DEFAULT_RETRYABLE.items():
        assert Reject(code, "x").retryable is expected
    assert Reject("OVERLOADED", "x", retryable=False).retryable is False

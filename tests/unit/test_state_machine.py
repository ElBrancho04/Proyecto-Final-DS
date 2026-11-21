import pytest

from common.errors import PlatformError
from orchestrator.state_machine import (
    StepState,
    TaskState,
    can_transition_step,
    can_transition_task,
    transition_step,
    transition_task,
)


def test_task_valid_happy_path_transitions():
    assert can_transition_task(TaskState.CREATED, TaskState.RUNNING)
    assert can_transition_task(TaskState.RUNNING, TaskState.WAITING)
    assert can_transition_task(TaskState.WAITING, TaskState.RUNNING)
    assert can_transition_task(TaskState.RUNNING, TaskState.COMPLETED)
    assert can_transition_task(TaskState.WAITING, TaskState.COMPLETED)


def test_task_valid_failure_and_cancellation_transitions():
    assert can_transition_task(TaskState.CREATED, TaskState.CANCELLED)
    assert can_transition_task(TaskState.RUNNING, TaskState.FAILED)
    assert can_transition_task(TaskState.RUNNING, TaskState.CANCELLED)
    assert can_transition_task(TaskState.WAITING, TaskState.FAILED)
    assert can_transition_task(TaskState.WAITING, TaskState.CANCELLED)


def test_task_terminal_states_cannot_transition():
    terminal_states = [TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELLED]
    all_states = list(TaskState)

    for term in terminal_states:
        for target in all_states:
            assert not can_transition_task(term, target), (
                f"Estado terminal {term} no debería transicionar a {target}"
            )


def test_task_invalid_transitions_raise_platform_error():
    task = {"task_id": "t-1", "state": TaskState.CREATED.value}
    assert not can_transition_task(TaskState.CREATED, TaskState.COMPLETED)

    with pytest.raises(PlatformError) as exc_info:
        transition_task(task, TaskState.COMPLETED)

    assert exc_info.value.code == "INVALID_TRANSITION"
    assert task["state"] == TaskState.CREATED.value


def test_transition_task_updates_state_and_metadata():
    task = {"task_id": "t-2", "state": TaskState.RUNNING.value}
    old_state, meta = transition_task(
        task,
        TaskState.COMPLETED.value,
        {"output": {"result": 42}},
    )

    assert old_state == TaskState.RUNNING.value
    assert task["state"] == TaskState.COMPLETED.value
    assert task["output"] == {"result": 42}
    assert meta["from_state"] == TaskState.RUNNING.value
    assert meta["to_state"] == TaskState.COMPLETED.value
    assert meta["output"] == {"result": 42}


def test_step_transitions_and_terminal_states():
    assert can_transition_step(StepState.PENDING, StepState.RUNNING)
    assert can_transition_step(StepState.RUNNING, StepState.DONE)
    assert can_transition_step(StepState.RUNNING, StepState.FAILED)
    assert can_transition_step(StepState.RUNNING, StepState.PENDING)

    for term in [StepState.DONE, StepState.FAILED]:
        for target in list(StepState):
            assert not can_transition_step(term, target)

    step = {"step_id": "s-1", "state": StepState.DONE.value}
    with pytest.raises(PlatformError) as exc_info:
        transition_step(step, StepState.RUNNING)

    assert exc_info.value.code == "INVALID_TRANSITION"
    assert step["state"] == StepState.DONE.value


def test_transition_invalid_entity_raises_platform_error():
    with pytest.raises(PlatformError) as exc_info:
        transition_task({}, TaskState.RUNNING)

    assert exc_info.value.code == "INVALID_STATE_ENTITY"
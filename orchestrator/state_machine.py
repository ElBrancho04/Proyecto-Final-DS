from enum import Enum
from typing import Any, Dict, Optional, Set, Tuple
from common.errors import PlatformError


class TaskState(str, Enum):
    CREATED = "CREATED"
    RUNNING = "RUNNING"
    WAITING = "WAITING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class StepState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"


TERMINAL_TASK_STATES: Set[str] = {
    TaskState.COMPLETED.value,
    TaskState.FAILED.value,
    TaskState.CANCELLED.value,
}

TERMINAL_STEP_STATES: Set[str] = {
    StepState.DONE.value,
    StepState.FAILED.value,
}

ALLOWED_TASK_TRANSITIONS: Dict[str, Set[str]] = {
    TaskState.CREATED.value: {
        TaskState.RUNNING.value,
        TaskState.CANCELLED.value,
        TaskState.FAILED.value,
    },
    TaskState.RUNNING.value: {
        TaskState.WAITING.value,
        TaskState.COMPLETED.value,
        TaskState.FAILED.value,
        TaskState.CANCELLED.value,
    },
    TaskState.WAITING.value: {
        TaskState.RUNNING.value,
        TaskState.COMPLETED.value,
        TaskState.FAILED.value,
        TaskState.CANCELLED.value,
    },
    TaskState.COMPLETED.value: set(),
    TaskState.FAILED.value: set(),
    TaskState.CANCELLED.value: set(),
}

ALLOWED_STEP_TRANSITIONS: Dict[str, Set[str]] = {
    StepState.PENDING.value: {
        StepState.RUNNING.value,
        StepState.FAILED.value,
    },
    StepState.RUNNING.value: {
        StepState.DONE.value,
        StepState.FAILED.value,
        StepState.PENDING.value,
    },
    StepState.DONE.value: set(),
    StepState.FAILED.value: set(),
}


def _normalize_state(state: Any) -> str:
    return state.value if isinstance(state, Enum) else str(state)


def can_transition_task(old_state: Any, new_state: Any) -> bool:
    """Indica si una transición de estado de tarea es válida."""
    old_val = _normalize_state(old_state)
    new_val = _normalize_state(new_state)
    if old_val in TERMINAL_TASK_STATES:
        return False
    return new_val in ALLOWED_TASK_TRANSITIONS.get(old_val, set())


def can_transition_step(old_state: Any, new_state: Any) -> bool:
    """Indica si una transición de estado de paso es válida."""
    old_val = _normalize_state(old_state)
    new_val = _normalize_state(new_state)
    if old_val in TERMINAL_STEP_STATES:
        return False
    return new_val in ALLOWED_STEP_TRANSITIONS.get(old_val, set())


def transition(
    entity: Dict[str, Any],
    new_state: Any,
    payload: Optional[Dict[str, Any]] = None,
    *,
    entity_kind: str = "task",
) -> Tuple[str, Dict[str, Any]]:
    """
    Valida y aplica una transición de estado sobre un diccionario de tarea o paso.
    Retorna (estado_anterior, metainfo_actualizada).
    """
    if not isinstance(entity, dict) or "state" not in entity:
        raise PlatformError("INVALID_STATE_ENTITY", "La entidad debe ser un dict con clave 'state'")

    old_val = _normalize_state(entity["state"])
    new_val = _normalize_state(new_state)
    extra = dict(payload) if payload else {}

    if entity_kind == "step":
        allowed = can_transition_step(old_val, new_val)
    else:
        allowed = can_transition_task(old_val, new_val)

    if not allowed:
        raise PlatformError(
            "INVALID_TRANSITION",
            f"Transición no permitida en {entity_kind}: {old_val} -> {new_val}",
        )

    entity["state"] = new_val
    if "output" in extra and extra["output"] is not None:
        entity["output"] = extra["output"]
    if "error" in extra and extra["error"] is not None:
        entity["error"] = extra["error"]

    return old_val, {
        "from_state": old_val,
        "to_state": new_val,
        **extra,
    }


def transition_task(
    task: Dict[str, Any],
    new_state: Any,
    payload: Optional[Dict[str, Any]] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Aplica una transición de estado a una tarea."""
    return transition(task, new_state, payload, entity_kind="task")


def transition_step(
    step: Dict[str, Any],
    new_state: Any,
    payload: Optional[Dict[str, Any]] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Aplica una transición de estado a un paso."""
    return transition(step, new_state, payload, entity_kind="step")
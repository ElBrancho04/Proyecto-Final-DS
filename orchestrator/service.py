import asyncio
import os
from typing import Any, Callable, Dict, List, Optional
import ray

from common.config import RAY_NAMESPACE
from common.errors import PlatformError
from common.logging import log_event
from common.models import DiscoverQuery
from orchestrator.state_machine import (
    StepState,
    TERMINAL_TASK_STATES,
    TaskState,
    transition_step,
    transition_task,
)
from orchestrator.store import OrchestratorStore
from orchestrator.workflows import resolve_input_map, resolve_steps


class OrchestratorService:
    """
    Núcleo del Orquestador: gestiona tareas, pasos e historial append-only en SQLite.
    Todo el estado vive en SQLite; el servicio es stateless respecto al estado de las tareas.
    """

    MAX_STEP_ATTEMPTS = 3

    def __init__(
        self,
        db_path: str = "data/orchestrator.db",
        registry: Any = None,
        broker: Any = None,
    ) -> None:
        self.store = OrchestratorStore(db_path=db_path)
        self.registry = registry
        self.broker = broker

    async def health(self) -> Dict[str, Any]:
        tasks = self.store.list_tasks()
        return {
            "service": "orchestrator",
            "status": "healthy",
            "tasks_count": len(tasks),
            "db_path": self.store.db_path,
        }

    async def create_task(
        self,
        steps: Optional[List[Dict[str, Any]]] = None,
        workflow: Optional[str] = None,
        input_data: Optional[Dict[str, Any]] = None,
        flow_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> str:
        resolved_steps = resolve_steps(steps=steps, workflow=workflow, input_data=input_data)
        task = self.store.create_task_with_steps(
            steps_spec=resolved_steps,
            input_data=input_data,
            flow_id=flow_id,
            task_id=task_id,
        )
        created_task_id = task["task_id"]
        log_event(
            "TASK_CREATED",
            "orchestrator",
            correlation_id=created_task_id,
            task_id=created_task_id,
            flow_id=flow_id,
            steps_count=len(resolved_steps),
        )
        return created_task_id

    async def get_task(self, task_id: str) -> Dict[str, Any]:
        return self.store.get_task(task_id)

    async def list_tasks(self, state: Optional[str] = None) -> List[Dict[str, Any]]:
        return self.store.list_tasks(state=state)

    async def cancel_task(self, task_id: str) -> None:
        """Cancelación best-effort de una tarea no terminal."""
        task = self.store.get_task(task_id)
        current_state = task["state"]
        if current_state in TERMINAL_TASK_STATES:
            return

        if current_state == TaskState.CREATED.value:
            transition_task(task, TaskState.RUNNING.value)
            self.store.update_task_state(task_id, TaskState.RUNNING.value)
            task = self.store.get_task(task_id)

        transition_task(task, TaskState.CANCELLED.value)
        self.store.update_task_state(
            task_id,
            TaskState.CANCELLED.value,
            event_type="TASK_CANCELLED",
            event_payload={"task_id": task_id, "previous_state": current_state},
        )
        log_event(
            "TASK_CANCELLED",
            "orchestrator",
            correlation_id=task_id,
            task_id=task_id,
        )

    async def recover_pending_tasks(self) -> List[str]:
        """
        Recuperación al arrancar: los pasos que quedaron RUNNING vuelven a PENDING (misma idempotency_key)
        y se devuelven los ids de las tareas no terminales que deben reanudarse.
        """
        resumable: List[str] = []
        for task in self.store.list_tasks():
            if task["state"] in TERMINAL_TASK_STATES:
                continue
            for step in task["steps"]:
                if step["state"] == StepState.RUNNING.value:
                    transition_step(step, StepState.PENDING.value)
                    self.store.update_step(
                        step["step_id"],
                        state=StepState.PENDING.value,
                        event_task_id=task["task_id"],
                        event_type="STEP_REASSIGNED",
                        event_payload={
                            "step_id": step["step_id"],
                            "capability_id": step["capability_id"],
                            "attempt": step["attempt"],
                            "idempotency_key": step["idempotency_key"],
                            "reason": "recovery",
                        },
                    )
            resumable.append(task["task_id"])
        return resumable

    async def _discover_capability(self, capability_id: str) -> List[Any]:
        """Consulta el registro (o FakeRegistry) para descubrir instancias de una capacidad."""
        if self.registry is None:
            return []
        query = DiscoverQuery(capability=capability_id)
        if hasattr(self.registry, "discover"):
            res = self.registry.discover(query)
            if asyncio.iscoroutine(res):
                return await res
            return res
        return []

    async def advance_step(
        self,
        task_id: str,
        step_executor: Optional[Callable[[str, Dict[str, Any], Dict[str, Any]], Any]] = None,
    ) -> Dict[str, Any]:
        """
        Avanza el siguiente paso pendiente de una tarea, persistiendo estado, salidas y
        eventos append-only en SQLite.
        """
        task = self.store.get_task(task_id)
        if task["state"] in TERMINAL_TASK_STATES:
            return task

        if task["state"] in (TaskState.CREATED.value, TaskState.WAITING.value):
            transition_task(task, TaskState.RUNNING.value)
            self.store.update_task_state(task_id, TaskState.RUNNING.value)
            task = self.store.get_task(task_id)

        next_step = next(
            (s for s in task["steps"] if s["state"] == StepState.PENDING.value),
            None,
        )
        if next_step is None:
            all_done = all(s["state"] == StepState.DONE.value for s in task["steps"])
            if all_done:
                last_output = task["steps"][-1]["output"] if task["steps"] else task["output"]
                transition_task(task, TaskState.COMPLETED.value, {"output": last_output})
                return self.store.update_task_state(
                    task_id,
                    TaskState.COMPLETED.value,
                    output=last_output,
                    event_type="TASK_COMPLETED",
                    event_payload={"task_id": task_id, "output": last_output},
                )
            return task

        step_id = next_step["step_id"]
        capability_id = next_step["capability_id"]
        attempt = next_step["attempt"] + 1

        # Si el paso está en estado RUNNING desde un intento previo, devolver a PENDING antes de re-ejecutar
        if next_step["state"] == StepState.RUNNING.value:
            transition_step(next_step, StepState.PENDING.value)
            self.store.update_step(step_id, state=StepState.PENDING.value)
            next_step = self.store.get_task(task_id)["steps"][next_step["step_index"]]

        matches = await self._discover_capability(capability_id)
        selected_match = matches[0] if matches else None

        transition_step(next_step, StepState.RUNNING.value)
        event_type = "STEP_STARTED" if attempt == 1 else "STEP_REASSIGNED"
        self.store.update_step(
            step_id,
            state=StepState.RUNNING.value,
            attempt=attempt,
            event_task_id=task_id,
            event_type=event_type,
            event_payload={
                "step_id": step_id,
                "capability_id": capability_id,
                "attempt": attempt,
                "idempotency_key": next_step["idempotency_key"],
                "target_agent_id": getattr(selected_match, "agent_id", None),
            },
        )

        # Preparar entrada del paso: si tiene input propio no vacío lo usa; si no, encadena output previo o input de tarea
        task_now = self.store.get_task(task_id)
        current_step = next(s for s in task_now["steps"] if s["step_id"] == step_id)
        step_input = dict(current_step["input"]) if current_step["input"] else {}
        if not step_input:
            prev_steps = [s for s in task_now["steps"] if s["step_index"] < current_step["step_index"]]
            if prev_steps and prev_steps[-1]["output"] is not None:
                step_input = dict(prev_steps[-1]["output"])
            else:
                step_input = dict(task_now["input"])
            self.store.update_step(step_id, input_data=step_input)

        try:
            if current_step.get("input_map"):
                step_input = resolve_input_map(
                    current_step["input_map"],
                    task_now["input"],
                    [s["output"] for s in task_now["steps"]],
                )
                self.store.update_step(step_id, input_data=step_input)

            if step_executor is not None:
                raw_result = step_executor(capability_id, step_input, current_step)
                step_output = await raw_result if asyncio.iscoroutine(raw_result) else raw_result
            else:
                # Nunca se marca un paso como DONE sin respuesta real de un agente
                raise PlatformError("EXECUTOR_NOT_CONFIGURED", "No hay ejecutor de pasos configurado")

            transition_step(current_step, StepState.DONE.value, {"output": step_output})
            self.store.update_step(
                step_id,
                state=StepState.DONE.value,
                output=step_output,
                event_task_id=task_id,
                event_type="STEP_DONE",
                event_payload={
                    "step_id": step_id,
                    "capability_id": capability_id,
                    "attempt": attempt,
                    "output": step_output,
                },
            )

            updated_task = self.store.get_task(task_id)
            if all(s["state"] == StepState.DONE.value for s in updated_task["steps"]):
                transition_task(updated_task, TaskState.COMPLETED.value, {"output": step_output})
                updated_task = self.store.update_task_state(
                    task_id,
                    TaskState.COMPLETED.value,
                    output=step_output,
                    event_type="TASK_COMPLETED",
                    event_payload={"task_id": task_id, "output": step_output},
                )
            else:
                if updated_task["state"] != TaskState.WAITING.value:
                    transition_task(updated_task, TaskState.WAITING.value)
                    updated_task = self.store.update_task_state(
                        task_id,
                        TaskState.WAITING.value,
                    )
            return updated_task

        except Exception as exc:
            err_info = {"error": str(exc), "attempt": attempt}
            non_retryable = isinstance(exc, PlatformError) and exc.code == "VALIDATION_ERROR"
            if attempt < self.MAX_STEP_ATTEMPTS and not non_retryable:
                transition_step(current_step, StepState.PENDING.value)
                self.store.update_step(
                    step_id,
                    state=StepState.PENDING.value,
                    error=err_info,
                    event_task_id=task_id,
                    event_type="STEP_RETRY",
                    event_payload={
                        "step_id": step_id,
                        "capability_id": capability_id,
                        "attempt": attempt,
                        "idempotency_key": current_step["idempotency_key"],
                        "error": str(exc),
                    },
                )
                return self.store.get_task(task_id)

            transition_step(current_step, StepState.FAILED.value, {"error": err_info})
            self.store.update_step(
                step_id,
                state=StepState.FAILED.value,
                error=err_info,
            )
            failed_task = self.store.get_task(task_id)
            transition_task(failed_task, TaskState.FAILED.value, {"error": err_info})
            return self.store.update_task_state(
                task_id,
                TaskState.FAILED.value,
                error=err_info,
                event_type="TASK_FAILED",
                event_payload={"task_id": task_id, "step_id": step_id, "error": str(exc)},
            )

    async def run_task_with_fakes(
        self,
        task_id: str,
        step_executor: Optional[Callable[[str, Dict[str, Any], Dict[str, Any]], Any]] = None,
        max_iterations: int = 25,
    ) -> Dict[str, Any]:
        """Ejecuta secuencialmente los pasos de una tarea contra fakes hasta estado terminal."""
        for _ in range(max_iterations):
            task = self.store.get_task(task_id)
            if task["state"] in TERMINAL_TASK_STATES:
                return task
            has_active_or_pending = any(
                s["state"] in (StepState.PENDING.value, StepState.RUNNING.value)
                for s in task["steps"]
            )
            if not has_active_or_pending:
                return task
            await self.advance_step(task_id, step_executor=step_executor)
        return self.store.get_task(task_id)


@ray.remote(namespace=RAY_NAMESPACE)
class OrchestratorActor:
    """Actor de Ray para el Orquestador."""

    def __init__(self, db_path: str = "data/orchestrator.db", registry: Any = None, broker: Any = None) -> None:
        self._service = OrchestratorService(db_path=db_path, registry=registry, broker=broker)

    async def start(self) -> List[str]:
        return await self._service.recover_pending_tasks()

    async def health(self) -> Dict[str, Any]:
        return {
            **await self._service.health(),
            "pid": os.getpid(),
            "node_id": ray.get_runtime_context().get_node_id(),
        }

    async def create_task(
        self,
        steps: Optional[List[Dict[str, Any]]] = None,
        workflow: Optional[str] = None,
        input_data: Optional[Dict[str, Any]] = None,
        flow_id: Optional[str] = None,
    ) -> str:
        return await self._service.create_task(
            steps=steps,
            workflow=workflow,
            input_data=input_data,
            flow_id=flow_id,
        )

    async def get_task(self, task_id: str) -> Dict[str, Any]:
        return await self._service.get_task(task_id)

    async def list_tasks(self, state: Optional[str] = None) -> List[Dict[str, Any]]:
        return await self._service.list_tasks(state=state)

    async def cancel_task(self, task_id: str) -> None:
        return await self._service.cancel_task(task_id)

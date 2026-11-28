import asyncio
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
import ray

from common.config import RAY_NAMESPACE
from common.errors import PlatformError, Reject
from common.logging import log_event
from common.models import DiscoverQuery
from orchestrator.executor import BusStepExecutor
from orchestrator.state_machine import (
    StepState,
    TERMINAL_TASK_STATES,
    TaskState,
    transition_step,
    transition_task,
)
from orchestrator.store import OrchestratorStore
from orchestrator.workflows import resolve_input_map, resolve_steps
from sdk.messaging import MessageBus


class OrchestratorService:
    """
    Núcleo del Orquestador: gestiona tareas, pasos e historial append-only en SQLite
    y ejecuta los pasos contra agentes reales a través del MessageBus.

    Todo el estado vive en SQLite. advance_step ejecuta un único intento de paso;
    los reintentos con backoff corren en run_pending_tasks, y la reasignación de
    pasos interrumpidos se hace en recover_pending_tasks al arrancar el actor.
    """

    MAX_STEP_ATTEMPTS = 3
    STEP_RETRY_BACKOFFS_S: Tuple[float, ...] = (1.0, 2.0, 4.0)

    def __init__(
        self,
        db_path: str = "data/orchestrator.db",
        registry: Any = None,
        broker: Any = None,
        step_timeout_s: float = 10.0,
    ) -> None:
        self.store = OrchestratorStore(db_path=db_path)
        self.registry = registry
        self.broker = broker
        self.default_executor: Optional[BusStepExecutor]
        if broker is not None:
            bus = MessageBus("orchestrator", broker)
            self.default_executor = BusStepExecutor(bus, timeout_s=step_timeout_s)
        else:
            self.default_executor = None

    def _utcnow(self) -> datetime:
        """Reloj del servicio; los tests lo sustituyen para controlar el backoff."""
        return datetime.now(timezone.utc)

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
        Recuperación al arrancar: los pasos que quedaron RUNNING vuelven a PENDING
        (misma idempotency_key) y se devuelven los ids de las tareas no terminales
        que deben reanudarse.
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
        """Consulta el registro y devuelve solo las instancias con lease vigente."""
        if self.registry is None:
            return []
        query = DiscoverQuery(capability=capability_id)
        if hasattr(self.registry, "discover"):
            res = self.registry.discover(query)
            matches = await res if asyncio.iscoroutine(res) else res
            now = time.time()
            return [m for m in matches if getattr(m, "lease_expires_at", 0) > now]
        return []

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        """Un reject decide por su marca; VALIDATION_ERROR nunca se reintenta; el resto de errores sí."""
        if isinstance(exc, Reject):
            return exc.retryable
        if isinstance(exc, PlatformError):
            return exc.code != "VALIDATION_ERROR"
        return True

    def _step_backoff_elapsed(self, step: Dict[str, Any]) -> bool:
        """Indica si un paso pendiente con intentos previos ya cumplió el backoff de su último intento."""
        attempt = step.get("attempt", 0)
        if attempt <= 0:
            return True
        backoff = self.STEP_RETRY_BACKOFFS_S[min(attempt - 1, len(self.STEP_RETRY_BACKOFFS_S) - 1)]
        if backoff <= 0:
            return True
        try:
            updated_at = datetime.fromisoformat(step["updated_at"])
        except (KeyError, TypeError, ValueError):
            return True
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        return (self._utcnow() - updated_at).total_seconds() >= backoff

    async def run_pending_tasks(self) -> Dict[str, int]:
        """
        Ejecuta una pasada sobre las tareas no terminales.

        Por cada tarea con un paso pendiente lo ejecuta con el ejecutor real; un paso
        con intentos previos solo se ejecuta cuando transcurrió el backoff de su último
        intento (calculado contra updated_at en SQLite) y, mientras espera, se difiere
        sin bloquear al resto de tareas.
        """
        summary = {"executed": 0, "deferred": 0}
        for task in self.store.list_tasks():
            if task["state"] in TERMINAL_TASK_STATES:
                continue
            next_step = next(
                (s for s in task["steps"] if s["state"] == StepState.PENDING.value),
                None,
            )
            if next_step is None:
                continue
            if any(
                s["state"] != StepState.DONE.value
                for s in task["steps"][: next_step["step_index"]]
            ):
                # Paso previo sin completar (residuo RUNNING tras una caída): lo reasigna recover_pending_tasks
                continue
            if not self._step_backoff_elapsed(next_step):
                summary["deferred"] += 1
                continue
            await self.advance_step(task_id=task["task_id"])
            summary["executed"] += 1
        return summary

    async def advance_step(
        self,
        task_id: str,
        step_executor: Optional[Callable[[str, Dict[str, Any], Dict[str, Any]], Any]] = None,
    ) -> Dict[str, Any]:
        """
        Ejecuta el siguiente paso pendiente de una tarea como un único intento,
        persistiendo estado, salida y eventos en SQLite.

        step_executor permite inyectar un ejecutor de prueba; por defecto se usa el
        ejecutor real sobre el MessageBus. Sin ejecutor disponible lanza
        EXECUTOR_NOT_CONFIGURED.
        """
        executor = step_executor if step_executor is not None else self.default_executor

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
            },
        )

        # Entrada del paso: si tiene input propio no vacío lo usa; si no, encadena output previo o input de tarea
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
            if executor is None:
                raise PlatformError("EXECUTOR_NOT_CONFIGURED", "No hay ejecutor de pasos configurado")

            candidates = await self._discover_capability(capability_id)
            # El requisito de instancia viva aplica al ejecutor real; un ejecutor
            # inyectado simula el agente directamente y no pasa por el registro.
            if step_executor is None and not candidates:
                raise PlatformError(
                    "UNAVAILABLE",
                    f"No hay instancias vivas para la capacidad '{capability_id}'",
                )

            if current_step.get("input_map"):
                step_input = resolve_input_map(
                    current_step["input_map"],
                    task_now["input"],
                    [s["output"] for s in task_now["steps"]],
                )
                self.store.update_step(step_id, input_data=step_input)

            # Nunca se marca un paso como DONE sin respuesta real de un agente
            raw_result = executor(capability_id, step_input, current_step)
            step_output = await raw_result if asyncio.iscoroutine(raw_result) else raw_result

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
                    "candidate_agent_id": getattr(candidates[0], "agent_id", None) if candidates else None,
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
            if attempt < self.MAX_STEP_ATTEMPTS and self._is_retryable_error(exc):
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
        """Ejecuta secuencialmente los pasos de una tarea contra un ejecutor inyectado hasta estado terminal."""
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

    TICK_S = 1.0

    def __init__(
        self,
        db_path: str = "data/orchestrator.db",
        registry: Any = None,
        broker: Any = None,
        step_timeout_s: float = 10.0,
    ) -> None:
        self._service = OrchestratorService(
            db_path=db_path,
            registry=registry,
            broker=broker,
            step_timeout_s=step_timeout_s,
        )
        self._loop_task: Optional[asyncio.Task] = None

    async def _task_loop(self) -> None:
        while True:
            try:
                await self._service.run_pending_tasks()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log_event("ORCHESTRATOR_LOOP_ERROR", "orchestrator", error=str(exc))
            await asyncio.sleep(self.TICK_S)

    async def start(self) -> List[str]:
        """Recupera los pasos interrumpidos y arranca el bucle de ejecución de tareas."""
        resumable = await self._service.recover_pending_tasks()
        if self._loop_task is None or self._loop_task.done():
            self._loop_task = asyncio.create_task(self._task_loop())
            log_event("ORCHESTRATOR_LOOP_STARTED", "orchestrator", resumed_tasks=len(resumable))
        return resumable

    async def stop(self) -> None:
        """Detiene el bucle de ejecución de tareas."""
        if self._loop_task is not None:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
            self._loop_task = None
        log_event("ORCHESTRATOR_LOOP_STOPPED", "orchestrator")

    async def health(self) -> Dict[str, Any]:
        return {
            **await self._service.health(),
            "pid": os.getpid(),
            "node_id": ray.get_runtime_context().get_node_id(),
            "loop_running": self._loop_task is not None and not self._loop_task.done(),
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
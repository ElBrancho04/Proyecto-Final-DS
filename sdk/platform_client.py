"""Cliente de la plataforma para aplicaciones externas y pruebas E2E.

Expone las operaciones del contrato sin exponer detalles del runtime: los
PlatformError de los servicios se propagan tal cual, y los rejects de capacidad
nunca se lanzan (request devuelve el Envelope recibido).

Las operaciones de identidad y registro se completarán al integrar los clientes
correspondientes.
"""

import asyncio
import time
from typing import Any, Dict, List, Optional

from broker.client import BrokerClient
from common.errors import PlatformError
from common.ids import new_agent_id
from common.models import DiscoverQuery
from orchestrator.client import OrchestratorClient
from orchestrator.state_machine import TERMINAL_TASK_STATES
from sdk.messaging import MessageBus


class PlatformClient:
    """Cliente de la plataforma.

    Cada instancia genera su propio id de cliente y su propio bus de mensajería,
    de modo que las respuestas de clientes concurrentes no se mezclan.
    """

    def __init__(
        self,
        orchestrator: Any = None,
        broker: Any = None,
        identity: Any = None,
        registry: Any = None,
        *,
        client_id: Optional[str] = None,
        default_timeout_s: float = 10.0,
        max_retries: int = 3,
        poll_interval_s: float = 0.5,
    ) -> None:
        self.client_id = client_id or f"client-{new_agent_id()}"
        self._orchestrator = orchestrator if orchestrator is not None else OrchestratorClient()
        self._broker = broker if broker is not None else BrokerClient()
        # TODO(A): usar IdentityClient/RegistryClient como defaults cuando existan
        self._identity = identity
        self._registry = registry
        self.bus = MessageBus(
            self.client_id,
            self._broker,
            default_timeout_s=default_timeout_s,
            max_retries=max_retries,
        )
        self.poll_interval_s = poll_interval_s

    async def create_task(
        self,
        steps: Optional[List[Dict[str, Any]]] = None,
        workflow: Optional[str] = None,
        input_data: Optional[Dict[str, Any]] = None,
        flow_id: Optional[str] = None,
    ) -> str:
        """Crea una tarea con pasos explícitos o a partir de una plantilla de workflow.

        Devuelve el task_id. La tarea queda persistida en estado CREATED antes de
        retornar y su ejecución continúa de forma independiente del cliente.
        Errores: INVALID_PAYLOAD, NOT_FOUND (plantilla desconocida), VALIDATION_ERROR.
        """
        return await self._orchestrator.create_task(
            steps=steps, workflow=workflow, input_data=input_data, flow_id=flow_id
        )

    async def get_task(self, task_id: str) -> Dict[str, Any]:
        """Devuelve la tarea completa, con sus pasos y su historial de eventos."""
        return await self._orchestrator.get_task(task_id)

    async def list_tasks(self, state: Optional[str] = None) -> List[Dict[str, Any]]:
        """Lista las tareas, opcionalmente filtradas por estado."""
        return await self._orchestrator.list_tasks(state=state)

    async def cancel_task(self, task_id: str) -> None:
        """Cancela una tarea no terminal (best-effort). No-op si ya está terminal."""
        await self._orchestrator.cancel_task(task_id)

    async def wait_task(self, task_id: str, timeout_s: float = 60.0) -> Dict[str, Any]:
        """Espera hasta que la tarea alcance un estado terminal y la devuelve completa.

        Sondea get_task cada poll_interval_s. Lanza REQUEST_TIMEOUT si expira el
        tiempo sin alcanzar COMPLETED, FAILED o CANCELLED, y NOT_FOUND si la tarea
        no existe.
        """
        deadline = time.monotonic() + timeout_s
        while True:
            task = await self._orchestrator.get_task(task_id)
            state = task.get("state")
            if state in TERMINAL_TASK_STATES:
                return task
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PlatformError(
                    "REQUEST_TIMEOUT",
                    f"La tarea '{task_id}' no alcanzó un estado terminal en {timeout_s}s "
                    f"(último estado observado: {state})",
                )
            await asyncio.sleep(min(self.poll_interval_s, remaining))

    async def request(
        self,
        capability: str,
        payload: Dict[str, Any],
        timeout_s: float = 10.0,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Envía un request a una capacidad y devuelve el Envelope de respuesta.

        Nunca lanza por un reject: un reject retryable se reintenta con backoff y la
        misma idempotency_key (por defecto 1 intento + 3 reintentos) y, agotados los
        reintentos, se devuelve el último reject recibido; uno no retryable se
        devuelve en el primer intento. REQUEST_FAILED solo cuando ningún intento
        obtuvo respuesta.

        Pasar la misma idempotency_key en varios requests permite reproducir
        entregas duplicadas desde las pruebas.
        """
        return await self.bus.send_request(
            to_capability=capability,
            payload=payload,
            timeout_s=timeout_s,
            idempotency_key=idempotency_key,
        )

    async def register_agent(self, name: Optional[str] = None) -> Dict[str, Any]:
        """Registra un agente y devuelve {agent_id, token, expires_at}."""
        raise NotImplementedError("register_agent: falta IdentityClient")

    async def discover(self, query: DiscoverQuery) -> List[Dict[str, Any]]:
        """Descubre agentes por capacidad; devuelve una lista de Match."""
        raise NotImplementedError("discover: falta RegistryClient")

    async def get_registry_state(self, node: str = "registry1") -> Dict[str, Any]:
        """Devuelve el estado interno de un nodo de registro (depuración)."""
        raise NotImplementedError("get_registry_state: falta RegistryClient")
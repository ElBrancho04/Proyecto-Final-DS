from typing import Any, Dict

from common.errors import Reject
from sdk.messaging import MessageBus


class BusStepExecutor:
    """Ejecuta un paso enviando un request por el MessageBus.

    Adaptador entre el orquestador y el bus: un response devuelve su payload y un
    reject se lanza como Reject para que la política de reintento del paso decida.
    Un timeout sin respuesta se propaga como REQUEST_FAILED. Cada llamada realiza
    un único intento (max_retries=0); el reintento con la misma idempotency_key es
    responsabilidad del orquestador.
    """

    def __init__(self, bus: MessageBus, timeout_s: float = 10.0) -> None:
        self.bus = bus
        self.timeout_s = timeout_s

    async def __call__(
        self,
        capability_id: str,
        payload: Dict[str, Any],
        step: Dict[str, Any],
    ) -> Dict[str, Any]:
        reply = await self.bus.send_request(
            to_capability=capability_id,
            payload=payload,
            timeout_s=self.timeout_s,
            idempotency_key=step.get("idempotency_key"),
            task_id=step.get("task_id"),
            step_id=step.get("step_id"),
            max_retries=0,
        )
        if reply.get("type") == "reject":
            reject_payload = reply.get("payload") or {}
            raise Reject(
                reject_payload.get("code", "INTERNAL_ERROR"),
                reject_payload.get("details", ""),
                retryable=reject_payload.get("retryable"),
            )
        return reply.get("payload") or {}
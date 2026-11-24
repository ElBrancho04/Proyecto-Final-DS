from enum import Enum
from typing import Dict, Any, Optional
from common.ids import new_id, now_iso


class RejectCode(str, Enum):
    OVERLOADED = "OVERLOADED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    NOT_COMPETENT = "NOT_COMPETENT"
    UNAVAILABLE = "UNAVAILABLE"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    INVALID_PAYLOAD = "INVALID_PAYLOAD"
    UNAUTHORIZED = "UNAUTHORIZED"
    NOT_FOUND = "NOT_FOUND"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"


# Reintento por defecto de cada código (roadmap, sección 5.3)
DEFAULT_RETRYABLE: Dict[str, bool] = {
    RejectCode.NOT_COMPETENT.value: False,
    RejectCode.OVERLOADED.value: True,
    RejectCode.UNAVAILABLE.value: True,
    RejectCode.VALIDATION_ERROR.value: False,
    RejectCode.INVALID_PAYLOAD.value: False,
    RejectCode.UNAUTHORIZED.value: False,
    RejectCode.NOT_FOUND.value: False,
    RejectCode.CANCELLED.value: False,
    RejectCode.TIMEOUT.value: True,
    RejectCode.INTERNAL_ERROR.value: False,
}

ENVELOPE_FIELDS = (
    "v", "message_id", "correlation_id", "idempotency_key", "type", "from_agent_id",
    "to_capability", "reply_to", "task_id", "step_id", "timeout_ms", "timestamp", "payload",
)
ENVELOPE_TYPES = ("request", "response", "reject", "event")


def create_envelope(
    sender_id: Optional[str] = None,
    recipient_id: Optional[str] = None,
    msg_type: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    correlation_id: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    *,
    v: int = 1,
    envelope_type: Optional[str] = None,
    from_agent_id: Optional[str] = None,
    to_capability: Optional[str] = None,
    reply_to: Optional[str] = None,
    task_id: Optional[str] = None,
    step_id: Optional[str] = None,
    timeout_ms: Optional[int] = 10000,
) -> Dict[str, Any]:
    # Soporte por si se llama posicionalmente: create_envelope("request", {"a": 2}, ...)
    if sender_id in ("request", "response", "reject", "event") and isinstance(recipient_id, dict):
        resolved_type = sender_id
        resolved_payload = recipient_id
        resolved_from = from_agent_id or "unknown"
        resolved_to_cap = to_capability if msg_type is None else msg_type
    else:
        resolved_type = envelope_type or msg_type or "request"
        resolved_payload = payload if payload is not None else {}
        resolved_from = from_agent_id or sender_id or "unknown"
        resolved_to_cap = to_capability if to_capability is not None else recipient_id

    message_id = new_id()
    resolved_corr = correlation_id or message_id
    resolved_idem = idempotency_key or new_id()

    return {
        "v": v,
        "message_id": message_id,
        "correlation_id": resolved_corr,
        "idempotency_key": resolved_idem,
        "type": resolved_type,
        "from_agent_id": resolved_from,
        "to_capability": resolved_to_cap,
        "reply_to": reply_to,
        "task_id": task_id,
        "step_id": step_id,
        "timeout_ms": timeout_ms if resolved_type == "request" else None,
        "timestamp": now_iso(),
        "payload": resolved_payload,
    }


def validate_envelope(envelope: Dict[str, Any]) -> None:
    """Valida que un mensaje cumpla el Envelope v1 (roadmap, sección 5.1); lanza PlatformError VALIDATION_ERROR."""
    from common.errors import PlatformError  # import diferido: errors.py importa este módulo

    if not isinstance(envelope, dict):
        raise PlatformError("VALIDATION_ERROR", "El envelope debe ser un diccionario")
    missing = [f for f in ENVELOPE_FIELDS if f not in envelope]
    extra = [f for f in envelope if f not in ENVELOPE_FIELDS]
    if missing or extra:
        raise PlatformError("VALIDATION_ERROR", f"Campos faltantes: {missing}; campos no permitidos: {extra}")
    if envelope["v"] != 1:
        raise PlatformError("VALIDATION_ERROR", f"Versión de envelope no soportada: {envelope['v']}")
    if envelope["type"] not in ENVELOPE_TYPES:
        raise PlatformError("VALIDATION_ERROR", f"Tipo de envelope inválido: {envelope['type']}")
    if envelope["type"] in ("request", "event") and not envelope["to_capability"]:
        raise PlatformError("VALIDATION_ERROR", "request/event requieren to_capability")
    if not isinstance(envelope["payload"], dict):
        raise PlatformError("VALIDATION_ERROR", "payload debe ser un objeto")
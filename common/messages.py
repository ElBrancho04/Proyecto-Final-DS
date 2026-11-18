from enum import Enum
from typing import Dict, Any, Optional
from common.ids import new_id, now_iso

class RejectCode(str, Enum):
    OVERLOADED = "OVERLOADED"
    INVALID_PAYLOAD = "INVALID_PAYLOAD"
    UNAUTHORIZED = "UNAUTHORIZED"
    NOT_FOUND = "NOT_FOUND"
    INTERNAL_ERROR = "INTERNAL_ERROR"

def create_envelope(
    sender_id: str,
    recipient_id: str,
    msg_type: str,
    payload: Dict[str, Any],
    correlation_id: Optional[str] = None,
    idempotency_key: Optional[str] = None
) -> Dict[str, Any]:
    return {
        "message_id": new_id(),
        "correlation_id": correlation_id or new_id(),
        "idempotency_key": idempotency_key or new_id(),
        "sender_id": sender_id,
        "recipient_id": recipient_id,
        "msg_type": msg_type,  # REQUEST, REPLY, REJECT, HEARTBEAT
        "timestamp": now_iso(),
        "payload": payload
    }
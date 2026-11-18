from datetime import datetime, timezone
import uuid
import uuid6

def new_agent_id() -> str:
    """Genera un UUIDv7 para la identidad del agente."""
    return str(uuid6.uuid7())

def new_id() -> str:
    """Genera un UUIDv4 estándar para IDs generales."""
    return str(uuid.uuid4())

def now_iso() -> str:
    """Retorna la fecha/hora actual en formato ISO 8601 UTC."""
    return datetime.now(timezone.utc).isoformat()
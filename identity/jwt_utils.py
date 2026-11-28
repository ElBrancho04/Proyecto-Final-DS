"""Emisión y verificación de JWT HS256 para la identidad de agentes.

Funciones puras, sin estado ni acceso a red: el registro las usa para validar
tokens localmente. Todo fallo de verificación se traduce a PlatformError
AUTH_ERROR, de modo que los llamadores no dependen de las excepciones de pyjwt.
"""

import time
from typing import List, Optional

import jwt

from common.errors import PlatformError
from common.models import Claims


def issue_jwt(
    agent_id: str,
    secret: str,
    ttl_s: float = 3600.0,
    scopes: Optional[List[str]] = None,
) -> str:
    """Emite un token HS256 con expiración ttl_s segundos desde ahora."""
    now = time.time()
    payload = {
        "agent_id": agent_id,
        "scopes": scopes if scopes is not None else ["agent"],
        "iat": now,
        "exp": now + ttl_s,
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def verify_jwt(token: str, secret: str) -> Claims:
    """Valida firma y expiración y devuelve los claims; lanza AUTH_ERROR ante cualquier fallo."""
    try:
        payload = jwt.decode(token, secret, algorithms=["HS256"])
        return Claims(
            agent_id=payload["agent_id"],
            scopes=list(payload.get("scopes", [])),
            exp=float(payload["exp"]),
        )
    except jwt.ExpiredSignatureError as exc:
        raise PlatformError("AUTH_ERROR", "Token expirado") from exc
    except jwt.PyJWTError as exc:
        raise PlatformError("AUTH_ERROR", f"Token inválido: {exc}") from exc
    except (KeyError, TypeError, ValueError) as exc:
        raise PlatformError("AUTH_ERROR", "Token con claims incompletos") from exc

import os
import time
from typing import Any, Dict, List, Optional

import ray

from common.config import JWT_SECRET, RAY_NAMESPACE
from common.errors import PlatformError
from common.ids import new_agent_id
from common.logging import log_event
from common.models import Claims
from identity.jwt_utils import issue_jwt, verify_jwt
from identity.store import STATUS_ACTIVE, IdentityStore


class IdentityService:
    """Identidad distribuida: emisión y renovación de JWT HS256, persistencia de agentes.

    verify_token aplica la verificación fuerte (firma, expiración y estado del agente en
    el store); jwt_utils.verify_jwt es la versión pura que usa el registro sin red.
    """

    JWT_TTL_S = 3600.0

    def __init__(self, db_path: str = "data/identity.db", secret: Optional[str] = None) -> None:
        self.store = IdentityStore(db_path=db_path)
        self.secret = secret if secret is not None else JWT_SECRET

    def _reject_auth(self, agent_id: Optional[str], operation: str, details: str) -> PlatformError:
        """Registra el fallo de autenticación y devuelve la excepción correspondiente."""
        log_event(
            "AUTH_ERROR",
            "identity",
            correlation_id=agent_id or "N/A",
            agent_id=agent_id,
            operation=operation,
            error=details,
        )
        return PlatformError("AUTH_ERROR", details)

    async def register_agent(self, name: Optional[str] = None) -> Dict[str, Any]:
        agent_id = new_agent_id()
        self.store.insert_agent(agent_id, name=name)
        token = issue_jwt(agent_id, self.secret, ttl_s=self.JWT_TTL_S)
        log_event("AGENT_REGISTERED", "identity", correlation_id=agent_id, agent_id=agent_id, name=name)
        return {"agent_id": agent_id, "token": token, "expires_at": time.time() + self.JWT_TTL_S}

    async def heartbeat(self, agent_id: str, token: str) -> Dict[str, Any]:
        """Renueva el JWT de un agente activo. Cualquier fallo de autenticación es AUTH_ERROR."""
        try:
            claims = verify_jwt(token, self.secret)
        except PlatformError as exc:
            raise self._reject_auth(agent_id, "heartbeat", exc.details) from exc
        if claims.agent_id != agent_id:
            raise self._reject_auth(agent_id, "heartbeat", "El token no corresponde al agente")
        agent = self.store.get_agent(agent_id)
        if agent is None:
            raise self._reject_auth(agent_id, "heartbeat", "Agente desconocido")
        if agent["status"] != STATUS_ACTIVE:
            raise self._reject_auth(agent_id, "heartbeat", "Agente desactivado")
        log_event("AGENT_HEARTBEAT", "identity", correlation_id=agent_id, agent_id=agent_id)
        return {
            "token": issue_jwt(agent_id, self.secret, ttl_s=self.JWT_TTL_S),
            "expires_at": time.time() + self.JWT_TTL_S,
        }

    async def verify_token(self, token: str) -> Claims:
        """Verificación fuerte: firma y expiración (jwt_utils) más estado del agente en el store."""
        try:
            claims = verify_jwt(token, self.secret)
        except PlatformError as exc:
            raise self._reject_auth(None, "verify_token", exc.details) from exc
        agent = self.store.get_agent(claims.agent_id)
        if agent is None or agent["status"] != STATUS_ACTIVE:
            raise self._reject_auth(claims.agent_id, "verify_token", "Agente desconocido o desactivado")
        return claims

    async def deactivate_agent(self, agent_id: str) -> None:
        if not self.store.set_status(agent_id, STATUS_ACTIVE.replace("active", "deactivated")):
            raise PlatformError("NOT_FOUND", f"Agente '{agent_id}' no encontrado")
        log_event("AGENT_DEACTIVATED", "identity", correlation_id=agent_id, agent_id=agent_id)

    async def get_agent(self, agent_id: str) -> Dict[str, Any]:
        agent = self.store.get_agent(agent_id)
        if agent is None:
            raise PlatformError("NOT_FOUND", f"Agente '{agent_id}' no encontrado")
        return agent

    async def list_agents(self) -> List[Dict[str, Any]]:
        return self.store.list_agents()

    async def health(self) -> Dict[str, Any]:
        return {
            "service": "identity",
            "status": "healthy",
            "agents_count": len(self.store.list_agents()),
            "db_path": self.store.db_path,
        }


@ray.remote(namespace=RAY_NAMESPACE)
class IdentityActor:
    """Actor de Ray para el servicio de identidad."""

    def __init__(self, db_path: str = "data/identity.db", secret: Optional[str] = None) -> None:
        self._service = IdentityService(db_path=db_path, secret=secret)

    async def register_agent(self, name: Optional[str] = None) -> Dict[str, Any]:
        return await self._service.register_agent(name)

    async def heartbeat(self, agent_id: str, token: str) -> Dict[str, Any]:
        return await self._service.heartbeat(agent_id, token)

    async def verify_token(self, token: str) -> Claims:
        return await self._service.verify_token(token)

    async def deactivate_agent(self, agent_id: str) -> None:
        return await self._service.deactivate_agent(agent_id)

    async def get_agent(self, agent_id: str) -> Dict[str, Any]:
        return await self._service.get_agent(agent_id)

    async def list_agents(self) -> List[Dict[str, Any]]:
        return await self._service.list_agents()

    async def health(self) -> Dict[str, Any]:
        return {
            **await self._service.health(),
            "pid": os.getpid(),
            "node_id": ray.get_runtime_context().get_node_id(),
        }
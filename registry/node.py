import asyncio
import os
import time
from typing import Any, Dict, List, Optional

import ray

from common.config import GOSSIP_S, JWT_SECRET, LEASE_S, RAY_NAMESPACE
from common.errors import PlatformError
from common.logging import log_event
from common.models import CapabilitySpec, DiscoverQuery, Match
from identity.jwt_utils import verify_jwt
from registry.state import (
    apply_publish,
    apply_renew,
    apply_withdraw,
    is_alive,
    merge_lww,
    purge,
)
from registry.state import query as query_state

PURGE_INTERVAL_S = 30.0


class RegistryNodeService:
    """Nodo de registro: estado en memoria con leases, validación JWT local y purga.

    Los nodos son simétricos y no comparten estado más allá del gossip (Bloque 4):
    publish/renew/withdraw operan sobre el estado local y discover responde con ese
    mismo estado. La autenticación usa solo verify_jwt, sin consultar identidad:
    un token emitido sigue siendo válido aquí hasta su expiración.
    """

    def __init__(
        self,
        node_id: str,
        lease_s: float = LEASE_S,
        gossip_s: float = GOSSIP_S,
        purge_interval_s: float = PURGE_INTERVAL_S,
        secret: Optional[str] = None,
    ) -> None:
        self.node_id = node_id
        self.state: Dict[str, Dict[str, Any]] = {}
        self.lease_s = lease_s
        self.gossip_s = gossip_s
        self.purge_interval_s = purge_interval_s
        self.secret = secret if secret is not None else JWT_SECRET

    def _now(self) -> float:
        """Reloj del nodo; los tests lo sustituyen para controlar los leases."""
        return time.time()

    def _reject_auth(self, operation: str, agent_id: str, details: str) -> PlatformError:
        log_event(
            "AUTH_ERROR",
            self.node_id,
            correlation_id=agent_id,
            agent_id=agent_id,
            operation=operation,
            error=details,
        )
        return PlatformError("AUTH_ERROR", details)

    def _verify_agent_token(self, token: str, agent_id: str, operation: str) -> None:
        try:
            claims = verify_jwt(token, self.secret)
        except PlatformError as exc:
            raise self._reject_auth(operation, agent_id, exc.details) from exc
        if claims.agent_id != agent_id:
            raise self._reject_auth(operation, agent_id, "El token no corresponde al agente")

    async def publish(self, token: str, agent_id: str, capability: CapabilitySpec, instance_id: str) -> None:
        self._verify_agent_token(token, agent_id, "publish")
        apply_publish(self.state, agent_id, capability, instance_id, self.lease_s, self._now(), self.node_id)
        log_event(
            "CAPABILITY_PUBLISHED",
            self.node_id,
            correlation_id=agent_id,
            agent_id=agent_id,
            capability_id=capability.capability_id,
            instance_id=instance_id,
        )

    async def renew(self, token: str, agent_id: str, load: float = 0.0) -> None:
        self._verify_agent_token(token, agent_id, "renew")
        if not apply_renew(self.state, agent_id, load, self.lease_s, self._now()):
            raise PlatformError("NOT_FOUND", f"Sin entradas vivas para el agente '{agent_id}'")
        log_event("LEASE_RENEWED", self.node_id, correlation_id=agent_id, agent_id=agent_id, load=load)

    async def withdraw(self, token: str, agent_id: str, capability_id: str) -> None:
        self._verify_agent_token(token, agent_id, "withdraw")
        removed = apply_withdraw(self.state, agent_id, capability_id)
        log_event(
            "CAPABILITY_WITHDRAWN",
            self.node_id,
            correlation_id=agent_id,
            agent_id=agent_id,
            capability_id=capability_id,
            removed=removed,
        )

    async def discover(self, query: DiscoverQuery) -> List[Match]:
        """Lectura sin autenticación, sobre el estado local y con leases vigentes."""
        return query_state(self.state, query, self._now())

    async def merge(self, remote_state: dict) -> None:
        """Aplica LWW sobre el estado local; el conteo solo se registra si aplicó cambios."""
        applied = merge_lww(self.state, remote_state)
        if applied:
            log_event("STATE_MERGED", self.node_id, correlation_id="N/A", applied=applied)

    async def get_state(self) -> dict:
        return {k: dict(v) for k, v in self.state.items()}

    async def health(self) -> Dict[str, Any]:
        now = self._now()
        return {
            "node": self.node_id,
            "entries": len(self.state),
            "alive": sum(1 for e in self.state.values() if is_alive(e, now)),
        }

    async def run_purge_loop(self) -> None:
        """Bucle de purga periódica; el actor lo lanza como tarea y lo cancela en stop()."""
        while True:
            await asyncio.sleep(self.purge_interval_s)
            try:
                purged = purge(self.state, self._now())
                if purged:
                    log_event("ENTRIES_PURGED", self.node_id, correlation_id="N/A", purged=purged)
            except Exception as exc:
                log_event("PURGE_ERROR", self.node_id, correlation_id="N/A", error=str(exc))


@ray.remote(namespace=RAY_NAMESPACE)
class RegistryNodeActor:
    """Actor de Ray para un nodo de registro (registry1..3, simétricos)."""

    def __init__(
        self,
        node_id: str,
        lease_s: float = LEASE_S,
        gossip_s: float = GOSSIP_S,
        purge_interval_s: float = PURGE_INTERVAL_S,
        secret: Optional[str] = None,
    ) -> None:
        self._service = RegistryNodeService(
            node_id=node_id,
            lease_s=lease_s,
            gossip_s=gossip_s,
            purge_interval_s=purge_interval_s,
            secret=secret,
        )
        self._peers: List[str] = []
        self._purge_task: Optional[asyncio.Task] = None

    async def start(self, peers: List[str]) -> None:
        """Arranca el bucle de purga; guarda los peers para el bucle de gossip."""
        self._peers = list(peers)
        if self._purge_task is None or self._purge_task.done():
            self._purge_task = asyncio.create_task(self._service.run_purge_loop())
        log_event("REGISTRY_NODE_STARTED", self._service.node_id, correlation_id="N/A", peers=self._peers)

    async def stop(self) -> None:
        if self._purge_task is not None:
            self._purge_task.cancel()
            try:
                await self._purge_task
            except asyncio.CancelledError:
                pass
            self._purge_task = None
        log_event("REGISTRY_NODE_STOPPED", self._service.node_id, correlation_id="N/A")

    async def publish(self, token: str, agent_id: str, capability: CapabilitySpec, instance_id: str) -> None:
        await self._service.publish(token, agent_id, capability, instance_id)

    async def renew(self, token: str, agent_id: str, load: float = 0.0) -> None:
        await self._service.renew(token, agent_id, load)

    async def withdraw(self, token: str, agent_id: str, capability_id: str) -> None:
        await self._service.withdraw(token, agent_id, capability_id)

    async def discover(self, query: DiscoverQuery) -> List[Match]:
        return await self._service.discover(query)

    async def merge(self, remote_state: dict) -> None:
        await self._service.merge(remote_state)

    async def get_state(self) -> dict:
        return await self._service.get_state()

    async def health(self) -> Dict[str, Any]:
        return {
            **await self._service.health(),
            "pid": os.getpid(),
            "node_id": ray.get_runtime_context().get_node_id(),
        }
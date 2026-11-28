from typing import Any, Dict, List, Optional

from common import ray_support
from common.models import Claims


class IdentityClient:
    """Cliente del actor Identity: mismas firmas que el servicio, resolución del actor encapsulada."""

    def __init__(self, actor_name: str = "identity"):
        self.actor_name = actor_name

    async def register_agent(self, name: Optional[str] = None) -> Dict[str, Any]:
        return await ray_support.call_async(self.actor_name, "register_agent", name)

    async def heartbeat(self, agent_id: str, token: str) -> Dict[str, Any]:
        return await ray_support.call_async(self.actor_name, "heartbeat", agent_id, token)

    async def verify_token(self, token: str) -> Claims:
        return await ray_support.call_async(self.actor_name, "verify_token", token)

    async def deactivate_agent(self, agent_id: str) -> None:
        await ray_support.call_async(self.actor_name, "deactivate_agent", agent_id)

    async def get_agent(self, agent_id: str) -> Dict[str, Any]:
        return await ray_support.call_async(self.actor_name, "get_agent", agent_id)

    async def list_agents(self) -> List[Dict[str, Any]]:
        return await ray_support.call_async(self.actor_name, "list_agents")

    async def health(self) -> Dict[str, Any]:
        return await ray_support.call_async(self.actor_name, "health")
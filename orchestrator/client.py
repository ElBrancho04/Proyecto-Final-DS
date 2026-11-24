from typing import Any, Dict, List, Optional

from common import ray_support


class OrchestratorClient:
    """Cliente del actor Orchestrator: expone las operaciones del contrato sin exponer Ray."""

    def __init__(self, actor_name: str = "orchestrator"):
        self.actor_name = actor_name

    async def create_task(
        self,
        steps: Optional[List[Dict[str, Any]]] = None,
        workflow: Optional[str] = None,
        input_data: Optional[Dict[str, Any]] = None,
        flow_id: Optional[str] = None,
    ) -> str:
        return await ray_support.call_async(
            self.actor_name, "create_task", steps=steps, workflow=workflow, input_data=input_data, flow_id=flow_id
        )

    async def get_task(self, task_id: str) -> Dict[str, Any]:
        return await ray_support.call_async(self.actor_name, "get_task", task_id)

    async def list_tasks(self, state: Optional[str] = None) -> List[Dict[str, Any]]:
        return await ray_support.call_async(self.actor_name, "list_tasks", state=state)

    async def cancel_task(self, task_id: str) -> None:
        await ray_support.call_async(self.actor_name, "cancel_task", task_id)

    async def health(self) -> Dict[str, Any]:
        return await ray_support.call_async(self.actor_name, "health")

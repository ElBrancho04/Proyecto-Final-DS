from typing import Dict, Any, List, Optional
from common.models import CapabilitySpec, DiscoverQuery, Match

class FakeRegistry:
    def __init__(self):
        self.registrations: List[Dict[str, Any]] = []

    async def publish(self, token: str, agent_id: str, capability: CapabilitySpec, instance_id: str) -> None:
        self.registrations.append({
            "token": token, "agent_id": agent_id, "capability": capability, "instance_id": instance_id
        })

    async def discover(self, query: DiscoverQuery) -> List[Match]:
        matches = []
        for reg in self.registrations:
            # DiscoverQuery expone el campo 'capability' (no 'capability_id')
            if reg["capability"].capability_id == query.capability:
                matches.append(Match(
                    agent_id=reg["agent_id"],
                    instance_id=reg["instance_id"],
                    capability_id=reg["capability"].capability_id,
                    lease_expires_at=9999999999.0,
                    load=0.0
                ))
        return matches

    async def health(self) -> Dict[str, Any]:
        return {"node": "fake_registry", "entries": len(self.registrations), "alive": True}

class FakeBroker:
    def __init__(self):
        self.queues: Dict[str, List[dict]] = {}

    async def publish(self, queue: str, envelope: dict) -> str:
        if queue not in self.queues:
            self.queues[queue] = []
        self.queues[queue].append(envelope)
        return envelope.get("message_id", "msg-id")

    async def consume(self, queue: str) -> Optional[Dict[str, Any]]:
        if queue in self.queues and self.queues[queue]:
            return {"delivery_id": "del-1", "queue": queue, "attempts": 1, "envelope": self.queues[queue].pop(0)}
        return None

    async def ack(self, queue: str, delivery_id: str) -> None:
        pass

    async def nack(self, queue: str, delivery_id: str) -> None:
        pass

    async def health(self) -> Dict[str, Any]:
        return {"status": "healthy", "queues": list(self.queues.keys())}
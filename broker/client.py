import asyncio
from typing import Any, Dict, List, Optional
import ray
from common import ray_support
from common.config import RAY_NAMESPACE
from common.errors import PlatformError


class BrokerClient:
    """Cliente de acceso al actor BrokerService de Ray con soporte async y utilidades síncronas."""

    def __init__(self, actor_name: str = "broker", namespace: str = RAY_NAMESPACE, actor_handle: Any = None):
        self.actor_name = actor_name
        self.namespace = namespace
        self._actor = actor_handle

    def _get_actor(self):
        """Resuelve el handle del actor del broker en Ray si aún no está en caché."""
        if self._actor is not None:
            return self._actor
        try:
            self._actor = ray_support.get_handle(self.actor_name, self.namespace)
            return self._actor
        except Exception as exc:
            raise PlatformError("BROKER_UNAVAILABLE", f"No se pudo obtener el actor '{self.actor_name}': {exc}") from exc

    def reset_handle(self) -> None:
        """Limpia el handle cacheado para forzar una nueva resolución ante fallos o reinicios."""
        self._actor = None
        ray_support.reset_handle(self.actor_name, self.namespace)

    async def _call_async(self, method_name: str, *args, **kwargs) -> Any:
        """Ejecuta un método remoto del actor BrokerService de forma no bloqueante en asyncio."""
        for attempt in range(2):
            actor = self._get_actor()
            method = getattr(actor, method_name)
            obj_ref = method.remote(*args, **kwargs)
            try:
                return await asyncio.to_thread(ray.get, obj_ref)
            except PlatformError:
                raise
            except Exception as exc:
                self.reset_handle()
                if attempt == 1:
                    raise PlatformError("BROKER_RPC_ERROR", f"Error en '{method_name}': {exc}") from exc

    def _call_sync(self, method_name: str, *args, **kwargs) -> Any:
        """Ejecuta un método remoto del actor BrokerService de forma síncrona."""
        for attempt in range(2):
            actor = self._get_actor()
            method = getattr(actor, method_name)
            obj_ref = method.remote(*args, **kwargs)
            try:
                return ray.get(obj_ref)
            except PlatformError:
                raise
            except Exception as exc:
                self.reset_handle()
                if attempt == 1:
                    raise PlatformError("BROKER_RPC_ERROR", f"Error en '{method_name}': {exc}") from exc

    async def publish(self, queue: str, envelope: dict) -> str:
        """Publica un envelope en la cola indicada y retorna el message_id."""
        return await self._call_async("publish", queue, envelope)

    async def consume(self, queue: str) -> Optional[Dict[str, Any]]:
        """Consume un mensaje pendiente de la cola indicada (o None si está vacía)."""
        return await self._call_async("consume", queue)

    async def ack(self, queue: str, delivery_id: str) -> None:
        """Confirma el procesamiento exitoso de un mensaje."""
        await self._call_async("ack", queue, delivery_id)

    async def nack(self, queue: str, delivery_id: str) -> None:
        """Rechaza un mensaje para reintento o envío a DLQ."""
        await self._call_async("nack", queue, delivery_id)

    async def stats(self) -> Dict[str, Any]:
        """Consulta las estadísticas de colas del broker."""
        return await self._call_async("stats")

    async def list_messages(self, limit: int = 50, queue: Optional[str] = None) -> List[Dict[str, Any]]:
        """Lista mensajes almacenados en el broker."""
        return await self._call_async("list_messages", limit=limit, queue=queue)

    async def health(self) -> Dict[str, Any]:
        """Verifica el estado de salud del broker."""
        return await self._call_async("health")

    # --- Variantes síncronas explícitas ---

    def publish_sync(self, queue: str, envelope: dict) -> str:
        return self._call_sync("publish", queue, envelope)

    def consume_sync(self, queue: str) -> Optional[Dict[str, Any]]:
        return self._call_sync("consume", queue)

    def ack_sync(self, queue: str, delivery_id: str) -> None:
        self._call_sync("ack", queue, delivery_id)

    def nack_sync(self, queue: str, delivery_id: str) -> None:
        self._call_sync("nack", queue, delivery_id)

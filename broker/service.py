import ray
import asyncio
from typing import Optional, Dict, Any, List
from broker.store import BrokerStore
from common.logging import log_event

@ray.remote(num_cpus=0.5)
class BrokerService:
    def __init__(self, db_path: str = "data/broker.db", visibility_timeout: int = 30, max_attempts: int = 3):
        self.store = BrokerStore(db_path=db_path)
        self.visibility_timeout = visibility_timeout
        self.max_attempts = max_attempts
        self._running = False
        self._reaper_task: Optional[asyncio.Task] = None
        log_event("BROKER_INITIALIZED", "broker", db_path=db_path)

    async def start(self) -> None:
        """Inicia el servicio del broker y su tarea en segundo plano (reaper)."""
        if self._running:
            return
        self._running = True
        # Iniciar bucle de limpieza (reaper) para manejar timeouts de visibilidad
        self._reaper_task = asyncio.create_task(self._reaper_loop())
        log_event("BROKER_STARTED", "broker")

    async def stop(self) -> None:
        """Detiene el servicio del broker y el reaper."""
        self._running = False
        if self._reaper_task:
            self._reaper_task.cancel()
            try:
                await self._reaper_task
            except asyncio.CancelledError:
                pass
        log_event("BROKER_STOPPED", "broker")

    async def publish(self, queue: str, envelope: dict) -> str:
        """Publica un mensaje en la cola especificada."""
        message_id = self.store.publish(queue, envelope)
        log_event("MESSAGE_PUBLISHED", "broker", correlation_id=envelope.get("correlation_id", "N/A"), queue=queue, message_id=message_id)
        return message_id

    async def consume(self, queue: str) -> Optional[Dict[str, Any]]:
        """Consume un mensaje pendiente aplicando el visibility timeout (default 30s)."""
        msg = self.store.consume(queue, visibility_timeout=self.visibility_timeout)
        if msg:
            envelope = msg.get("envelope", {})
            log_event("MESSAGE_CONSUMED", "broker", correlation_id=envelope.get("correlation_id", "N/A"), queue=queue, delivery_id=msg["delivery_id"])
        return msg

    async def ack(self, queue: str, delivery_id: str) -> None:
        """Confirma el procesamiento exitoso de un mensaje."""
        success = self.store.ack(queue, delivery_id)
        if success:
            log_event("MESSAGE_ACKED", "broker", queue=queue, delivery_id=delivery_id)
        else:
            log_event("MESSAGE_ACK_FAILED", "broker", queue=queue, delivery_id=delivery_id)

    async def nack(self, queue: str, delivery_id: str) -> None:
        """Rechaza un mensaje: incrementa intentos, reencola o mueve a DLQ si excede el límite[cite: 1, 2]."""
        success = self.store.nack(queue, delivery_id)
        if success:
            log_event("MESSAGE_NACKED", "broker", queue=queue, delivery_id=delivery_id)
        else:
            log_event("MESSAGE_NACK_FAILED", "broker", queue=queue, delivery_id=delivery_id)

    async def stats(self) -> Dict[str, Any]:
        """Obtiene las estadísticas actuales de las colas y estados de mensajes[cite: 2]."""
        return self.store.get_stats()

    async def list_messages(self, limit: int = 50, queue: Optional[str] = None) -> List[Dict[str, Any]]:
        """Lista los mensajes almacenados para fines de auditoría y depuración[cite: 2]."""
        return self.store.list_messages(limit=limit, queue=queue)

    async def health(self) -> Dict[str, Any]:
        """Devuelve el estado de salud del actor del broker."""
        try:
            stats = self.store.get_stats()
            return {"status": "healthy", "service": "broker", "stats": stats}
        except Exception as e:
            return {"status": "unhealthy", "service": "broker", "error": str(e)}

    async def _reaper_loop(self) -> None:
        """Bucle periódico que ejecuta el reaper cada 2 segundos para liberar mensajes expirados."""
        while self._running:
            try:
                expired_count = self.store.reaper(max_attempts=self.max_attempts)
                if expired_count > 0:
                    log_event("REAPER_PROCESSED_EXPIRED", "broker", expired_count=expired_count)
            except Exception as e:
                log_event("REAPER_ERROR", "broker", error=str(e))
            await asyncio.sleep(2)
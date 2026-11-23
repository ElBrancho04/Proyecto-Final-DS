import asyncio
import time
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple
from common.errors import PlatformError, Reject
from common.ids import new_id, now_iso
from common.logging import log_event
from common.messages import RejectCode

HandlerFunc = Callable[[str, Dict[str, Any], Dict[str, Any]], Awaitable[Dict[str, Any]]]


class MessageBus:
    """Bus de mensajería sobre BrokerService / BrokerClient con request/reply, reintentos e idempotencia."""

    RETRY_BACKOFFS_S: Tuple[float, ...] = (0.1, 0.5, 2.0)  # 100 ms, 500 ms, 2 s
    IDEMPOTENCY_TTL_S: float = 300.0  # 5 minutos
    POLL_INTERVAL_S: float = 0.4  # 400 ms

    def __init__(
        self,
        agent_id: str,
        broker: Any,
        *,
        default_timeout_s: float = 10.0,
        max_retries: int = 3,
    ) -> None:
        self.agent_id = agent_id
        self.broker = broker
        self.default_timeout_s = default_timeout_s
        self.max_retries = max_retries

        # Caché de respuestas/rechazos por idempotency_key: key -> (expires_at, response_envelope)
        self._idempotency_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        # Respuestas recibidas en reply.{agent_id} indexadas por correlation_id
        self._pending_replies: Dict[str, Dict[str, Any]] = {}
        self._reply_condition = asyncio.Condition()
        self._in_flight: int = 0
        self._stopping: bool = False

    def _build_envelope(
        self,
        msg_type: str,
        payload: Dict[str, Any],
        *,
        correlation_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        from_agent_id: Optional[str] = None,
        to_capability: Optional[str] = None,
        reply_to: Optional[str] = None,
        task_id: Optional[str] = None,
        step_id: Optional[str] = None,
        timeout_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        message_id = new_id()
        return {
            "v": 1,
            "message_id": message_id,
            "correlation_id": correlation_id or message_id,
            "idempotency_key": idempotency_key or new_id(),
            "type": msg_type,
            "from_agent_id": from_agent_id or self.agent_id,
            "to_capability": to_capability,
            "reply_to": reply_to,
            "task_id": task_id,
            "step_id": step_id,
            "timeout_ms": timeout_ms,
            "timestamp": now_iso(),
            "payload": payload,
        }

    def _cleanup_idempotency_cache(self, now: Optional[float] = None) -> None:
        current = now if now is not None else time.monotonic()
        expired_keys = [k for k, (exp, _) in self._idempotency_cache.items() if exp <= current]
        for key in expired_keys:
            del self._idempotency_cache[key]

    def _get_cached_reply(self, idempotency_key: Optional[str]) -> Optional[Dict[str, Any]]:
        if not idempotency_key:
            return None
        now = time.monotonic()
        self._cleanup_idempotency_cache(now)
        entry = self._idempotency_cache.get(idempotency_key)
        if entry is None:
            return None
        expires_at, cached_envelope = entry
        if expires_at <= now:
            del self._idempotency_cache[idempotency_key]
            return None
        return dict(cached_envelope)

    def _store_cached_reply(self, idempotency_key: Optional[str], reply_envelope: Dict[str, Any]) -> None:
        if not idempotency_key:
            return
        now = time.monotonic()
        self._cleanup_idempotency_cache(now)
        self._idempotency_cache[idempotency_key] = (now + self.IDEMPOTENCY_TTL_S, dict(reply_envelope))

    async def _poll_reply(self, reply_queue: str, correlation_id: str, deadline: float) -> Optional[Dict[str, Any]]:
        """Consume mensajes de reply.{agent_id} buscando el que coincida con correlation_id."""
        while time.monotonic() < deadline:
            async with self._reply_condition:
                if correlation_id in self._pending_replies:
                    return self._pending_replies.pop(correlation_id)

            item = await self.broker.consume(reply_queue)
            if item is not None:
                delivery_id = item.get("delivery_id")
                envelope = item.get("envelope") or {}
                msg_corr_id = envelope.get("correlation_id")

                if delivery_id:
                    await self.broker.ack(reply_queue, delivery_id)

                if msg_corr_id == correlation_id:
                    return envelope

                if msg_corr_id:
                    async with self._reply_condition:
                        self._pending_replies[msg_corr_id] = envelope
                        self._reply_condition.notify_all()
                continue

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            await asyncio.sleep(min(self.POLL_INTERVAL_S, remaining))

        return None

    async def send_request(
        self,
        to_capability: str,
        payload: Dict[str, Any],
        *,
        timeout_s: float = 10.0,
        idempotency_key: Optional[str] = None,
        task_id: Optional[str] = None,
        step_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Envía un request a cap.{to_capability} y espera response/reject en reply.{agent_id} con reintentos."""
        effective_timeout = timeout_s if timeout_s is not None else self.default_timeout_s
        idem_key = idempotency_key or new_id()
        reply_queue = f"reply.{self.agent_id}"
        cap_queue = f"cap.{to_capability}"

        last_error: Optional[Exception] = None
        total_attempts = 1 + self.max_retries

        for attempt in range(total_attempts):
            request_envelope = self._build_envelope(
                "request",
                payload,
                idempotency_key=idem_key,
                from_agent_id=self.agent_id,
                to_capability=to_capability,
                reply_to=reply_queue,
                task_id=task_id,
                step_id=step_id,
                timeout_ms=int(effective_timeout * 1000),
            )
            correlation_id = request_envelope["correlation_id"]
            attempt_deadline = time.monotonic() + effective_timeout

            await self.broker.publish(cap_queue, request_envelope)
            log_event(
                "BUS_REQUEST_SENT",
                f"bus:{self.agent_id}",
                correlation_id=correlation_id,
                capability=to_capability,
                attempt=attempt + 1,
                idempotency_key=idem_key,
            )

            reply = await self._poll_reply(reply_queue, correlation_id, attempt_deadline)
            if reply is not None:
                reply_type = reply.get("type")
                if reply_type == "reject":
                    reject_payload = reply.get("payload") or {}
                    retryable = bool(reject_payload.get("retryable", False))
                    code = reject_payload.get("code", RejectCode.INTERNAL_ERROR.value)
                    details = reject_payload.get("details", "Request rejected")
                    last_error = Reject(code=code, details=details, retryable=retryable)
                    if retryable and attempt < self.max_retries:
                        backoff = self.RETRY_BACKOFFS_S[min(attempt, len(self.RETRY_BACKOFFS_S) - 1)]
                        log_event(
                            "BUS_REQUEST_RETRY",
                            f"bus:{self.agent_id}",
                            correlation_id=correlation_id,
                            backoff_s=backoff,
                            reason=code,
                        )
                        await asyncio.sleep(backoff)
                        continue
                    return reply
                return reply

            last_error = PlatformError(
                "REQUEST_TIMEOUT",
                f"Timeout ({effective_timeout}s) esperando respuesta para '{to_capability}'",
            )
            if attempt < self.max_retries:
                backoff = self.RETRY_BACKOFFS_S[min(attempt, len(self.RETRY_BACKOFFS_S) - 1)]
                log_event(
                    "BUS_REQUEST_RETRY",
                    f"bus:{self.agent_id}",
                    correlation_id=correlation_id,
                    backoff_s=backoff,
                    reason="TIMEOUT",
                )
                await asyncio.sleep(backoff)

        raise PlatformError(
            "REQUEST_FAILED",
            f"Fallaron todos los intentos hacia '{to_capability}' ({last_error})",
        ) from last_error

    async def _publish_reply(
        self,
        request_envelope: Dict[str, Any],
        msg_type: str,
        payload: Dict[str, Any],
    ) -> None:
        reply_to = request_envelope.get("reply_to")
        if not reply_to:
            return
        reply_envelope = self._build_envelope(
            msg_type,
            payload,
            correlation_id=request_envelope.get("correlation_id") or request_envelope.get("message_id"),
            idempotency_key=request_envelope.get("idempotency_key"),
            from_agent_id=self.agent_id,
            to_capability=None,
            reply_to=None,
            task_id=request_envelope.get("task_id"),
            step_id=request_envelope.get("step_id"),
            timeout_ms=None,
        )
        await self.broker.publish(reply_to, reply_envelope)

    async def _process_item(
        self,
        queue: str,
        capability_id: str,
        item: Dict[str, Any],
        handler: HandlerFunc,
        max_concurrency: int,
    ) -> None:
        delivery_id = item.get("delivery_id")
        envelope = item.get("envelope") or {}
        correlation_id = envelope.get("correlation_id") or envelope.get("message_id") or "N/A"
        idem_key = envelope.get("idempotency_key")
        msg_type = envelope.get("type", "request")

        if msg_type == "event":
            try:
                await handler(capability_id, envelope.get("payload") or {}, envelope)
                if delivery_id:
                    await self.broker.ack(queue, delivery_id)
            except Exception as exc:
                log_event("BUS_EVENT_ERROR", f"bus:{self.agent_id}", correlation_id=correlation_id, error=str(exc))
                if delivery_id:
                    await self.broker.nack(queue, delivery_id)
            return

        # Verificar sobrecarga antes de aceptar ejecución
        if self._in_flight >= max_concurrency:
            reject_payload = {
                "code": RejectCode.OVERLOADED.value,
                "details": f"Capacidad '{capability_id}' alcanzó max_concurrency={max_concurrency}",
                "retryable": True,
            }
            await self._publish_reply(envelope, "reject", reject_payload)
            if delivery_id:
                await self.broker.ack(queue, delivery_id)
            log_event(
                "BUS_REJECT_OVERLOADED",
                f"bus:{self.agent_id}",
                correlation_id=correlation_id,
                capability=capability_id,
            )
            return

        # Consultar caché de idempotencia (TTL 5 min)
        cached = self._get_cached_reply(idem_key)
        if cached is not None and envelope.get("reply_to"):
            reply_to_send = dict(cached)
            reply_to_send["correlation_id"] = correlation_id
            await self.broker.publish(envelope["reply_to"], reply_to_send)
            if delivery_id:
                await self.broker.ack(queue, delivery_id)
            log_event(
                "BUS_IDEMPOTENCY_HIT",
                f"bus:{self.agent_id}",
                correlation_id=correlation_id,
                idempotency_key=idem_key,
            )
            return

        self._in_flight += 1
        try:
            result_payload = await handler(capability_id, envelope.get("payload") or {}, envelope)
            reply_env = self._build_envelope(
                "response",
                result_payload if isinstance(result_payload, dict) else {"result": result_payload},
                correlation_id=correlation_id,
                idempotency_key=idem_key,
                from_agent_id=self.agent_id,
                to_capability=None,
                reply_to=None,
                task_id=envelope.get("task_id"),
                step_id=envelope.get("step_id"),
                timeout_ms=None,
            )
            self._store_cached_reply(idem_key, reply_env)
            if envelope.get("reply_to"):
                await self.broker.publish(envelope["reply_to"], reply_env)
            if delivery_id:
                await self.broker.ack(queue, delivery_id)
        except Reject as rej:
            reject_payload = {
                "code": rej.code,
                "details": rej.details,
                "retryable": rej.retryable,
            }
            reject_env = self._build_envelope(
                "reject",
                reject_payload,
                correlation_id=correlation_id,
                idempotency_key=idem_key,
                from_agent_id=self.agent_id,
                to_capability=None,
                reply_to=None,
                task_id=envelope.get("task_id"),
                step_id=envelope.get("step_id"),
                timeout_ms=None,
            )
            self._store_cached_reply(idem_key, reject_env)
            if envelope.get("reply_to"):
                await self.broker.publish(envelope["reply_to"], reject_env)
            if delivery_id:
                await self.broker.ack(queue, delivery_id)
        except Exception as exc:
            reject_payload = {
                "code": RejectCode.INTERNAL_ERROR.value,
                "details": str(exc),
                "retryable": False,
            }
            await self._publish_reply(envelope, "reject", reject_payload)
            if delivery_id:
                await self.broker.nack(queue, delivery_id)
        finally:
            self._in_flight = max(0, self._in_flight - 1)

    async def serve(
        self,
        capability_id: str,
        handler: HandlerFunc,
        max_concurrency: int = 10,
    ) -> None:
        """Bucle de consumo cada 400 ms en cap.{capability_id} con control de concurrencia e idempotencia."""
        queue = f"cap.{capability_id}"
        active_tasks = set()
        self._stopping = False

        try:
            while not self._stopping:
                # Limpiar tareas terminadas
                done_tasks = {t for t in active_tasks if t.done()}
                active_tasks -= done_tasks

                item = await self.broker.consume(queue)
                if item is not None:
                    task = asyncio.create_task(
                        self._process_item(queue, capability_id, item, handler, max_concurrency)
                    )
                    active_tasks.add(task)
                    continue

                await asyncio.sleep(self.POLL_INTERVAL_S)
        except asyncio.CancelledError:
            self._stopping = True
            raise
        finally:
            self._stopping = True
            if active_tasks:
                await asyncio.gather(*active_tasks, return_exceptions=True)

    async def send_event(self, to_capability: str, name: str, data: Dict[str, Any]) -> None:
        """Publica un mensaje de tipo event hacia cap.{to_capability} sin esperar respuesta."""
        event_envelope = self._build_envelope(
            "event",
            {"name": name, "data": data},
            from_agent_id=self.agent_id,
            to_capability=to_capability,
            reply_to=None,
        )
        await self.broker.publish(f"cap.{to_capability}", event_envelope)
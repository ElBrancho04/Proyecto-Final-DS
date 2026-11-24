"""Única frontera con Ray: conexión al clúster, resolución de actores por nombre y llamadas remotas."""
import asyncio
from typing import Any, Dict

import ray

from common.config import RAY_NAMESPACE
from common.errors import PlatformError

_handles: Dict[str, Any] = {}


def connect() -> None:
    """Se conecta al clúster iniciado con `ray start --head` (idempotente)."""
    if not ray.is_initialized():
        try:
            ray.init(address="auto", namespace=RAY_NAMESPACE, ignore_reinit_error=True)
        except Exception:
            ray.init(namespace=RAY_NAMESPACE, ignore_reinit_error=True)


def get_handle(name: str, namespace: str = RAY_NAMESPACE) -> Any:
    """Resuelve un actor por nombre (ray.get_actor) con caché local."""
    key = f"{namespace}/{name}"
    handle = _handles.get(key)
    if handle is None:
        connect()
        handle = ray.get_actor(name, namespace=namespace)
        _handles[key] = handle
    return handle


def reset_handle(name: str, namespace: str = RAY_NAMESPACE) -> None:
    """Descarta el handle cacheado para forzar una nueva resolución."""
    _handles.pop(f"{namespace}/{name}", None)


def call(name: str, method: str, *args: Any, **kwargs: Any) -> Any:
    """Llamada remota síncrona; re-resuelve el actor y reintenta una vez ante RayActorError."""
    for attempt in range(2):
        handle = get_handle(name)
        try:
            return ray.get(getattr(handle, method).remote(*args, **kwargs))
        except PlatformError:
            raise
        except ray.exceptions.RayActorError:
            reset_handle(name)
            if attempt == 1:
                raise


async def call_async(name: str, method: str, *args: Any, **kwargs: Any) -> Any:
    """Llamada remota no bloqueante; re-resuelve el actor y reintenta una vez ante RayActorError."""
    for attempt in range(2):
        handle = get_handle(name)
        try:
            return await asyncio.to_thread(ray.get, getattr(handle, method).remote(*args, **kwargs))
        except PlatformError:
            raise
        except ray.exceptions.RayActorError:
            reset_handle(name)
            if attempt == 1:
                raise

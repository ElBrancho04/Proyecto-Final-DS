import ray
import time
from typing import Dict, Any
from common.config import RAY_NAMESPACE
from common.logging import log_event

@ray.remote
class EmptyActor:
    def health(self) -> Dict[str, Any]:
        return {"status": "healthy"}

def start_platform() -> None:
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
    
    log_event("START_PLATFORM", "bootstrap")
    
    # Actores fijos requeridos por el roadmap
    actors = ["identity", "registry1", "registry2", "registry3", "broker", "orchestrator"]
    for name in actors:
        try:
            actor = ray.get_actor(name, namespace=RAY_NAMESPACE)
            log_event("ACTOR_ALREADY_EXISTS", "bootstrap", actor_name=name)
        except ValueError:
            EmptyActor.options(name=name, namespace=RAY_NAMESPACE, lifetime="detached").remote()
            log_event("ACTOR_CREATED", "bootstrap", actor_name=name)

def stop_platform() -> None:
    log_event("STOP_PLATFORM", "bootstrap")
    try:
        if not ray.is_initialized():
            # Conecta al clúster existente en lugar de crear uno nuevo
            ray.init(address="auto", ignore_reinit_error=True)
        
        actors = ["identity", "registry1", "registry2", "registry3", "broker", "orchestrator"]
        for name in actors:
            try:
                actor = ray.get_actor(name, namespace=RAY_NAMESPACE)
                ray.kill(actor, no_restart=True)
                log_event("ACTOR_KILLED", "bootstrap", actor_name=name)
            except Exception:
                pass
                
        if ray.is_initialized():
            ray.shutdown()
    except Exception:
        # Silencia errores si no hay ningún clúster activo al intentar apagar
        pass

if __name__ == "__main__":
    start_platform()
    print("Plataforma levantada con éxito en Ray. Presiona Ctrl+C o ejecuta 'make down' para detenerla.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stop_platform()
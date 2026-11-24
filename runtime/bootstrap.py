import os
import ray
from typing import Dict, Any
from common.config import RAY_NAMESPACE
from common.logging import log_event
from common.ray_support import connect
from broker.client import BrokerClient
from broker.service import BrokerService
from orchestrator.service import OrchestratorActor

@ray.remote
class EmptyActor:
    def health(self) -> Dict[str, Any]:
        return {"status": "healthy", "pid": os.getpid(), "node_id": ray.get_runtime_context().get_node_id()}

def start_platform() -> None:
    connect()

    log_event("START_PLATFORM", "bootstrap")

    # Inicializar el Broker real
    try:
        broker_actor = ray.get_actor("broker", namespace=RAY_NAMESPACE)
        log_event("ACTOR_ALREADY_EXISTS", "bootstrap", actor_name="broker")
    except ValueError:
        broker_actor = BrokerService.options(name="broker", namespace=RAY_NAMESPACE, lifetime="detached").remote()
        # Iniciar tareas en segundo plano del broker (reaper)
        ray.get(broker_actor.start.remote())
        log_event("ACTOR_CREATED", "bootstrap", actor_name="broker")

    # Inicializar el Orquestador real (el registro se conecta cuando exista RegistryClient)
    try:
        ray.get_actor("orchestrator", namespace=RAY_NAMESPACE)
        log_event("ACTOR_ALREADY_EXISTS", "bootstrap", actor_name="orchestrator")
    except ValueError:
        orchestrator_actor = OrchestratorActor.options(name="orchestrator", namespace=RAY_NAMESPACE, lifetime="detached").remote(broker=BrokerClient())
        # Recuperar pasos RUNNING que quedaron de una ejecución anterior
        ray.get(orchestrator_actor.start.remote())
        log_event("ACTOR_CREATED", "bootstrap", actor_name="orchestrator")

    # Inicializar el resto de los actores (EmptyActor por ahora)
    other_actors = ["identity", "registry1", "registry2", "registry3"]
    for name in other_actors:
        try:
            ray.get_actor(name, namespace=RAY_NAMESPACE)
            log_event("ACTOR_ALREADY_EXISTS", "bootstrap", actor_name=name)
        except ValueError:
            EmptyActor.options(name=name, namespace=RAY_NAMESPACE, lifetime="detached").remote()
            log_event("ACTOR_CREATED", "bootstrap", actor_name=name)

def stop_platform() -> None:
    log_event("STOP_PLATFORM", "bootstrap")
    try:
        connect()
    except Exception as exc:
        log_event("STOP_PLATFORM_NO_CLUSTER", "bootstrap", error=str(exc))
        return
    actors =["identity", "registry1", "registry2", "registry3", "broker", "orchestrator"]
    for name in actors:
        try:
            actor = ray.get_actor(name, namespace=RAY_NAMESPACE)
            # Detener ordenadamente el broker si es necesario
            if name == "broker":
                try:
                    ray.get(actor.stop.remote(), timeout=2.0)
                except Exception:
                    pass
            ray.kill(actor, no_restart=True)
            log_event("ACTOR_KILLED", "bootstrap", actor_name=name)
        except Exception:
            pass
    if ray.is_initialized():
        ray.shutdown()

if __name__ == "__main__":
    start_platform()
    print("Plataforma levantada con éxito en Ray.")

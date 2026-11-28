"""Estado en memoria del registro de capacidades.

Funciones puras sobre un dict plano {clave: entrada}; ninguna opera I/O ni
importa Ray. Todas las que dependen del tiempo reciben `now` (epoch float) como
parámetro explícito, de modo que el comportamiento de leases y LWW sea
determinista y verificable sin esperas.
"""

from typing import Any, Dict, List

from common.models import CapabilitySpec, DiscoverQuery, Match


def make_key(agent_id: str, capability_id: str) -> str:
    """Clave del estado: una entrada por par (agente, capacidad)."""
    return f"{agent_id}:{capability_id}"


def is_alive(entry: Dict[str, Any], now: float) -> bool:
    """Una entrada está viva mientras su lease no haya vencido."""
    return entry["lease_expires_at"] > now


def apply_publish(
    state: Dict[str, Dict[str, Any]],
    agent_id: str,
    capability: CapabilitySpec,
    instance_id: str,
    lease_s: float,
    now: float,
    origin_node: str,
) -> None:
    """Inserta o reemplaza la entrada del par (agente, capacidad) con lease fresco.

    Un publish siempre crea una entrada completa y nueva (load a cero): es el
    mecanismo con el que un agente reconstruye su estado tras un NOT_FOUND.
    """
    state[make_key(agent_id, capability.capability_id)] = {
        "agent_id": agent_id,
        "capability_id": capability.capability_id,
        "version": capability.version,
        "tags": list(capability.tags),
        "constraints": dict(capability.constraints),
        "instance_id": instance_id,
        "load": 0.0,
        "lease_expires_at": now + lease_s,
        "updated_at": now,
        "origin_node": origin_node,
    }


def apply_renew(
    state: Dict[str, Dict[str, Any]],
    agent_id: str,
    load: float,
    lease_s: float,
    now: float,
) -> bool:
    """Renueva todas las entradas de un agente si al menos una está viva.

    El bump de updated_at es lo que permite que el gossip propague la renovación
    al resto de los nodos. Devuelve False si el agente no tiene entradas o todas
    están vencidas: en ese caso el llamador responde NOT_FOUND y el agente debe
    re-publicar.
    """
    keys = [k for k, e in state.items() if e["agent_id"] == agent_id]
    if not keys or not any(state[k]["lease_expires_at"] > now for k in keys):
        return False
    for k in keys:
        entry = state[k]
        entry["lease_expires_at"] = now + lease_s
        entry["load"] = load
        entry["updated_at"] = now
    return True


def apply_withdraw(state: Dict[str, Dict[str, Any]], agent_id: str, capability_id: str) -> bool:
    """Elimina la entrada del par (agente, capacidad). Idempotente.

    Sin tumbas: una entrada retirada puede reaparecer transitoriamente vía gossip
    desde un nodo rezagado, pero su lease vencido la mantiene invisible para query
    hasta que la purga la elimine en todos los nodos.
    """
    return state.pop(make_key(agent_id, capability_id), None) is not None


def merge_lww(state: Dict[str, Dict[str, Any]], remote_state: Dict[str, Dict[str, Any]]) -> int:
    """Aplica last-write-wins por updated_at y devuelve cuántas entradas se actualizaron.

    Una entrada remota solo reemplaza a la local si es estrictamente más nueva;
    en empate gana la local. La regla determinista es lo que garantiza la
    convergencia del gossip.
    """
    applied = 0
    for key, entry in remote_state.items():
        local = state.get(key)
        if local is None or entry["updated_at"] > local["updated_at"]:
            state[key] = dict(entry)
            applied += 1
    return applied


def purge(state: Dict[str, Dict[str, Any]], now: float) -> int:
    """Elimina las entradas con lease vencido y devuelve cuántas purgó."""
    dead = [k for k, e in state.items() if e["lease_expires_at"] <= now]
    for k in dead:
        del state[k]
    return len(dead)


def query(state: Dict[str, Dict[str, Any]], query: DiscoverQuery, now: float) -> List[Match]:
    """Descubre instancias vivas por capacidad, versión y tags.

    Filtros: capacidad exacta, solo entradas vivas, versión exacta (si se pide) y
    todas las tags del query presentes en la entrada. Ranking: lease restante
    descendente, luego load ascendente, con desempate (agent_id, instance_id) para
    resultados reproducibles.
    """
    matches: List[Match] = []
    for entry in state.values():
        if entry["capability_id"] != query.capability:
            continue
        if entry["lease_expires_at"] <= now:
            continue
        if query.version is not None and entry["version"] != query.version:
            continue
        if query.tags and not set(query.tags).issubset(entry["tags"]):
            continue
        matches.append(
            Match(
                agent_id=entry["agent_id"],
                instance_id=entry["instance_id"],
                capability_id=entry["capability_id"],
                version=entry["version"],
                lease_expires_at=entry["lease_expires_at"],
                load=entry["load"],
            )
        )
    matches.sort(key=lambda m: (-m.lease_expires_at, m.load, m.agent_id, m.instance_id))
    return matches[: query.max_results]
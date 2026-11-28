from common.models import CapabilitySpec, DiscoverQuery
from registry.state import (
    apply_publish,
    apply_renew,
    apply_withdraw,
    is_alive,
    make_key,
    merge_lww,
    purge,
    query,
)

T0 = 1000.0
LEASE = 15.0


def spec(capability_id="math.add", version="1.0.0", tags=None):
    return CapabilitySpec(capability_id=capability_id, name=capability_id, version=version, tags=tags or [])


def pub(state, agent_id, now, capability=None, instance_id=None, lease_s=LEASE):
    apply_publish(
        state,
        agent_id,
        capability or spec(),
        instance_id or f"{agent_id}-inst",
        lease_s,
        now,
        "registry1",
    )


def test_make_key():
    assert make_key("agent-1", "math.add") == "agent-1:math.add"


def test_publish_stamps_entry():
    state = {}
    pub(state, "agent-1", T0)

    entry = state[make_key("agent-1", "math.add")]
    assert entry["agent_id"] == "agent-1"
    assert entry["capability_id"] == "math.add"
    assert entry["instance_id"] == "agent-1-inst"
    assert entry["lease_expires_at"] == T0 + LEASE
    assert entry["updated_at"] == T0
    assert entry["origin_node"] == "registry1"
    assert entry["load"] == 0.0


def test_publish_replaces_entry_fresh():
    state = {}
    pub(state, "agent-1", T0)
    state[make_key("agent-1", "math.add")]["load"] = 7.0

    pub(state, "agent-1", T0 + 10)

    entry = state[make_key("agent-1", "math.add")]
    assert entry["load"] == 0.0
    assert entry["lease_expires_at"] == T0 + 10 + LEASE


def test_is_alive_boundary():
    entry = {"lease_expires_at": 100.0}
    assert is_alive(entry, 99.999)
    assert not is_alive(entry, 100.0)
    assert not is_alive(entry, 100.001)


def test_merge_lww_newer_remote_wins_and_counts():
    local, remote = {}, {}
    pub(local, "agent-1", T0)                              # updated_at = T0
    pub(remote, "agent-1", T0 + 5, capability=spec(version="2.0.0"))  # más nueva
    pub(remote, "agent-2", T0 + 5)                         # clave desconocida en local

    applied = merge_lww(local, remote)

    assert applied == 2
    assert local[make_key("agent-1", "math.add")]["version"] == "2.0.0"
    assert make_key("agent-2", "math.add") in local


def test_merge_lww_tie_keeps_local_and_unknown_key_is_added():
    local, remote = {}, {}
    pub(local, "agent-1", T0)
    pub(remote, "agent-1", T0, capability=spec(version="9.9.9"))  # empate: gana la local
    pub(remote, "agent-2", T0 - 5)  # clave desconocida para local: siempre se incorpora

    # Sin entrada local no hay contra qué comparar: aceptar claves nuevas es el
    # mecanismo por el que el gossip propaga el estado. Una copia vieja es
    # inofensiva: query la filtra por lease vencido cuando deje de estar viva.
    assert merge_lww(local, remote) == 1
    assert local[make_key("agent-1", "math.add")]["version"] == "1.0.0"
    assert make_key("agent-2", "math.add") in local


def test_purge_removes_only_expired():
    state = {}
    pub(state, "agent-1", T0)          # vence en T0 + 15
    pub(state, "agent-2", T0 + 10)     # vence en T0 + 25

    assert purge(state, T0 + 15) == 1
    assert make_key("agent-1", "math.add") not in state
    assert make_key("agent-2", "math.add") in state
    assert purge(state, T0 + 15) == 0  # idempotente


def test_renew_extends_all_agent_entries():
    state = {}
    pub(state, "agent-1", T0, capability=spec("math.add"))
    pub(state, "agent-1", T0, capability=spec("math.sub"))

    assert apply_renew(state, "agent-1", load=0.5, lease_s=LEASE, now=T0 + 10)

    for cap in ("math.add", "math.sub"):
        entry = state[make_key("agent-1", cap)]
        assert entry["lease_expires_at"] == T0 + 10 + LEASE
        assert entry["load"] == 0.5
        assert entry["updated_at"] == T0 + 10


def test_renew_with_one_alive_entry_renews_expired_sibling():
    state = {}
    pub(state, "agent-1", T0, capability=spec("math.add"))       # vence en T0 + 15
    pub(state, "agent-1", T0 + 14, capability=spec("math.sub"))  # vence en T0 + 29

    # En T0 + 16 la primera ya venció, pero la segunda sigue viva: la renovación
    # alcanza a ambas y el agente no pierde la capacidad vencida.
    assert apply_renew(state, "agent-1", load=0.0, lease_s=LEASE, now=T0 + 16)
    assert state[make_key("agent-1", "math.add")]["lease_expires_at"] == T0 + 16 + LEASE
    assert state[make_key("agent-1", "math.sub")]["lease_expires_at"] == T0 + 16 + LEASE


def test_renew_fails_for_unknown_or_all_expired():
    state = {}
    assert not apply_renew(state, "ghost", load=0.0, lease_s=LEASE, now=T0)

    pub(state, "agent-1", T0)
    assert not apply_renew(state, "agent-1", load=0.0, lease_s=LEASE, now=T0 + LEASE + 1)

    # Un renew rechazado no altera la entrada: el agente decide re-publicar.
    entry = state[make_key("agent-1", "math.add")]
    assert entry["lease_expires_at"] == T0 + LEASE
    assert entry["updated_at"] == T0


def test_withdraw_removes_and_is_idempotent():
    state = {}
    pub(state, "agent-1", T0)

    assert apply_withdraw(state, "agent-1", "math.add")
    assert make_key("agent-1", "math.add") not in state
    assert not apply_withdraw(state, "agent-1", "math.add")


def test_query_filters_capability_and_expired_with_deterministic_order():
    state = {}
    pub(state, "agent-1", T0)          # vivo
    pub(state, "agent-2", T0)          # vivo
    pub(state, "agent-3", T0 - LEASE)  # vence exactamente en T0: excluido
    pub(state, "agent-4", T0, capability=spec("math.sub"))

    matches = query(state, DiscoverQuery(capability="math.add"), T0)

    assert [m.agent_id for m in matches] == ["agent-1", "agent-2"]
    first = matches[0]
    assert first.instance_id == "agent-1-inst"
    assert first.capability_id == "math.add"
    assert first.version == "1.0.0"
    assert first.lease_expires_at == T0 + LEASE
    assert first.load == 0.0


def test_query_version_filter_is_exact():
    state = {}
    pub(state, "agent-1", T0, capability=spec(version="1.0.0"))
    pub(state, "agent-2", T0, capability=spec(version="2.0.0"))

    matches = query(state, DiscoverQuery(capability="math.add", version="2.0.0"), T0)

    assert [m.agent_id for m in matches] == ["agent-2"]


def test_query_tags_require_all():
    state = {}
    pub(state, "agent-1", T0, capability=spec(tags=["fast"]))
    pub(state, "agent-2", T0, capability=spec(tags=["fast", "precise"]))

    matches = query(state, DiscoverQuery(capability="math.add", tags=["fast", "precise"]), T0)
    assert [m.agent_id for m in matches] == ["agent-2"]

    matches = query(state, DiscoverQuery(capability="math.add", tags=["fast"]), T0)
    assert [m.agent_id for m in matches] == ["agent-1", "agent-2"]


def test_query_ranks_by_lease_then_load():
    state = {}
    pub(state, "agent-1", T0)        # vence en T0 + 15
    pub(state, "agent-2", T0 + 10)   # vence en T0 + 25
    pub(state, "agent-3", T0 + 10)   # vence en T0 + 25
    apply_renew(state, "agent-2", load=9.0, lease_s=LEASE, now=T0 + 10)

    matches = query(state, DiscoverQuery(capability="math.add"), T0 + 11)

    assert [m.agent_id for m in matches] == ["agent-3", "agent-2", "agent-1"]
    assert matches[1].load == 9.0


def test_query_max_results_truncates_ranked_list():
    state = {}
    for i in (1, 2, 3):
        pub(state, f"agent-{i}", T0)

    matches = query(state, DiscoverQuery(capability="math.add", max_results=2), T0)

    assert [m.agent_id for m in matches] == ["agent-1", "agent-2"]
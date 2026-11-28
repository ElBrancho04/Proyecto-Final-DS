import pytest

from common.errors import PlatformError
from common.models import CapabilitySpec, DiscoverQuery
from identity.jwt_utils import issue_jwt
from registry.node import RegistryNodeService

SECRET = "test-secret-32-bytes-for-hs256-rfc7518"
LEASE = 15.0
T0 = 1000.0


class FakeClock:
    """Reloj controlable: el service lee la hora a través de _now(), el test la avanza."""

    def __init__(self, now: float = T0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_service(node_id: str, clock: FakeClock) -> RegistryNodeService:
    service = RegistryNodeService(node_id=node_id, lease_s=LEASE, secret=SECRET)
    service._now = clock
    return service


def token_for(agent_id: str, ttl_s: float = 3600.0) -> str:
    return issue_jwt(agent_id, SECRET, ttl_s=ttl_s)


def spec(capability_id="math.add", version="1.0.0", tags=None):
    return CapabilitySpec(capability_id=capability_id, name=capability_id, version=version, tags=tags or [])


@pytest.mark.asyncio
async def test_publish_and_discover_roundtrip():
    clock = FakeClock()
    service = make_service("registry1", clock)

    await service.publish(token_for("agent-1"), "agent-1", spec(), "inst-1")

    matches = await service.discover(DiscoverQuery(capability="math.add"))
    assert [m.agent_id for m in matches] == ["agent-1"]
    assert matches[0].instance_id == "inst-1"
    assert matches[0].lease_expires_at == T0 + LEASE


@pytest.mark.asyncio
async def test_publish_rejects_malformed_token():
    clock = FakeClock()
    service = make_service("registry1", clock)

    with pytest.raises(PlatformError) as exc:
        await service.publish("no-es-un-token", "agent-1", spec(), "inst-1")

    assert exc.value.code == "AUTH_ERROR"
    assert await service.discover(DiscoverQuery(capability="math.add")) == []


@pytest.mark.asyncio
async def test_publish_rejects_expired_token():
    clock = FakeClock()
    service = make_service("registry1", clock)
    expired = issue_jwt("agent-1", SECRET, ttl_s=-1)

    with pytest.raises(PlatformError) as exc:
        await service.publish(expired, "agent-1", spec(), "inst-1")

    assert exc.value.code == "AUTH_ERROR"


@pytest.mark.asyncio
async def test_publish_rejects_token_of_another_agent():
    clock = FakeClock()
    service = make_service("registry1", clock)

    # El token es válido pero pertenece a otro agente: no puede publicar en su nombre.
    with pytest.raises(PlatformError) as exc:
        await service.publish(token_for("agent-2"), "agent-1", spec(), "inst-1")

    assert exc.value.code == "AUTH_ERROR"


@pytest.mark.asyncio
async def test_renew_unknown_agent_not_found():
    clock = FakeClock()
    service = make_service("registry1", clock)

    with pytest.raises(PlatformError) as exc:
        await service.renew(token_for("ghost"), "ghost")

    assert exc.value.code == "NOT_FOUND"


@pytest.mark.asyncio
async def test_renew_after_lease_expired_not_found():
    # El renew sobre un lease vencido responde NOT_FOUND: es la señal con la que un
    # agente que vuelve tras una pausa reconstruye su estado re-publicando.
    clock = FakeClock()
    service = make_service("registry1", clock)
    await service.publish(token_for("agent-1"), "agent-1", spec(), "inst-1")

    clock.advance(LEASE + 1)

    with pytest.raises(PlatformError) as exc:
        await service.renew(token_for("agent-1"), "agent-1")
    assert exc.value.code == "NOT_FOUND"


@pytest.mark.asyncio
async def test_full_flow_publish_discover_renew_withdraw():
    clock = FakeClock()
    service = make_service("registry1", clock)

    await service.publish(token_for("agent-1"), "agent-1", spec(), "inst-1")
    assert len(await service.discover(DiscoverQuery(capability="math.add"))) == 1

    clock.advance(10)
    await service.renew(token_for("agent-1"), "agent-1", load=0.25)

    matches = await service.discover(DiscoverQuery(capability="math.add"))
    assert matches[0].lease_expires_at == T0 + 10 + LEASE
    assert matches[0].load == 0.25

    await service.withdraw(token_for("agent-1"), "agent-1", "math.add")
    assert await service.discover(DiscoverQuery(capability="math.add")) == []

    # Idempotente: retirar de nuevo no falla ni altera el estado.
    await service.withdraw(token_for("agent-1"), "agent-1", "math.add")


@pytest.mark.asyncio
async def test_discover_excludes_expired_without_purge():
    clock = FakeClock()
    service = make_service("registry1", clock)
    await service.publish(token_for("agent-1"), "agent-1", spec(), "inst-1")

    clock.advance(LEASE + 1)

    # La entrada sigue en estado hasta que corra la purga, pero discover no la expone.
    matches = await service.discover(DiscoverQuery(capability="math.add"))
    assert matches == []


@pytest.mark.asyncio
async def test_merge_between_nodes_and_get_state():
    clock = FakeClock()
    registry1 = make_service("registry1", clock)
    registry2 = make_service("registry2", clock)

    await registry1.publish(token_for("agent-1"), "agent-1", spec(), "inst-1")
    await registry2.merge(await registry1.get_state())

    matches = await registry2.discover(DiscoverQuery(capability="math.add"))
    assert [m.agent_id for m in matches] == ["agent-1"]

    # La procedencia viaja con la entrada: registry2 sabe que originó en registry1.
    merged = await registry2.get_state()
    assert merged["agent-1:math.add"]["origin_node"] == "registry1"

    # Re-publicar produce un updated_at estrictamente mayor: sin avanzar el reloj
    # sería un empate y la copia local de registry2 ganaría (regla LWW congelada).
    clock.advance(1)
    await registry1.publish(token_for("agent-1"), "agent-1", spec(version="2.0.0"), "inst-1")
    await registry2.merge(await registry1.get_state())
    matches = await registry2.discover(DiscoverQuery(capability="math.add"))
    assert matches[0].version == "2.0.0"


@pytest.mark.asyncio
async def test_health_reports_entries_and_alive():
    clock = FakeClock()
    service = make_service("registry1", clock)

    await service.publish(token_for("agent-1"), "agent-1", spec("math.add"), "inst-1")
    clock.advance(10)
    await service.publish(token_for("agent-2"), "agent-2", spec("math.sub"), "inst-2")
    clock.advance(6)  # T0 + 16: la primera venció (T0 + 15), la segunda sigue viva

    health = await service.health()
    assert health == {"node": "registry1", "entries": 2, "alive": 1}
import time
import uuid

import jwt
import pytest

from common.errors import PlatformError
from common.models import Claims
from identity.jwt_utils import issue_jwt, verify_jwt
from identity.service import IdentityService
from identity.store import STATUS_ACTIVE, STATUS_DEACTIVATED

SECRET = "test-secret-32-bytes-for-hs256-rfc7518"


def make_service(tmp_path):
    return IdentityService(db_path=str(tmp_path / "identity.db"), secret=SECRET)


# ----------------------------------------------------------------------
# jwt_utils (verificación pura, sin estado)
# ----------------------------------------------------------------------


def test_jwt_roundtrip():
    token = issue_jwt("agent-1", SECRET, ttl_s=3600, scopes=["agent"])
    claims = verify_jwt(token, SECRET)

    assert isinstance(claims, Claims)
    assert claims.agent_id == "agent-1"
    assert claims.scopes == ["agent"]
    assert claims.exp == pytest.approx(time.time() + 3600, abs=5)


def test_jwt_default_scopes():
    claims = verify_jwt(issue_jwt("agent-1", SECRET), SECRET)
    assert claims.scopes == ["agent"]


def test_jwt_wrong_secret_rejected():
    token = issue_jwt("agent-1", SECRET)
    with pytest.raises(PlatformError) as exc:
        verify_jwt(token, "other-secret")
    assert exc.value.code == "AUTH_ERROR"


def test_jwt_expired_token_rejected():
    token = issue_jwt("agent-1", SECRET, ttl_s=-1)
    with pytest.raises(PlatformError) as exc:
        verify_jwt(token, SECRET)
    assert exc.value.code == "AUTH_ERROR"


def test_jwt_tampered_token_rejected():
    token = issue_jwt("agent-1", SECRET)
    with pytest.raises(PlatformError) as exc:
        verify_jwt(token[:-3] + "xyz", SECRET)
    assert exc.value.code == "AUTH_ERROR"


def test_jwt_missing_agent_id_rejected():
    # Token firmado correctamente pero sin los claims requeridos
    token = jwt.encode({"exp": time.time() + 3600}, SECRET, algorithm="HS256")
    with pytest.raises(PlatformError) as exc:
        verify_jwt(token, SECRET)
    assert exc.value.code == "AUTH_ERROR"


# ----------------------------------------------------------------------
# IdentityService
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_agent_returns_identity_and_stores_it(tmp_path):
    service = make_service(tmp_path)

    result = await service.register_agent(name="echo")

    assert uuid.UUID(result["agent_id"]).version == 7
    assert result["token"]
    assert result["expires_at"] > time.time()

    agent = await service.get_agent(result["agent_id"])
    assert agent["name"] == "echo"
    assert agent["status"] == STATUS_ACTIVE
    assert result["agent_id"] in [a["agent_id"] for a in await service.list_agents()]


@pytest.mark.asyncio
async def test_verify_token_returns_claims_for_active_agent(tmp_path):
    service = make_service(tmp_path)
    result = await service.register_agent()

    claims = await service.verify_token(result["token"])

    assert claims.agent_id == result["agent_id"]
    assert claims.scopes == ["agent"]
    assert claims.exp == pytest.approx(result["expires_at"], abs=2)


@pytest.mark.asyncio
async def test_verify_token_fails_after_deactivation(tmp_path):
    # La verificación fuerte (service) difiere de la pura: el token sigue siendo
    # criptográficamente válido, pero el agente ya no está activo.
    service = make_service(tmp_path)
    result = await service.register_agent()
    await service.deactivate_agent(result["agent_id"])

    agent = await service.get_agent(result["agent_id"])
    assert agent["status"] == STATUS_DEACTIVATED

    with pytest.raises(PlatformError) as exc:
        await service.verify_token(result["token"])
    assert exc.value.code == "AUTH_ERROR"


@pytest.mark.asyncio
async def test_verify_token_fails_for_unknown_agent(tmp_path):
    service = make_service(tmp_path)
    token = issue_jwt("ghost-agent", SECRET)

    with pytest.raises(PlatformError) as exc:
        await service.verify_token(token)
    assert exc.value.code == "AUTH_ERROR"


@pytest.mark.asyncio
async def test_heartbeat_renews_token(tmp_path):
    service = make_service(tmp_path)
    result = await service.register_agent()

    renewed = await service.heartbeat(result["agent_id"], result["token"])

    assert renewed["token"] != result["token"]
    assert renewed["expires_at"] > time.time()
    claims = await service.verify_token(renewed["token"])
    assert claims.agent_id == result["agent_id"]


@pytest.mark.asyncio
async def test_heartbeat_rejects_token_of_another_agent(tmp_path):
    service = make_service(tmp_path)
    first = await service.register_agent()
    second = await service.register_agent()

    with pytest.raises(PlatformError) as exc:
        await service.heartbeat(first["agent_id"], second["token"])
    assert exc.value.code == "AUTH_ERROR"


@pytest.mark.asyncio
async def test_heartbeat_rejects_expired_token(tmp_path):
    service = make_service(tmp_path)
    result = await service.register_agent()
    expired = issue_jwt(result["agent_id"], SECRET, ttl_s=-1)

    with pytest.raises(PlatformError) as exc:
        await service.heartbeat(result["agent_id"], expired)
    assert exc.value.code == "AUTH_ERROR"


@pytest.mark.asyncio
async def test_heartbeat_rejects_deactivated_agent(tmp_path):
    service = make_service(tmp_path)
    result = await service.register_agent()
    await service.deactivate_agent(result["agent_id"])

    with pytest.raises(PlatformError) as exc:
        await service.heartbeat(result["agent_id"], result["token"])
    assert exc.value.code == "AUTH_ERROR"


@pytest.mark.asyncio
async def test_deactivate_unknown_agent_not_found(tmp_path):
    service = make_service(tmp_path)
    with pytest.raises(PlatformError) as exc:
        await service.deactivate_agent("no-existe")
    assert exc.value.code == "NOT_FOUND"


@pytest.mark.asyncio
async def test_get_unknown_agent_not_found(tmp_path):
    service = make_service(tmp_path)
    with pytest.raises(PlatformError) as exc:
        await service.get_agent("no-existe")
    assert exc.value.code == "NOT_FOUND"


@pytest.mark.asyncio
async def test_identity_persists_across_restart(tmp_path):
    # Núcleo de E2E-1 a nivel unitario: la identidad sobrevive al reinicio del servicio
    # conservando agent_id, y el token sigue verificando y renovando.
    db_path = str(tmp_path / "identity.db")
    service = IdentityService(db_path=db_path, secret=SECRET)
    result = await service.register_agent(name="echo")

    restarted = IdentityService(db_path=db_path, secret=SECRET)

    agent = await restarted.get_agent(result["agent_id"])
    assert agent["status"] == STATUS_ACTIVE

    claims = await restarted.verify_token(result["token"])
    assert claims.agent_id == result["agent_id"]

    renewed = await restarted.heartbeat(result["agent_id"], result["token"])
    assert renewed["expires_at"] > time.time()
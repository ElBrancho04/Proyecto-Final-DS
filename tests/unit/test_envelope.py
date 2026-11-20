from datetime import datetime
import uuid
import pytest
from common.messages import RejectCode, create_envelope


def _is_valid_uuid4(value: str) -> bool:
    parsed = uuid.UUID(value, version=4)
    return str(parsed) == value


def test_request_envelope_fields_and_defaults():
    env = create_envelope(
        "request",
        {"a": 2, "b": 3},
        from_agent_id="agent-caller-1",
        to_capability="math.add",
        reply_to="reply.agent-caller-1",
        task_id="task-123",
        step_id="step-1",
        timeout_ms=10000,
    )

    assert env["v"] == 1
    assert _is_valid_uuid4(env["message_id"])
    assert env["correlation_id"] == env["message_id"]
    assert _is_valid_uuid4(env["idempotency_key"])
    assert env["type"] == "request"
    assert env["from_agent_id"] == "agent-caller-1"
    assert env["to_capability"] == "math.add"
    assert env["reply_to"] == "reply.agent-caller-1"
    assert env["task_id"] == "task-123"
    assert env["step_id"] == "step-1"
    assert env["timeout_ms"] == 10000
    assert env["payload"] == {"a": 2, "b": 3}

    # Timestamp en formato ISO-8601 válido
    parsed_dt = datetime.fromisoformat(env["timestamp"])
    assert parsed_dt is not None


def test_response_and_reject_preserve_correlation_and_idempotency():
    req = create_envelope(
        "request",
        {"text": "hola"},
        from_agent_id="client-1",
        to_capability="echo.text",
        reply_to="reply.client-1",
        task_id="task-9",
        step_id="step-2",
    )

    resp = create_envelope(
        "response",
        {"text": "hola"},
        correlation_id=req["correlation_id"],
        idempotency_key=req["idempotency_key"],
        from_agent_id="echo-agent-1",
        to_capability=None,
        reply_to=None,
        task_id=req["task_id"],
        step_id=req["step_id"],
    )

    assert resp["type"] == "response"
    assert resp["correlation_id"] == req["correlation_id"]
    assert resp["idempotency_key"] == req["idempotency_key"]
    assert resp["to_capability"] is None
    assert resp["reply_to"] is None
    assert resp["payload"] == {"text": "hola"}

    rej = create_envelope(
        "reject",
        {
            "code": RejectCode.OVERLOADED.value,
            "details": "Worker busy",
            "retryable": True,
        },
        correlation_id=req["correlation_id"],
        idempotency_key=req["idempotency_key"],
        from_agent_id="reject-agent-1",
        to_capability=None,
        reply_to=None,
    )

    assert rej["type"] == "reject"
    assert rej["correlation_id"] == req["correlation_id"]
    assert rej["idempotency_key"] == req["idempotency_key"]
    assert rej["payload"]["code"] == "OVERLOADED"
    assert rej["payload"]["retryable"] is True


def test_event_envelope_structure():
    ev = create_envelope(
        "event",
        {"name": "heartbeat.lost", "data": {"agent_id": "agent-x"}},
        from_agent_id="registry-1",
        to_capability="audit.events",
        reply_to=None,
    )

    assert ev["v"] == 1
    assert ev["type"] == "event"
    assert ev["from_agent_id"] == "registry-1"
    assert ev["to_capability"] == "audit.events"
    assert ev["reply_to"] is None
    assert ev["payload"]["name"] == "heartbeat.lost"


def test_reject_codes_enum():
    assert RejectCode.OVERLOADED.value == "OVERLOADED"
    assert RejectCode.INVALID_PAYLOAD.value == "INVALID_PAYLOAD"
    assert RejectCode.UNAUTHORIZED.value == "UNAUTHORIZED"
    assert RejectCode.NOT_FOUND.value == "NOT_FOUND"
    assert RejectCode.INTERNAL_ERROR.value == "INTERNAL_ERROR"
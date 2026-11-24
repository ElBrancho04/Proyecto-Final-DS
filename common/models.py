from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional

class CapabilitySpec(BaseModel):
    capability_id: str
    name: str
    version: str = "1.0.0"
    description: str = ""
    inputs: Dict[str, Any] = Field(default_factory=dict)
    outputs: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    constraints: Dict[str, Any] = Field(default_factory=lambda: {"max_concurrency": 10})

class DiscoverQuery(BaseModel):
    capability: str
    version: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    max_results: int = 5

class Match(BaseModel):
    agent_id: str
    instance_id: str
    capability_id: str
    version: str = "1.0.0"
    lease_expires_at: float
    load: float = 0.0

class Claims(BaseModel):
    agent_id: str
    scopes: List[str] = Field(default_factory=list)
    exp: float

class Step(BaseModel):
    step_id: str
    capability_id: str
    state: str  # PENDING, RUNNING, DONE, FAILED
    attempt: int = 0
    idempotency_key: str
    input: Dict[str, Any] = Field(default_factory=dict)
    input_map: Optional[Dict[str, Any]] = None
    output: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None

class Task(BaseModel):
    task_id: str
    flow_id: Optional[str] = None
    state: str  # CREATED, RUNNING, WAITING, COMPLETED, FAILED, CANCELLED
    input: Dict[str, Any] = Field(default_factory=dict)
    output: Optional[Dict[str, Any]] = None
    steps: List[Step] = Field(default_factory=list)
    created_at: str
    updated_at: str

class TaskEvent(BaseModel):
    event_id: str
    task_id: str
    type: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: str

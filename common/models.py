from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional

class CapabilitySpec(BaseModel):
    capability_id: str
    name: str
    version: str = "1.0.0"
    description: str = ""
    inputs: Dict[str, Any] = Field(default_factory=dict)
    outputs: Dict[str, Any] = Field(default_factory=dict)

class DiscoverQuery(BaseModel):
    capability_id: str
    filters: Dict[str, Any] = Field(default_factory=dict)

class Match(BaseModel):
    agent_id: str
    instance_id: str
    capability_id: str
    lease_expires_at: float
    load: float = 0.0

class Claims(BaseModel):
    agent_id: str
    scopes: List[str] = Field(default_factory=list)
    exp: float

class TaskState(BaseModel):
    task_id: str
    flow_id: Optional[str] = None
    state: str  # CREATED, RUNNING, WAITING, COMPLETED, FAILED, CANCELLED
    input_data: Dict[str, Any] = Field(default_factory=dict)
    output_data: Optional[Dict[str, Any]] = None

class StepState(BaseModel):
    step_id: str
    task_id: str
    state: str  # PENDING, RUNNING, DONE, FAILED
    capability_id: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    result: Optional[Dict[str, Any]] = None
    attempts: int = 0

class TaskEvent(BaseModel):
    event_id: str
    task_id: str
    event_type: str
    timestamp: str
    payload: Dict[str, Any] = Field(default_factory=dict)
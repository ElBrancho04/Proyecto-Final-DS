import json
import sys
from typing import Any

def log_event(event: str, actor: str, correlation_id: str = "N/A", **extra: Any) -> None:
    log_record = {
        "event": event,
        "actor": actor,
        "correlation_id": correlation_id,
        **extra
    }
    sys.stdout.write(json.dumps(log_record) + "\n")
    sys.stdout.flush()
from typing import Any, Dict, List, Optional
from common.errors import PlatformError

PLANTILLAS: Dict[str, List[Dict[str, Any]]] = {
    "workflow.echo_math": [
        {"capability_id": "echo.text", "name": "echo_step", "input_map": {"text": "$input.text"}},
        {"capability_id": "math.add", "name": "add_step", "input_map": {"a": "$input.a", "b": "$input.b"}},
        {"capability_id": "math.mul", "name": "mul_step", "input_map": {"a": "$steps.1.result", "b": "$input.k"}},
    ]
}


def resolve_input_map(
    input_map: Dict[str, Any],
    task_input: Dict[str, Any],
    step_outputs: List[Optional[Dict[str, Any]]],
) -> Dict[str, Any]:
    """
    Construye la entrada de un paso a partir de su input_map.
    "$input.<campo>" lee del input de la tarea; "$steps.<n>.<campo>" lee del output del paso n (base 0).
    Los valores que no empiezan por "$" se copian tal cual. Una referencia inexistente lanza VALIDATION_ERROR.
    """
    resolved: Dict[str, Any] = {}
    for key, ref in input_map.items():
        if not (isinstance(ref, str) and ref.startswith("$")):
            resolved[key] = ref
            continue
        parts = ref[1:].split(".")
        try:
            if parts[0] == "input" and len(parts) == 2:
                resolved[key] = task_input[parts[1]]
            elif parts[0] == "steps" and len(parts) == 3 and int(parts[1]) >= 0:
                resolved[key] = step_outputs[int(parts[1])][parts[2]]
            else:
                raise KeyError(ref)
        except (KeyError, IndexError, TypeError, ValueError):
            raise PlatformError("VALIDATION_ERROR", f"input_map['{key}']: referencia inválida o sin valor '{ref}'")
    return resolved


def resolve_steps(
    steps: Optional[List[Dict[str, Any]]] = None,
    workflow: Optional[str] = None,
    input_data: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Resuelve la lista de definiciones de pasos a partir de una lista explícita
    o de una plantilla registrada en `PLANTILLAS`.
    """
    base_input = input_data if input_data is not None else {}

    if steps is not None:
        if not isinstance(steps, list) or len(steps) == 0:
            raise PlatformError("INVALID_PAYLOAD", "El parámetro 'steps' debe ser una lista no vacía")
        resolved: List[Dict[str, Any]] = []
        for idx, item in enumerate(steps):
            if isinstance(item, str):
                cap_id = item
                step_spec: Dict[str, Any] = {"capability_id": cap_id}
            elif isinstance(item, dict):
                cap_id = item.get("capability_id") or item.get("capability")
                if not cap_id:
                    raise PlatformError(
                        "INVALID_PAYLOAD",
                        f"El paso en el índice {idx} requiere 'capability_id'",
                    )
                step_spec = dict(item)
                step_spec["capability_id"] = cap_id
            else:
                raise PlatformError(
                    "INVALID_PAYLOAD",
                    f"Formato de paso inválido en el índice {idx}",
                )
            if "input" not in step_spec:
                step_spec["input"] = dict(base_input) if idx == 0 else {}
            resolved.append(step_spec)
        return resolved

    if workflow is not None:
        template = PLANTILLAS.get(workflow)
        if template is None:
            raise PlatformError("NOT_FOUND", f"Workflow '{workflow}' no encontrado en PLANTILLAS")
        resolved = []
        for idx, tpl_step in enumerate(template):
            step_spec = dict(tpl_step)
            step_spec["input"] = dict(base_input) if idx == 0 else {}
            resolved.append(step_spec)
        return resolved

    raise PlatformError(
        "INVALID_PAYLOAD",
        "Debe proporcionarse 'steps' o un 'workflow' válido para crear la tarea",
    )

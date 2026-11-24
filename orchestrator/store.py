import json
import os
import sqlite3
from typing import Any, Dict, List, Optional

from common.errors import PlatformError
from common.ids import new_id, now_iso
from orchestrator.state_machine import StepState, TaskState


class OrchestratorStore:
    """Persistencia en SQLite para tareas, pasos y eventos append-only del Orquestador."""

    def __init__(self, db_path: str = "data/orchestrator.db") -> None:
        self.db_path = db_path
        if db_path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._persistent_conn: Optional[sqlite3.Connection] = None
        if db_path == ":memory:":
            self._persistent_conn = sqlite3.connect(":memory:", check_same_thread=False)
            self._persistent_conn.row_factory = sqlite3.Row
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        if self._persistent_conn is not None:
            return self._persistent_conn
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        """Crea las tablas tasks, task_steps y task_events si no existen."""
        conn = self._get_connection()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    flow_id TEXT,
                    state TEXT NOT NULL,
                    input_json TEXT NOT NULL,
                    output_json TEXT,
                    error_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS task_steps (
                    step_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    step_index INTEGER NOT NULL,
                    capability_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    idempotency_key TEXT NOT NULL,
                    input_json TEXT NOT NULL,
                    input_map_json TEXT,
                    output_json TEXT,
                    error_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
                )
                """
            )
            step_columns = [r["name"] for r in conn.execute("PRAGMA table_info(task_steps)").fetchall()]
            if "input_map_json" not in step_columns:
                conn.execute("ALTER TABLE task_steps ADD COLUMN input_map_json TEXT")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS task_events (
                    event_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_steps_task_idx ON task_steps(task_id, step_index);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_task_created ON task_events(task_id, created_at);"
            )
            conn.commit()
        finally:
            if self._persistent_conn is None:
                conn.close()

    @staticmethod
    def _decode_json(raw: Optional[str], default: Any) -> Any:
        if raw is None:
            return default
        return json.loads(raw)

    @staticmethod
    def _encode_json(value: Any) -> Optional[str]:
        if value is None:
            return None
        return json.dumps(value)

    def _row_to_task_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "task_id": row["task_id"],
            "flow_id": row["flow_id"],
            "state": row["state"],
            "input": self._decode_json(row["input_json"], {}),
            "output": self._decode_json(row["output_json"], None),
            "error": self._decode_json(row["error_json"], None),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _row_to_step_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "step_id": row["step_id"],
            "task_id": row["task_id"],
            "step_index": row["step_index"],
            "capability_id": row["capability_id"],
            "state": row["state"],
            "attempt": row["attempt"],
            "idempotency_key": row["idempotency_key"],
            "input": self._decode_json(row["input_json"], {}),
            "input_map": self._decode_json(row["input_map_json"], None),
            "output": self._decode_json(row["output_json"], None),
            "error": self._decode_json(row["error_json"], None),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _row_to_event_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "event_id": row["event_id"],
            "task_id": row["task_id"],
            "type": row["type"],
            "payload": self._decode_json(row["payload_json"], {}),
            "created_at": row["created_at"],
        }

    def append_event(
        self,
        task_id: str,
        event_type: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Inserta un evento en task_events (historial append-only)."""
        event_id = new_id()
        created_at = now_iso()
        event_payload = payload if payload is not None else {}

        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT INTO task_events (event_id, task_id, type, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (event_id, task_id, event_type, json.dumps(event_payload), created_at),
            )
            conn.commit()
        finally:
            if self._persistent_conn is None:
                conn.close()

        return {
            "event_id": event_id,
            "task_id": task_id,
            "type": event_type,
            "payload": event_payload,
            "created_at": created_at,
        }

    def create_task_with_steps(
        self,
        steps_spec: List[Dict[str, Any]],
        input_data: Optional[Dict[str, Any]] = None,
        flow_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Crea una tarea en estado CREATED junto con sus pasos en PENDING y registra TASK_CREATED."""
        resolved_task_id = task_id or new_id()
        now = now_iso()
        task_input = input_data if input_data is not None else {}
        initial_state = TaskState.CREATED.value

        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT INTO tasks (task_id, flow_id, state, input_json, output_json, error_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, NULL, NULL, ?, ?)
                """,
                (
                    resolved_task_id,
                    flow_id,
                    initial_state,
                    json.dumps(task_input),
                    now,
                    now,
                ),
            )

            for idx, spec in enumerate(steps_spec):
                step_id = spec.get("step_id") or f"step-{idx + 1}-{new_id()[:8]}"
                capability_id = spec["capability_id"]
                step_input = spec.get("input", task_input if idx == 0 else {})
                idem_key = spec.get("idempotency_key") or f"{resolved_task_id}:{step_id}"

                conn.execute(
                    """
                    INSERT INTO task_steps (
                        step_id, task_id, step_index, capability_id, state,
                        attempt, idempotency_key, input_json, input_map_json, output_json, error_json,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, NULL, NULL, ?, ?)
                    """,
                    (
                        step_id,
                        resolved_task_id,
                        idx,
                        capability_id,
                        StepState.PENDING.value,
                        idem_key,
                        json.dumps(step_input),
                        self._encode_json(spec.get("input_map")),
                        now,
                        now,
                    ),
                )

            conn.execute(
                """
                INSERT INTO task_events (event_id, task_id, type, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    new_id(),
                    resolved_task_id,
                    "TASK_CREATED",
                    json.dumps(
                        {
                            "flow_id": flow_id,
                            "steps_count": len(steps_spec),
                            "input": task_input,
                        }
                    ),
                    now,
                ),
            )
            conn.commit()
        finally:
            if self._persistent_conn is None:
                conn.close()

        return self.get_task(resolved_task_id)

    def get_task(self, task_id: str) -> Dict[str, Any]:
        """Recupera una tarea con sus pasos ordenados y su historial task_events."""
        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
            task_row = cur.fetchone()
            if not task_row:
                raise PlatformError("NOT_FOUND", f"Tarea '{task_id}' no encontrada")

            cur.execute(
                "SELECT * FROM task_steps WHERE task_id = ? ORDER BY step_index ASC",
                (task_id,),
            )
            step_rows = cur.fetchall()

            cur.execute(
                "SELECT * FROM task_events WHERE task_id = ? ORDER BY rowid ASC",
                (task_id,),
            )
            event_rows = cur.fetchall()
        finally:
            if self._persistent_conn is None:
                conn.close()

        task_dict = self._row_to_task_dict(task_row)
        task_dict["steps"] = [self._row_to_step_dict(r) for r in step_rows]
        task_dict["task_events"] = [self._row_to_event_dict(r) for r in event_rows]
        return task_dict

    def list_tasks(self, state: Optional[str] = None) -> List[Dict[str, Any]]:
        """Lista tareas (opcionalmente filtradas por estado), incluyendo pasos y eventos."""
        conn = self._get_connection()
        try:
            cur = conn.cursor()
            if state:
                cur.execute(
                    "SELECT task_id FROM tasks WHERE state = ? ORDER BY created_at ASC, rowid ASC",
                    (state,),
                )
            else:
                cur.execute("SELECT task_id FROM tasks ORDER BY created_at ASC, rowid ASC")
            task_ids = [r["task_id"] for r in cur.fetchall()]
        finally:
            if self._persistent_conn is None:
                conn.close()

        return [self.get_task(tid) for tid in task_ids]

    def update_task_state(
        self,
        task_id: str,
        new_state: str,
        *,
        output: Any = None,
        error: Any = None,
        event_type: Optional[str] = None,
        event_payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Persiste el nuevo estado de una tarea y registra un evento en task_events."""
        now = now_iso()
        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT task_id FROM tasks WHERE task_id = ?", (task_id,))
            if not cur.fetchone():
                raise PlatformError("NOT_FOUND", f"Tarea '{task_id}' no encontrada")

            cur.execute(
                """
                UPDATE tasks
                SET state = ?,
                    output_json = COALESCE(?, output_json),
                    error_json = COALESCE(?, error_json),
                    updated_at = ?
                WHERE task_id = ?
                """,
                (
                    new_state,
                    self._encode_json(output),
                    self._encode_json(error),
                    now,
                    task_id,
                ),
            )

            if event_type:
                cur.execute(
                    """
                    INSERT INTO task_events (event_id, task_id, type, payload_json, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        new_id(),
                        task_id,
                        event_type,
                        json.dumps(event_payload if event_payload is not None else {"state": new_state}),
                        now,
                    ),
                )

            conn.commit()
        finally:
            if self._persistent_conn is None:
                conn.close()

        return self.get_task(task_id)

    def update_step(
        self,
        step_id: str,
        *,
        state: Optional[str] = None,
        attempt: Optional[int] = None,
        input_data: Any = None,
        output: Any = None,
        error: Any = None,
        event_task_id: Optional[str] = None,
        event_type: Optional[str] = None,
        event_payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Actualiza campos de un paso y opcionalmente registra un evento en task_events."""
        now = now_iso()
        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT task_id FROM task_steps WHERE step_id = ?", (step_id,))
            row = cur.fetchone()
            if not row:
                raise PlatformError("NOT_FOUND", f"Paso '{step_id}' no encontrado")
            resolved_task_id = event_task_id or row["task_id"]

            cur.execute(
                """
                UPDATE task_steps
                SET state = COALESCE(?, state),
                    attempt = COALESCE(?, attempt),
                    input_json = COALESCE(?, input_json),
                    output_json = COALESCE(?, output_json),
                    error_json = COALESCE(?, error_json),
                    updated_at = ?
                WHERE step_id = ?
                """,
                (
                    state,
                    attempt,
                    self._encode_json(input_data),
                    self._encode_json(output),
                    self._encode_json(error),
                    now,
                    step_id,
                ),
            )

            if event_type:
                cur.execute(
                    """
                    INSERT INTO task_events (event_id, task_id, type, payload_json, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        new_id(),
                        resolved_task_id,
                        event_type,
                        json.dumps(
                            event_payload
                            if event_payload is not None
                            else {"step_id": step_id, "state": state}
                        ),
                        now,
                    ),
                )

            conn.commit()
        finally:
            if self._persistent_conn is None:
                conn.close()

        return self.get_task(resolved_task_id)

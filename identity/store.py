import os
import sqlite3
from typing import Any, Dict, List, Optional

from common.ids import now_iso

STATUS_ACTIVE = "active"
STATUS_DEACTIVATED = "deactivated"


class IdentityStore:
    """Persistencia en SQLite de las identidades de agentes (tabla agents)."""

    def __init__(self, db_path: str = "data/identity.db") -> None:
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
        conn = self._get_connection()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS agents (
                    agent_id TEXT PRIMARY KEY,
                    name TEXT,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()
        finally:
            if self._persistent_conn is None:
                conn.close()

    @staticmethod
    def _row_to_agent(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "agent_id": row["agent_id"],
            "name": row["name"],
            "status": row["status"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def insert_agent(
        self,
        agent_id: str,
        name: Optional[str] = None,
        status: str = STATUS_ACTIVE,
    ) -> Dict[str, Any]:
        """Inserta una identidad y devuelve el registro almacenado."""
        now = now_iso()
        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT INTO agents (agent_id, name, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (agent_id, name, status, now, now),
            )
            conn.commit()
        finally:
            if self._persistent_conn is None:
                conn.close()
        return {"agent_id": agent_id, "name": name, "status": status, "created_at": now, "updated_at": now}

    def get_agent(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Devuelve la identidad, o None si no existe."""
        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,))
            row = cur.fetchone()
        finally:
            if self._persistent_conn is None:
                conn.close()
        return self._row_to_agent(row) if row else None

    def list_agents(self) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM agents ORDER BY created_at ASC, rowid ASC")
            rows = cur.fetchall()
        finally:
            if self._persistent_conn is None:
                conn.close()
        return [self._row_to_agent(r) for r in rows]

    def set_status(self, agent_id: str, status: str) -> bool:
        """Cambia el estado de una identidad; False si el agente no existe."""
        now = now_iso()
        conn = self._get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE agents SET status = ?, updated_at = ? WHERE agent_id = ?",
                (status, now, agent_id),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            if self._persistent_conn is None:
                conn.close()
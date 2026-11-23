import sqlite3
import json
import os
import time
from typing import Optional, Dict, Any, List
from common.ids import new_id

class BrokerStore:
    def __init__(self, db_path: str = "data/broker.db"):
        self.db_path = db_path
        # Asegurar que el directorio data/ exista
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        """Inicializa la tabla broker_messages."""
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS broker_messages (
                    message_id TEXT PRIMARY KEY,
                    queue TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    delivery_id TEXT,
                    visible_at REAL,
                    created_at REAL NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_status ON broker_messages(queue, status);")
            conn.commit()

    def publish(self, queue: str, envelope: dict) -> str:
        """Publica un nuevo mensaje en la cola especificada con estado PENDING."""
        message_id = envelope.get("message_id") or new_id()
        envelope["message_id"] = message_id
        payload_str = json.dumps(envelope)
        now = time.time()

        with self._get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO broker_messages 
                (message_id, queue, payload, status, attempts, delivery_id, visible_at, created_at)
                VALUES (?, ?, ?, 'PENDING', 0, NULL, NULL, ?)
            """, (message_id, queue, payload_str, now))
            conn.commit()
        return message_id

    def consume(self, queue: str, visibility_timeout: int = 30) -> Optional[Dict[str, Any]]:
        """Consume un mensaje PENDING, cambiándolo a IN_FLIGHT y aplicando visibility timeout."""
        now = time.time()
        delivery_id = new_id()
        visible_at = now + visibility_timeout

        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Seleccionar el primer mensaje pendiente en orden de llegada
            cursor.execute("""
                SELECT message_id, payload, attempts FROM broker_messages
                WHERE queue = ? AND status = 'PENDING'
                ORDER BY created_at ASC
                LIMIT 1
            """, (queue,))
            row = cursor.fetchone()

            if not row:
                return None

            message_id = row["message_id"]
            payload = json.loads(row["payload"])
            attempts = row["attempts"]

            # Actualizar a IN_FLIGHT con su delivery_id y tiempo de expiración de visibilidad
            cursor.execute("""
                UPDATE broker_messages
                SET status = 'IN_FLIGHT', delivery_id = ?, visible_at = ?
                WHERE message_id = ?
            """, (delivery_id, visible_at, message_id))
            conn.commit()

            return {
                "delivery_id": delivery_id,
                "queue": queue,
                "attempts": attempts,
                "envelope": payload
            }

    def ack(self, queue: str, delivery_id: str) -> bool:
        """Confirma el procesamiento exitoso de un mensaje (estado ACKED)."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE broker_messages
                SET status = 'ACKED', visible_at = NULL
                WHERE delivery_id = ? AND queue = ? AND status = 'IN_FLIGHT'
            """, (delivery_id, queue))
            conn.commit()
            return cursor.rowcount > 0

    def nack(self, queue: str, delivery_id: str) -> bool:
        """Rechaza un mensaje: incrementa intentos, lo devuelve a PENDING o lo mueve a DLQ si excede o iguala el límite."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT message_id, attempts, payload FROM broker_messages
                WHERE delivery_id = ? AND queue = ? AND status = 'IN_FLIGHT'
            """, (delivery_id, queue))
            row = cursor.fetchone()

            if not row:
                return False

            message_id = row["message_id"]
            attempts = row["attempts"] + 1
            payload = json.loads(row["payload"])

            if attempts >= 3:
                capability_id = payload.get("to_capability", "unknown")
                dlq_queue = f"dlq.{capability_id}"
                cursor.execute("""
                    UPDATE broker_messages
                    SET status = 'DLQ', queue = ?, attempts = ?, delivery_id = NULL, visible_at = NULL
                    WHERE message_id = ?
                """, (dlq_queue, attempts, message_id))
            else:
                cursor.execute("""
                    UPDATE broker_messages
                    SET status = 'PENDING', attempts = ?, delivery_id = NULL, visible_at = NULL
                    WHERE message_id = ?
                """, (attempts, message_id))

            conn.commit()
            return True

    def reaper(self, max_attempts: int = 3) -> int:
        """Procesa mensajes IN_FLIGHT cuyo visibility_timeout ha expirado."""
        now = time.time()
        expired_count = 0

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT message_id, attempts, payload FROM broker_messages
                WHERE status = 'IN_FLIGHT' AND visible_at IS NOT NULL AND visible_at < ?
            """, (now,))
            rows = cursor.fetchall()

            for row in rows:
                message_id = row["message_id"]
                attempts = row["attempts"] + 1
                payload = json.loads(row["payload"])

                if attempts >= max_attempts:
                    capability_id = payload.get("to_capability", "unknown")
                    dlq_queue = f"dlq.{capability_id}"
                    cursor.execute("""
                        UPDATE broker_messages
                        SET status = 'DLQ', queue = ?, attempts = ?, delivery_id = NULL, visible_at = NULL
                        WHERE message_id = ?
                    """, (dlq_queue, attempts, message_id))
                else:
                    cursor.execute("""
                        UPDATE broker_messages
                        SET status = 'PENDING', attempts = ?, delivery_id = NULL, visible_at = NULL
                        WHERE message_id = ?
                    """, (attempts, message_id))
                expired_count += 1

            conn.commit()
        return expired_count

    def get_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas del estado de las colas y mensajes."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT queue, status, COUNT(*) as count
                FROM broker_messages
                GROUP BY queue, status
            """)
            rows = cursor.fetchall()
            stats = {}
            for row in rows:
                q = row["queue"]
                st = row["status"]
                cnt = row["count"]
                if q not in stats:
                    stats[q] = {}
                stats[q][st] = cnt
            return stats

    def list_messages(self, limit: int = 50, queue: Optional[str] = None) -> List[Dict[str, Any]]:
        """Lista mensajes almacenados (útil para auditoría y depuración)."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if queue:
                cursor.execute("""
                    SELECT message_id, queue, status, attempts, delivery_id, visible_at, created_at, payload
                    FROM broker_messages
                    WHERE queue = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                """, (queue, limit))
            else:
                cursor.execute("""
                    SELECT message_id, queue, status, attempts, delivery_id, visible_at, created_at, payload
                    FROM broker_messages
                    ORDER BY created_at DESC
                    LIMIT ?
                """, (limit,))
            rows = cursor.fetchall()
            result = []
            for row in rows:
                d = dict(row)
                d["payload"] = json.loads(d["payload"])
                result.append(d)
            return result
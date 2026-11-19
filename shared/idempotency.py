import hashlib
import json
import sqlite3
from typing import Optional, Tuple
from shared.problems import ProblemException


def ensure_table(conn: sqlite3.Connection):
    conn.execute("""CREATE TABLE IF NOT EXISTS idempotency_keys (
        key TEXT PRIMARY KEY,
        request_hash TEXT NOT NULL,
        response_json TEXT NOT NULL,
        status_code INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)""")
    conn.commit()


def request_hash(payload) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()


def get_cached(conn: sqlite3.Connection, key: str, req_hash: str) -> Optional[Tuple[str, int]]:
    row = conn.execute(
        "SELECT request_hash, response_json, status_code FROM idempotency_keys WHERE key=?",
        (key,),
    ).fetchone()
    if not row:
        return None
    if row["request_hash"] != req_hash:
        raise ProblemException(
            status=422,
            title="Idempotency key mismatch",
            detail="Same Idempotency-Key reused with different payload",
            type="https://dstu-events.local/errors/idempotency-mismatch",
        )
    return row["response_json"], row["status_code"]


def save(conn: sqlite3.Connection, key: str, req_hash: str, response_json: str, status_code: int):
    conn.execute(
        "INSERT OR REPLACE INTO idempotency_keys (key, request_hash, response_json, status_code) "
        "VALUES (?,?,?,?)",
        (key, req_hash, response_json, status_code),
    )
    conn.commit()
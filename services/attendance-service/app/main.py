import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, Header, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from shared.database import connect
from shared.events import TOPIC_ATTENDANCE_MARKED, TOPIC_ATTENDANCE_NO_SHOW, make_envelope
from shared.idempotency import ensure_table, get_cached, save, request_hash
from shared.logging import configure_logging
from shared.problems import ProblemException, install_problem_handlers
from shared.rabbit import RabbitBus

configure_logging("attendance-service")
log = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "attendance.db")
REG_URL = os.getenv("REGISTRATION_SERVICE_URL", "http://localhost:8002")

conn = connect(DB_PATH)
conn.executescript("""
CREATE TABLE IF NOT EXISTS attendance (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    student_id TEXT NOT NULL,
    registration_id TEXT NOT NULL,
    qr_token TEXT,
    status TEXT NOT NULL,
    marked_at TEXT NOT NULL,
    marked_by TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_attendance_event_qr ON attendance(event_id, qr_token);
""")
conn.commit()
ensure_table(conn)

bus = RabbitBus()


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await bus.connect()
    except Exception:
        log.exception("rabbit connect failed")
    yield
    await bus.close()


app = FastAPI(title="attendance-service", lifespan=lifespan)
install_problem_handlers(app)


class ScanQrRequest(BaseModel):
    qrToken: str
    eventId: str
    scannedBy: str


class NoShowRequest(BaseModel):
    registrationId: str
    eventId: str
    markedBy: str


def _att_dict(row) -> dict:
    return {
        "id": row["id"], "eventId": row["event_id"], "studentId": row["student_id"],
        "registrationId": row["registration_id"],
        "status": row["status"], "markedAt": row["marked_at"], "markedBy": row["marked_by"],
    }


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


@app.post("/api/v1/attendance/scan", status_code=201)
async def scan_qr(
        req: ScanQrRequest,
        idempotency_key: str = Header(..., alias="Idempotency-Key"),
        x_correlation_id: Optional[str] = Header(None, alias="X-Correlation-Id"),
):
    r_hash = request_hash(req.model_dump())
    cached = get_cached(conn, idempotency_key, r_hash)
    if cached:
        body, code = cached
        return JSONResponse(content=json.loads(body), status_code=code)

    async with httpx.AsyncClient(timeout=5) as c:
        try:
            r = await c.get(f"{REG_URL}/internal/v1/registrations/by-qr/{req.qrToken}")
        except httpx.RequestError as e:
            raise ProblemException(503, "Registration service unavailable", str(e))
        if r.status_code == 404:
            raise ProblemException(404, "QR not found")
        if r.status_code == 410:
            raise ProblemException(410, "QR revoked")
        if r.status_code >= 400:
            raise ProblemException(r.status_code, "Registration service error", r.text[:200])
        reg = r.json()

    if reg["status"] != "CONFIRMED":
        raise ProblemException(409, "Registration not confirmed", f"Status: {reg['status']}")

    aid = str(uuid.uuid4())
    now = _now_iso()
    try:
        conn.execute(
            "INSERT INTO attendance (id, event_id, student_id, registration_id, qr_token, status, marked_at, marked_by) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (aid, req.eventId, reg["studentId"], reg["id"], req.qrToken, "PRESENT", now, req.scannedBy),
        )
        conn.commit()
    except Exception:
        raise ProblemException(409, "Already marked", "Attendance for this QR already recorded")

    result = {"id": aid, "eventId": req.eventId, "studentId": reg["studentId"],
              "registrationId": reg["id"], "status": "PRESENT", "markedAt": now, "markedBy": req.scannedBy}

    try:
        await bus.publish(TOPIC_ATTENDANCE_MARKED, make_envelope(TOPIC_ATTENDANCE_MARKED, {
            "attendanceId": aid, "studentId": reg["studentId"], "eventId": req.eventId, "status": "PRESENT",
        }, x_correlation_id or ""))
    except Exception:
        log.exception("publish attendance.marked failed")

    save(conn, idempotency_key, r_hash, json.dumps(result), 201)
    return result


@app.post("/api/v1/attendance/no-show", status_code=201)
async def mark_no_show(
        req: NoShowRequest,
        idempotency_key: str = Header(..., alias="Idempotency-Key"),
        x_correlation_id: Optional[str] = Header(None, alias="X-Correlation-Id"),
):
    r_hash = request_hash(req.model_dump())
    cached = get_cached(conn, idempotency_key, r_hash)
    if cached:
        body, code = cached
        return JSONResponse(content=json.loads(body), status_code=code)

    async with httpx.AsyncClient(timeout=5) as c:
        r = await c.get(f"{REG_URL}/internal/v1/registrations/{req.registrationId}")
        if r.status_code == 404:
            raise ProblemException(404, "Registration not found")
        reg = r.json()

    aid = str(uuid.uuid4())
    now = _now_iso()
    try:
        conn.execute(
            "INSERT INTO attendance (id, event_id, student_id, registration_id, qr_token, status, marked_at, marked_by) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (aid, req.eventId, reg["studentId"], req.registrationId, None, "NO_SHOW", now, req.markedBy),
        )
        conn.commit()
    except Exception:
        raise ProblemException(409, "Already marked")

    result = {"id": aid, "eventId": req.eventId, "studentId": reg["studentId"],
              "registrationId": req.registrationId, "status": "NO_SHOW", "markedAt": now, "markedBy": req.markedBy}

    try:
        await bus.publish(TOPIC_ATTENDANCE_NO_SHOW, make_envelope(TOPIC_ATTENDANCE_NO_SHOW, {
            "attendanceId": aid, "studentId": reg["studentId"], "eventId": req.eventId, "status": "NO_SHOW",
        }, x_correlation_id or ""))
    except Exception:
        log.exception("publish attendance.no_show failed")

    save(conn, idempotency_key, r_hash, json.dumps(result), 201)
    return result


@app.get("/internal/v1/attendance/by-event/{event_id}")
async def list_by_event(event_id: str, page: int = Query(0, ge=0), size: int = Query(20, ge=1, le=100)):
    q = "SELECT * FROM attendance WHERE event_id=?"
    rows = conn.execute(q, (event_id,)).fetchall()
    content = [_att_dict(r) for r in rows]
    total = len(content)
    return {"content": content[page * size:(page + 1) * size], "page": page, "size": size,
            "totalElements": total, "totalPages": (total + size - 1) // size if total else 0}
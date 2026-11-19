import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx
from fastapi import FastAPI, Header, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from shared.database import connect
from shared.events import (
    TOPIC_EVENT_CANCELLED, TOPIC_REGISTRATION_CANCELLED,
    TOPIC_REGISTRATION_CONFIRMED, TOPIC_REGISTRATION_WAITLISTED, make_envelope,
)
from shared.idempotency import ensure_table, get_cached, save, request_hash
from shared.logging import configure_logging
from shared.problems import ProblemException, install_problem_handlers
from shared.rabbit import RabbitBus

configure_logging("registration-service")
log = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "registration.db")
EVENT_URL = os.getenv("EVENT_SERVICE_URL", "http://localhost:8001")
PROFILE_URL = os.getenv("PROFILE_SERVICE_URL", "http://localhost:8004")

conn = connect(DB_PATH)
conn.executescript("""
CREATE TABLE IF NOT EXISTS registrations (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    student_id TEXT NOT NULL,
    status TEXT NOT NULL,
    qr_token TEXT,
    waitlist_position INTEGER,
    created_at TEXT NOT NULL,
    expires_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_reg_event_student ON registrations(event_id, student_id);
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    processed_at TEXT NOT NULL
);
""")
conn.commit()
ensure_table(conn)

bus = RabbitBus()


# --- Вспомогательные функции для HTTP-вызовов ---
async def _upstream(method: str, url: str, **kwargs):
    async with httpx.AsyncClient(timeout=5) as c:
        try:
            r = await c.request(method, url, **kwargs)
        except httpx.RequestError as e:
            raise ProblemException(503, "Upstream unavailable", str(e),
                                   "https://dstu-events.local/errors/service-unavailable")
        if r.status_code >= 400:
            try:
                b = r.json()
                raise ProblemException(r.status_code, b.get("title", "Upstream error"),
                                       b.get("detail"), b.get("type", "about:blank"))
            except Exception:
                raise ProblemException(r.status_code, "Upstream error", r.text[:200])
        return r.json()


async def _reserve_with_retry(event_id: str, student_id: str, reg_id: str):
    """Пытаемся зарезервировать место с 3 повторами (Circuit Breaker)"""
    last_err = None
    for attempt, delay in enumerate([0.0, 0.2, 0.5, 1.0]):
        if delay:
            await asyncio.sleep(delay)
        try:
            return await _upstream(
                "POST", f"{EVENT_URL}/internal/v1/events/{event_id}/seats/reserve",
                headers={"Idempotency-Key": f"reserve-{reg_id}"},
                json={"studentId": student_id, "registrationId": reg_id},
            )
        except ProblemException as e:
            if e.problem.status == 409:
                return None  # мест нет → waitlist
            last_err = e
            log.warning("reserve attempt %d failed: %s", attempt + 1, e.problem.title)
    raise last_err


async def _release_seat(event_id: str, student_id: str, reg_id: str, reason: str):
    """Компенсация: освобождение места при сбое"""
    try:
        await _upstream(
            "POST", f"{EVENT_URL}/internal/v1/events/{event_id}/seats/release",
            headers={"Idempotency-Key": f"release-{reg_id}-{reason}"},
            json={"studentId": student_id, "registrationId": reg_id, "reason": reason},
        )
        log.info("Компенсация: место для %s освобождено", reg_id)
    except Exception:
        log.exception("seat release failed for %s", reg_id)


# --- Асинхронные обработчики (Слушаем event.cancelled) ---
async def handle_event_cancelled(topic: str, envelope: dict):
    env_id = envelope.get("eventId")
    if conn.execute("SELECT 1 FROM processed_events WHERE event_id=?", (env_id,)).fetchone():
        return

    event_id = envelope["payload"]["eventId"]
    rows = conn.execute(
        "SELECT * FROM registrations WHERE event_id=? AND status IN ('CONFIRMED','PENDING','WAITLISTED')",
        (event_id,),
    ).fetchall()
    now = datetime.now(timezone.utc).isoformat()

    for r in rows:
        conn.execute("UPDATE registrations SET status='CANCELLED' WHERE id=?", (r["id"],))
        try:
            await bus.publish(TOPIC_REGISTRATION_CANCELLED,
                              make_envelope(TOPIC_REGISTRATION_CANCELLED, {
                                  "registrationId": r["id"], "studentId": r["student_id"],
                                  "eventId": r["event_id"], "reason": "EVENT_CANCELLED",
                              }, envelope.get("correlationId", "")))
        except Exception:
            log.exception("publish registration.cancelled failed")

    conn.execute("INSERT INTO processed_events (event_id, processed_at) VALUES (?,?)", (env_id, now))
    conn.commit()
    log.info("event.cancelled processed: %d registrations cancelled", len(rows))


# --- Фоновая задача для протухания PENDING-заявок (TTL) ---
async def _ttl_loop():
    while True:
        await asyncio.sleep(30)
        try:
            now = datetime.now(timezone.utc).isoformat()
            rows = conn.execute(
                "SELECT * FROM registrations WHERE status='PENDING' AND expires_at < ?", (now,)
            ).fetchall()
            for r in rows:
                conn.execute("UPDATE registrations SET status='EXPIRED' WHERE id=?", (r["id"],))
                conn.commit()
                await _release_seat(r["event_id"], r["student_id"], r["id"], "EXPIRED")
        except Exception:
            log.exception("ttl loop error")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await bus.connect()
        await bus.subscribe("registration-service", [TOPIC_EVENT_CANCELLED], handle_event_cancelled)
    except Exception:
        log.exception("rabbit connect failed")
    ttl_task = asyncio.create_task(_ttl_loop())
    yield
    ttl_task.cancel()
    await bus.close()


app = FastAPI(title="registration-service", lifespan=lifespan)
install_problem_handlers(app)


# --- Схемы данных ---
class CreateRegistrationRequest(BaseModel):
    eventId: str


class ConfirmRegistrationRequest(BaseModel):
    correlationId: str


# --- Вспомогательные функции ---
def _reg_dict(row) -> dict:
    return {
        "id": row["id"], "eventId": row["event_id"], "studentId": row["student_id"],
        "status": row["status"], "qrToken": row["qr_token"],
        "waitlistPosition": row["waitlist_position"], "createdAt": row["created_at"],
        "expiresAt": row["expires_at"],
    }


def _paginate(items, page, size):
    total = len(items)
    total_pages = (total + size - 1) // size if total else 0
    return {"content": items[page * size: (page + 1) * size], "page": page, "size": size,
            "totalElements": total, "totalPages": total_pages}


# --- Публичные эндпоинты ---
@app.post("/api/v1/registrations", status_code=201)
async def create_registration(
        req: CreateRegistrationRequest,
        x_student_id: str = Header(..., alias="X-Student-Id"),
        idempotency_key: str = Header(..., alias="Idempotency-Key"),
        x_correlation_id: Optional[str] = Header(None, alias="X-Correlation-Id"),
):
    r_hash = request_hash({**req.model_dump(), "studentId": x_student_id})
    cached = get_cached(conn, idempotency_key, r_hash)
    if cached:
        body, code = cached
        return JSONResponse(content=json.loads(body), status_code=code)

    # ШАГ 1: Проверяем статус студента (profile-service)
    profile = await _upstream("GET", f"{PROFILE_URL}/internal/v1/students/{x_student_id}")
    if profile["status"] != "ACTIVE":
        raise ProblemException(403, "Student cannot register", f"Status is {profile['status']}")

    # ШАГ 2: Резервируем место (event-service)
    reg_id = str(uuid.uuid4())
    reserve = await _reserve_with_retry(req.eventId, x_student_id, reg_id)
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    if reserve is None:
        # Мест нет → добавляем в лист ожидания
        max_pos = conn.execute(
            "SELECT COALESCE(MAX(waitlist_position), 0) FROM registrations WHERE event_id=? AND status='WAITLISTED'",
            (req.eventId,),
        ).fetchone()[0]
        position = max_pos + 1
        try:
            conn.execute(
                "INSERT INTO registrations (id, event_id, student_id, status, waitlist_position, created_at) "
                "VALUES (?,?,?,?,?,?)",
                (reg_id, req.eventId, x_student_id, "WAITLISTED", position, now_iso),
            )
            conn.commit()
        except Exception:
            raise ProblemException(409, "Already registered for this event")

        result = {"id": reg_id, "eventId": req.eventId, "studentId": x_student_id,
                  "status": "WAITLISTED", "waitlistPosition": position, "createdAt": now_iso}
        save(conn, idempotency_key, r_hash, json.dumps(result), 202)
        await bus.publish(TOPIC_REGISTRATION_WAITLISTED, make_envelope(TOPIC_REGISTRATION_WAITLISTED, {
            "registrationId": reg_id, "studentId": x_student_id, "eventId": req.eventId, "position": position,
        }, x_correlation_id or ""))
        return JSONResponse(content=result, status_code=202)

    # ШАГ 3: Создаем запись локально
    qr_token = f"QR-{req.eventId[:8]}-{uuid.uuid4().hex[:8]}"
    try:
        conn.execute(
            "INSERT INTO registrations (id, event_id, student_id, status, qr_token, created_at, expires_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (reg_id, req.eventId, x_student_id, "CONFIRMED", qr_token, now_iso,
             (now + timedelta(minutes=15)).isoformat()),
        )
        conn.commit()
    except Exception:
        # КОМПЕНСАЦИЯ: если локальная запись упала, освобождаем место
        await _release_seat(req.eventId, x_student_id, reg_id, "CANCELLED_BY_STUDENT")
        raise ProblemException(409, "Already registered for this event")

    result = {"id": reg_id, "eventId": req.eventId, "studentId": x_student_id,
              "status": "CONFIRMED", "qrToken": qr_token, "createdAt": now_iso}

    # ШАГ 4: Отправляем событие об успехе
    await bus.publish(TOPIC_REGISTRATION_CONFIRMED, make_envelope(TOPIC_REGISTRATION_CONFIRMED, {
        "registrationId": reg_id, "studentId": x_student_id, "eventId": req.eventId,
        "qrToken": qr_token, "studentEmail": profile.get("email"),
    }, x_correlation_id or ""))

    save(conn, idempotency_key, r_hash, json.dumps(result), 201)
    return result


@app.get("/api/v1/registrations/my")
async def list_my_registrations(
        x_student_id: str = Header(..., alias="X-Student-Id"),
        status: Optional[str] = Query(None),
        page: int = Query(0, ge=0),
        size: int = Query(20, ge=1, le=100),
):
    q = "SELECT * FROM registrations WHERE student_id=?"
    args = [x_student_id]
    if status: q += " AND status=?"; args.append(status)
    q += " ORDER BY created_at DESC"
    rows = conn.execute(q, args).fetchall()
    return _paginate([_reg_dict(r) for r in rows], page, size)


@app.delete("/api/v1/registrations/{reg_id}", status_code=204)
async def cancel_registration(reg_id: str, x_student_id: str = Header(..., alias="X-Student-Id")):
    row = conn.execute("SELECT * FROM registrations WHERE id=?", (reg_id,)).fetchone()
    if not row: raise ProblemException(404, "Registration not found")
    if row["student_id"] != x_student_id: raise ProblemException(403, "Forbidden")
    if row["status"] in ("CANCELLED", "EXPIRED", "REJECTED"):
        raise ProblemException(409, "Registration in final state")

    conn.execute("UPDATE registrations SET status='CANCELLED' WHERE id=?", (reg_id,))
    conn.commit()
    if row["status"] in ("CONFIRMED", "PENDING"):
        await _release_seat(row["event_id"], row["student_id"], reg_id, "CANCELLED_BY_STUDENT")
    await bus.publish(TOPIC_REGISTRATION_CANCELLED, make_envelope(TOPIC_REGISTRATION_CANCELLED, {
        "registrationId": reg_id, "studentId": row["student_id"],
        "eventId": row["event_id"], "reason": "STUDENT_CANCELLED",
    }, ""))
    return None


# --- Внутренние эндпоинты ---
@app.get("/internal/v1/registrations/by-qr/{token}")
async def get_by_qr(token: str):
    row = conn.execute("SELECT * FROM registrations WHERE qr_token=?", (token,)).fetchone()
    if not row: raise ProblemException(404, "Registration not found by QR")
    if row["status"] == "CANCELLED": raise ProblemException(410, "QR token revoked")
    return _reg_dict(row)


@app.get("/internal/v1/registrations/{reg_id}")
async def get_reg_internal(reg_id: str):
    row = conn.execute("SELECT * FROM registrations WHERE id=?", (reg_id,)).fetchone()
    if not row: raise ProblemException(404, "Registration not found")
    return _reg_dict(row)
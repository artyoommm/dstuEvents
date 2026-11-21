import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Header, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from shared.database import connect
from shared.idempotency import ensure_table, get_cached, save, request_hash
from shared.logging import configure_logging
from shared.problems import ProblemException, install_problem_handlers
from shared.rabbit import RabbitBus
from shared.events import TOPIC_EVENT_PUBLISHED, TOPIC_EVENT_CANCELLED, make_envelope

configure_logging("event-service")
log = logging.getLogger(__name__)

# --- Инициализация БД SQLite ---
DB_PATH = os.getenv("DB_PATH", "event.db")
conn = connect(DB_PATH)
conn.executescript("""
CREATE TABLE IF NOT EXISTS venues (
    id INTEGER PRIMARY KEY,
    building TEXT NOT NULL,
    room TEXT NOT NULL,
    capacity INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'DRAFT',
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    venue_id INTEGER NOT NULL,
    seat_limit INTEGER NOT NULL,
    seats_reserved INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS seat_reservations (
    registration_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    student_id TEXT NOT NULL,
    reserved_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
""")
conn.commit()
ensure_table(conn)

if conn.execute("SELECT COUNT(*) FROM venues").fetchone()[0] == 0:
    conn.executemany("INSERT INTO venues (id, building, room, capacity) VALUES (?,?,?,?)", [
        (12, "Корпус 8", "401", 200),
        (13, "Корпус 1", "Актовый зал", 500),
    ])
    conn.commit()

bus = RabbitBus()


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await bus.connect()
    except Exception:
        log.exception("Ошибка подключения к RabbitMQ")
    yield
    await bus.close()


app = FastAPI(title="event-service", lifespan=lifespan)
install_problem_handlers(app)


# --- Схемы данных ---
class CreateEventRequest(BaseModel):
    title: str = Field(max_length=200)
    description: Optional[str] = Field(default=None, max_length=2000)
    type: str
    startsAt: str
    endsAt: str
    venueId: int
    seatLimit: int = Field(ge=1)


class UpdateEventRequest(BaseModel):
    title: Optional[str] = Field(default=None, max_length=200)
    description: Optional[str] = Field(default=None, max_length=2000)
    startsAt: Optional[str] = None
    endsAt: Optional[str] = None
    venueId: Optional[int] = None
    seatLimit: Optional[int] = Field(default=None, ge=1)


class ReserveSeatRequest(BaseModel):
    studentId: str
    registrationId: str


class ReleaseSeatRequest(BaseModel):
    studentId: str
    registrationId: str
    reason: str


# --- Вспомогательные функции ---
def _event_to_dict(row) -> dict:
    venue = conn.execute("SELECT * FROM venues WHERE id=?", (row["venue_id"],)).fetchone()
    return {
        "id": row["id"], "title": row["title"], "description": row["description"],
        "type": row["type"], "status": row["status"], "startsAt": row["starts_at"],
        "endsAt": row["ends_at"], "venue": dict(venue) if venue else None,
        "seatLimit": row["seat_limit"], "seatsAvailable": row["seat_limit"] - row["seats_reserved"],
        "createdAt": row["created_at"], "updatedAt": row["updated_at"],
    }


def _event_summary(row) -> dict:
    return {
        "id": row["id"], "title": row["title"], "type": row["type"],
        "status": row["status"], "startsAt": row["starts_at"],
        "seatsAvailable": row["seat_limit"] - row["seats_reserved"],
    }


def _paginate(items: list, page: int, size: int) -> dict:
    total = len(items)
    total_pages = (total + size - 1) // size if total else 0
    return {
        "content": items[page * size: (page + 1) * size],
        "page": page, "size": size, "totalElements": total, "totalPages": total_pages
    }


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _require_admin(x_roles: Optional[str]):
    if not x_roles or "admin" not in [r.strip() for r in x_roles.split(",")]:
        raise ProblemException(403, "Forbidden", "Admin role required")


# --- Публичные эндпоинты ---
@app.get("/api/v1/events")
async def list_events(
        type: Optional[str] = Query(None),
        dateFrom: Optional[str] = Query(None),
        dateTo: Optional[str] = Query(None),
        page: int = Query(0, ge=0),
        size: int = Query(20, ge=1, le=100),
):
    q = "SELECT * FROM events WHERE status = 'PUBLISHED'"
    args = []
    if type: q += " AND type = ?"; args.append(type)
    if dateFrom: q += " AND starts_at >= ?"; args.append(dateFrom)
    if dateTo: q += " AND starts_at <= ?"; args.append(dateTo)
    rows = conn.execute(q, args).fetchall()
    return _paginate([_event_summary(r) for r in rows], page, size)


@app.get("/api/v1/events/{event_id}")
async def get_event(event_id: str):
    row = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
    if not row:
        raise ProblemException(404, "Event not found", f"Event {event_id} not found")
    return _event_to_dict(row)


# --- Админские эндпоинты ---
@app.post("/api/v1/admin/events", status_code=201)
async def create_event(
        req: CreateEventRequest,
        idempotency_key: str = Header(..., alias="Idempotency-Key"),
        x_roles: Optional[str] = Header(None, alias="X-Roles"),
):
    _require_admin(x_roles)
    r_hash = request_hash(req.model_dump())
    cached = get_cached(conn, idempotency_key, r_hash)
    if cached:
        body, code = cached
        return JSONResponse(content=json.loads(body), status_code=code)

    eid = str(uuid.uuid4())
    now = _now_iso()
    conn.execute(
        "INSERT INTO events (id, title, description, type, status, starts_at, ends_at, venue_id, seat_limit, seats_reserved, created_at, updated_at) "
        "VALUES (?,?,?,?,'DRAFT',?,?,?,?,0,?,?)",
        (eid, req.title, req.description, req.type, req.startsAt, req.endsAt, req.venueId, req.seatLimit, now, now),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM events WHERE id=?", (eid,)).fetchone()
    result = _event_to_dict(row)
    save(conn, idempotency_key, r_hash, json.dumps(result), 201)
    return result


@app.put("/api/v1/admin/events/{event_id}")
async def update_event(
        event_id: str,
        req: UpdateEventRequest,
        x_roles: Optional[str] = Header(None, alias="X-Roles")
):
    _require_admin(x_roles)
    row = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
    if not row: raise ProblemException(404, "Event not found")
    if req.seatLimit is not None and req.seatLimit < row["seats_reserved"]:
        raise ProblemException(409, "Seat limit below reservations", f"Cannot reduce to {req.seatLimit}")

    fields, values = [], []
    for k, v in req.model_dump(exclude_none=True).items():
        if k == "startsAt":
            fields.append("starts_at = ?");
            values.append(v)
        elif k == "endsAt":
            fields.append("ends_at = ?");
            values.append(v)
        elif k == "venueId":
            fields.append("venue_id = ?");
            values.append(v)
        elif k == "seatLimit":
            fields.append("seat_limit = ?");
            values.append(v)
        else:
            fields.append(f"{k} = ?");
            values.append(v)

    if fields:
        fields.append("updated_at = ?")
        values.append(_now_iso())
        values.append(event_id)
        conn.execute(f"UPDATE events SET {', '.join(fields)} WHERE id=?", values)
        conn.commit()

    row = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
    return _event_to_dict(row)


@app.post("/api/v1/admin/events/{event_id}/publish")
async def publish_event(
        event_id: str,
        x_roles: Optional[str] = Header(None, alias="X-Roles"),
        x_correlation_id: Optional[str] = Header(None, alias="X-Correlation-Id")
):
    _require_admin(x_roles)
    row = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
    if not row: raise ProblemException(404, "Event not found")

    conn.execute("UPDATE events SET status='PUBLISHED', updated_at=? WHERE id=?", (_now_iso(), event_id))
    conn.commit()

    venue = conn.execute("SELECT * FROM venues WHERE id=?", (row["venue_id"],)).fetchone()
    env = make_envelope(TOPIC_EVENT_PUBLISHED, {
        "eventId": event_id, "title": row["title"], "startsAt": row["starts_at"],
        "venue": f'{venue["building"]}-{venue["room"]}' if venue else "",
    }, x_correlation_id or "")

    try:
        await bus.publish(TOPIC_EVENT_PUBLISHED, env)
    except Exception:
        log.exception("publish failed")
    return _event_to_dict(conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone())


@app.delete("/api/v1/admin/events/{event_id}", status_code=204)
async def cancel_event(
        event_id: str,
        x_roles: Optional[str] = Header(None, alias="X-Roles"),
        x_correlation_id: Optional[str] = Header(None, alias="X-Correlation-Id")
):
    _require_admin(x_roles)
    if not conn.execute("SELECT 1 FROM events WHERE id=?", (event_id,)).fetchone():
        raise ProblemException(404, "Event not found")

    conn.execute("UPDATE events SET status='CANCELLED', updated_at=? WHERE id=?", (_now_iso(), event_id))
    conn.commit()

    env = make_envelope(TOPIC_EVENT_CANCELLED, {"eventId": event_id, "reason": "ADMIN_CANCELLED"},
                        x_correlation_id or "")

    try:
        await bus.publish(TOPIC_EVENT_CANCELLED, env)
    except Exception:
        log.exception("publish failed")
    return None


# --- Внутренние эндпоинты ---
@app.post("/internal/v1/events/{event_id}/seats/reserve")
async def reserve_seat(
        event_id: str,
        req: ReserveSeatRequest,
        idempotency_key: str = Header(..., alias="Idempotency-Key")
):
    r_hash = request_hash(req.model_dump())
    cached = get_cached(conn, idempotency_key, r_hash)
    if cached:
        return JSONResponse(content=json.loads(cached[0]), status_code=cached[1])

    row = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
    if not row: raise ProblemException(404, "Event not found")
    if row["status"] != "PUBLISHED":
        raise ProblemException(409, "Event is not published", f"Status is {row['status']}")

    cur = conn.execute(
        "UPDATE events SET seats_reserved = seats_reserved + 1 WHERE id=? AND seats_reserved < seat_limit", (event_id,))
    if cur.rowcount == 0:
        conn.rollback()
        raise ProblemException(409, "No seats available", f"Event {event_id} has no free seats")

    now = _now_iso()
    expires_iso = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() + 900, tz=timezone.utc).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO seat_reservations (registration_id, event_id, student_id, reserved_at, expires_at) VALUES (?,?,?,?,?)",
        (req.registrationId, event_id, req.studentId, now, expires_iso))
    conn.commit()

    result = {"eventId": event_id, "registrationId": req.registrationId, "reservedAt": now, "expiresAt": expires_iso}
    save(conn, idempotency_key, r_hash, json.dumps(result), 200)
    return result


@app.post("/internal/v1/events/{event_id}/seats/release")
async def release_seat(
        event_id: str,
        req: ReleaseSeatRequest,
        idempotency_key: str = Header(..., alias="Idempotency-Key")
):
    r_hash = request_hash(req.model_dump())
    cached = get_cached(conn, idempotency_key, r_hash)
    if cached:
        return JSONResponse(content=json.loads(cached[0]), status_code=cached[1])

    reservation = conn.execute("SELECT * FROM seat_reservations WHERE registration_id=?",
                               (req.registrationId,)).fetchone()
    if not reservation: raise ProblemException(404, "Reservation not found")

    conn.execute("UPDATE events SET seats_reserved = MAX(seats_reserved - 1, 0) WHERE id=?", (event_id,))
    conn.execute("DELETE FROM seat_reservations WHERE registration_id=?", (req.registrationId,))
    conn.commit()

    result = {"eventId": event_id, "registrationId": req.registrationId, "reservedAt": reservation["reserved_at"],
              "expiresAt": reservation["expires_at"]}
    save(conn, idempotency_key, r_hash, json.dumps(result), 200)
    return result
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Header, Query
from pydantic import BaseModel

from shared.database import connect
from shared.events import TOPIC_ATTENDANCE_MARKED, TOPIC_ATTENDANCE_NO_SHOW
from shared.logging import configure_logging
from shared.problems import ProblemException, install_problem_handlers
from shared.rabbit import RabbitBus

configure_logging("profile-service")
log = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "profile.db")
conn = connect(DB_PATH)

# Инициализация БД
conn.executescript("""
CREATE TABLE IF NOT EXISTS profiles (
    id TEXT PRIMARY KEY,
    full_name TEXT NOT NULL,
    "group" TEXT NOT NULL,
    faculty TEXT,
    email TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    total_points INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS points_transactions (
    id TEXT PRIMARY KEY,
    student_id TEXT NOT NULL,
    amount INTEGER NOT NULL,
    reason TEXT NOT NULL,
    event_id TEXT,
    occurred_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    processed_at TEXT NOT NULL
);
""")
conn.commit()

# Заселяем тестовых студентов
if conn.execute("SELECT COUNT(*) FROM profiles").fetchone()[0] == 0:
    conn.executemany(
        'INSERT INTO profiles (id, full_name, "group", faculty, email, status) VALUES (?,?,?,?,?,?)',
        [
            ("u-active", "Иванов Иван Иванович", "ИС-21", "ФИВТ", "ivanov@dstu.ru", "ACTIVE"),
            ("u-active2", "Петров Пётр Петрович", "ИС-21", "ФИВТ", "petrov@dstu.ru", "ACTIVE"),
            ("u-banned", "Сидоров Сидор", "ИС-20", "ФИВТ", "sidorov@dstu.ru", "BANNED"),
        ],
    )
    conn.commit()

bus = RabbitBus()


# --- Асинхронные обработчики ---
def _award_points(student_id: str, amount: int, reason: str, event_id: str, env_id: str):
    # Идемпотентность: проверяем, не обрабатывали ли мы уже это событие
    if conn.execute("SELECT 1 FROM processed_events WHERE event_id=?", (env_id,)).fetchone():
        log.info("skip duplicate event %s", env_id)
        return
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO points_transactions (id, student_id, amount, reason, event_id, occurred_at) "
        "VALUES (?,?,?,?,?,?)",
        (str(uuid.uuid4()), student_id, amount, reason, event_id, now),
    )
    conn.execute("UPDATE profiles SET total_points = total_points + ? WHERE id=?",
                 (amount, student_id))
    conn.execute("INSERT INTO processed_events (event_id, processed_at) VALUES (?,?)",
                 (env_id, now))
    conn.commit()
    log.info("points %s for %s (%s)", amount, student_id, reason)


async def handle_attendance(topic: str, envelope: dict):
    payload = envelope.get("payload", {})
    student_id = payload.get("studentId")
    event_id = payload.get("eventId")
    env_id = envelope.get("eventId")
    if not student_id:
        return
    if topic == TOPIC_ATTENDANCE_MARKED:
        _award_points(student_id, 10, "ATTENDANCE", event_id, env_id)
    elif topic == TOPIC_ATTENDANCE_NO_SHOW:
        _award_points(student_id, -20, "NO_SHOW", event_id, env_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await bus.connect()
        await bus.subscribe(
            "profile-service",
            [TOPIC_ATTENDANCE_MARKED, TOPIC_ATTENDANCE_NO_SHOW],
            handle_attendance,
        )
    except Exception:
        log.exception("rabbit connect failed")
    yield
    await bus.close()


app = FastAPI(title="profile-service", lifespan=lifespan)
install_problem_handlers(app)


# --- Вспомогательные функции ---
def _profile_dict(row) -> dict:
    rating = conn.execute("SELECT COUNT(*) FROM profiles WHERE total_points > ?",
                          (row["total_points"],)).fetchone()[0] + 1
    return {
        "id": row["id"],
        "fullName": row["full_name"],
        "group": row["group"],
        "faculty": row["faculty"],
        "status": row["status"],
        "totalPoints": row["total_points"],
        "rating": rating,
    }


# --- Публичные эндпоинты ---
@app.get("/api/v1/profile/me")
async def get_my_profile(x_student_id: str = Header(..., alias="X-Student-Id")):
    row = conn.execute("SELECT * FROM profiles WHERE id=?", (x_student_id,)).fetchone()
    if not row:
        raise ProblemException(404, "Profile not found", f"Student {x_student_id} not found")
    return _profile_dict(row)


@app.get("/api/v1/profile/me/points")
async def list_my_points(
    x_student_id: str = Header(..., alias="X-Student-Id"),
    reason: Optional[str] = Query(None),
    page: int = Query(0, ge=0),
    size: int = Query(20, ge=1, le=100),
):
    q = "SELECT * FROM points_transactions WHERE student_id=?"
    args = [x_student_id]
    if reason:
        q += " AND reason=?"; args.append(reason)
    q += " ORDER BY occurred_at DESC"
    rows = conn.execute(q, args).fetchall()
    content = [
        {"id": r["id"], "amount": r["amount"], "reason": r["reason"],
         "eventId": r["event_id"], "occurredAt": r["occurred_at"]}
        for r in rows
    ]
    total = len(content)
    total_pages = (total + size - 1) // size if total else 0
    return {
        "content": content[page * size: (page + 1) * size],
        "page": page, "size": size, "totalElements": total, "totalPages": total_pages,
    }


# --- Внутренние эндпоинты (для других сервисов) ---
@app.get("/internal/v1/students/{student_id}/status")
async def get_student_status(student_id: str):
    row = conn.execute("SELECT id, status FROM profiles WHERE id=?", (student_id,)).fetchone()
    if not row:
        raise ProblemException(404, "Student not found")
    return {"studentId": row["id"], "status": row["status"]}


@app.get("/internal/v1/students/{student_id}")
async def get_student_full(student_id: str):
    """Полный профиль (нужен registration-service для получения email)"""
    row = conn.execute("SELECT * FROM profiles WHERE id=?", (student_id,)).fetchone()
    if not row:
        raise ProblemException(404, "Student not found")
    return {
        "id": row["id"], "fullName": row["full_name"], "group": row["group"],
        "faculty": row["faculty"], "email": row["email"], "status": row["status"],
    }
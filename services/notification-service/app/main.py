import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional
from email.message import EmailMessage

import aiosmtplib
from fastapi import FastAPI, Header, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from shared.database import connect
from shared.events import (
    TOPIC_EVENT_CANCELLED, TOPIC_EVENT_PUBLISHED,
    TOPIC_REGISTRATION_CANCELLED, TOPIC_REGISTRATION_CONFIRMED,
    TOPIC_REGISTRATION_WAITLISTED, TOPIC_REGISTRATION_WAITLIST_PROMOTED,
)
from shared.idempotency import ensure_table, get_cached, save, request_hash
from shared.logging import configure_logging
from shared.problems import ProblemException, install_problem_handlers
from shared.rabbit import RabbitBus

configure_logging("notification-service")
log = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "notification.db")
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))

conn = connect(DB_PATH)
conn.executescript("""
CREATE TABLE IF NOT EXISTS templates (code TEXT PRIMARY KEY, subject TEXT NOT NULL, body TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS notifications (
    id TEXT PRIMARY KEY, recipient_email TEXT NOT NULL, type TEXT NOT NULL, status TEXT NOT NULL,
    subject TEXT, payload_json TEXT, provider_message_id TEXT, error_message TEXT,
    retry_count INTEGER DEFAULT 0, created_at TEXT NOT NULL, sent_at TEXT, idempotency_key TEXT UNIQUE
);
CREATE TABLE IF NOT EXISTS processed_events (event_id TEXT PRIMARY KEY, processed_at TEXT NOT NULL);
""")
conn.commit()
ensure_table(conn)

TEMPLATES = [
    ("REGISTRATION_CONFIRMED", "Регистрация подтверждена", "Здравствуйте! Ваш QR: {{qrLink}}"),
    ("REGISTRATION_CANCELLED", "Регистрация отменена", "Здравствуйте! Регистрация на {{eventId}} отменена."),
]
if conn.execute("SELECT COUNT(*) FROM templates").fetchone()[0] == 0:
    conn.executemany("INSERT INTO templates (code, subject, body) VALUES (?,?,?)", TEMPLATES)
    conn.commit()

bus = RabbitBus()


def _render(template: str, params: dict) -> str:
    out = template
    for k, v in params.items():
        out = out.replace("{{" + k + "}}", str(v or ""))
    return out


async def _send_email(to: str, subject: str, body: str) -> Optional[str]:
    msg = EmailMessage()
    msg["From"] = "noreply@dstu-events.local"
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body)
    try:
        await aiosmtplib.send(msg, hostname=SMTP_HOST, port=SMTP_PORT, timeout=10)
        return "msg_" + uuid.uuid4().hex[:8]
    except Exception as e:
        log.warning("SMTP send failed: %s", e)
        return None


async def _process(notif_type: str, template_code: str, params: dict, recipient: str, payload: dict):
    row = conn.execute("SELECT * FROM templates WHERE code=?", (template_code,)).fetchone()
    if not row: return
    subject = _render(row["subject"], params)
    body = _render(row["body"], params)
    nid = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO notifications (id, recipient_email, type, status, subject, payload_json, created_at) "
        "VALUES (?,?,?,?,?,?,?)", (nid, recipient, notif_type, "QUEUED", subject, json.dumps(payload), now)
    )
    conn.commit()

    provider_id = await _send_email(recipient, subject, body)
    if provider_id:
        conn.execute("UPDATE notifications SET status='SENT', provider_message_id=?, sent_at=? WHERE id=?",
                     (provider_id, datetime.now(timezone.utc).isoformat(), nid))
    else:
        conn.execute("UPDATE notifications SET status='FAILED', error_message=? WHERE id=?", ("SMTP failed", nid))
    conn.commit()


def _mark_processed(env_id: str) -> bool:
    if conn.execute("SELECT 1 FROM processed_events WHERE event_id=?", (env_id,)).fetchone():
        return False
    conn.execute("INSERT INTO processed_events (event_id, processed_at) VALUES (?,?)",
                 (env_id, datetime.now(timezone.utc).isoformat()))
    conn.commit()
    return True


async def handle_event(topic: str, envelope: dict):
    if not _mark_processed(envelope.get("eventId", "")): return
    p = envelope.get("payload", {})
    email = p.get("studentEmail")

    if topic == TOPIC_REGISTRATION_CONFIRMED and email:
        await _process("REGISTRATION_CONFIRMED", "REGISTRATION_CONFIRMED", {"qrLink": p.get("qrToken", "")}, email, p)
    elif topic == TOPIC_REGISTRATION_CANCELLED and email:
        await _process("REGISTRATION_CANCELLED", "REGISTRATION_CANCELLED", {"eventId": p.get("eventId")}, email, p)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await bus.connect()
        await bus.subscribe("notification-service", [
            TOPIC_REGISTRATION_CONFIRMED, TOPIC_REGISTRATION_CANCELLED,
            TOPIC_REGISTRATION_WAITLISTED, TOPIC_REGISTRATION_WAITLIST_PROMOTED,
            TOPIC_EVENT_PUBLISHED, TOPIC_EVENT_CANCELLED,
        ], handle_event)
    except Exception:
        log.exception("rabbit failed")
    yield
    await bus.close()


app = FastAPI(title="notification-service", lifespan=lifespan)
install_problem_handlers(app)


class SendNotificationRequest(BaseModel):
    recipient: str
    templateCode: str
    channel: str = "EMAIL"
    params: dict


@app.get("/internal/v1/notifications")
async def list_notifications(status: Optional[str] = Query(None), page: int = Query(0, ge=0),
                             size: int = Query(20, ge=1, le=100)):
    q = "SELECT * FROM notifications WHERE 1=1"
    args = []
    if status: q += " AND status=?"; args.append(status)
    q += " ORDER BY created_at DESC"
    rows = conn.execute(q, args).fetchall()
    content = []
    for r in rows:
        content.append({
            "id": r["id"], "recipientEmail": r["recipient_email"], "type": r["type"], "status": r["status"],
            "subject": r["subject"], "payload": json.loads(r["payload_json"]) if r["payload_json"] else {},
            "providerMessageId": r["provider_message_id"], "createdAt": r["created_at"], "sentAt": r["sent_at"],
        })
    total = len(content)
    return {"content": content[page * size:(page + 1) * size], "page": page, "size": size,
            "totalElements": total, "totalPages": (total + size - 1) // size if total else 0}


@app.post("/internal/v1/notifications/send", status_code=202)
async def send_manual(req: SendNotificationRequest, idempotency_key: str = Header(..., alias="Idempotency-Key")):
    r_hash = request_hash(req.model_dump())
    cached = get_cached(conn, idempotency_key, r_hash)
    if cached:
        return JSONResponse(content=json.loads(cached[0]), status_code=cached[1])

    await _process(req.templateCode, req.templateCode, req.params, req.recipient, req.params)
    result = {"notificationId": str(uuid.uuid4()), "status": "QUEUED",
              "queuedAt": datetime.now(timezone.utc).isoformat()}
    save(conn, idempotency_key, r_hash, json.dumps(result), 202)
    return result
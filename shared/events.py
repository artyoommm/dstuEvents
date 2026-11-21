from typing import Optional
from pydantic import BaseModel, Field
from datetime import datetime, timezone

# -------- Routing keys --------
TOPIC_REGISTRATION_CONFIRMED         = "registration.confirmed"
TOPIC_REGISTRATION_CANCELLED         = "registration.cancelled"
TOPIC_REGISTRATION_WAITLISTED        = "registration.waitlisted"
TOPIC_REGISTRATION_WAITLIST_PROMOTED = "registration.waitlist_promoted"
TOPIC_ATTENDANCE_MARKED              = "attendance.marked"
TOPIC_ATTENDANCE_NO_SHOW             = "attendance.no_show"
TOPIC_EVENT_PUBLISHED                = "event.published"
TOPIC_EVENT_CANCELLED                = "event.cancelled"


# -------- Envelope --------
class EventEnvelope(BaseModel):
    eventId: str
    eventType: str
    schemaVersion: int = 1
    occurredAt: datetime
    correlationId: str
    payload: dict



def make_envelope(event_type: str, payload: dict, correlation_id: str) -> dict:
    import uuid
    return EventEnvelope(
        eventId=str(uuid.uuid4()),
        eventType=event_type,
        occurredAt=datetime.now(timezone.utc),
        correlationId=correlation_id or str(uuid.uuid4()),
        payload=payload,
    ).model_dump(mode="json")
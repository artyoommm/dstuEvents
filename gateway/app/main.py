import logging
import os
from typing import Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response

from shared.logging import configure_logging
from shared.problems import install_problem_handlers, ProblemException

configure_logging("gateway")
log = logging.getLogger(__name__)
app = FastAPI(title="gateway")
install_problem_handlers(app)

EVENT_URL = os.getenv("EVENT_SERVICE_URL", "http://localhost:8001")
REG_URL = os.getenv("REGISTRATION_SERVICE_URL", "http://localhost:8002")
ATT_URL = os.getenv("ATTENDANCE_SERVICE_URL", "http://localhost:8003")
PROFILE_URL = os.getenv("PROFILE_SERVICE_URL", "http://localhost:8004")
NOTIF_URL = os.getenv("NOTIFICATION_SERVICE_URL", "http://localhost:8005")

ROUTES = [
    ("/api/v1/events", EVENT_URL),
    ("/api/v1/admin/events", EVENT_URL),
    ("/api/v1/registrations", REG_URL),
    ("/api/v1/attendance", ATT_URL),
    ("/api/v1/profile", PROFILE_URL),
    ("/internal/v1/events", EVENT_URL),
    ("/internal/v1/registrations", REG_URL),
    ("/internal/v1/attendance", ATT_URL),
    ("/internal/v1/notifications", NOTIF_URL),
    ("/internal/v1/students", PROFILE_URL),
]

def route_for(path: str) -> Optional[str]:
    for prefix, url in ROUTES:
        if path == prefix or path.startswith(prefix + "/"):
            return url
    return None

@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(full_path: str, request: Request):
    path = "/" + full_path
    upstream = route_for(path)
    if not upstream:
        raise ProblemException(404, "Not found", f"No route for {path}")

    headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}
    body = await request.body()
    async with httpx.AsyncClient(timeout=15) as c:
        try:
            r = await c.request(request.method, upstream + path, headers=headers, content=body, params=request.query_params)
        except httpx.RequestError as e:
            raise ProblemException(503, "Upstream unavailable", str(e))

    passthrough = {k: v for k, v in r.headers.items() if k.lower() not in ("content-length", "transfer-encoding", "connection")}
    return Response(content=r.content, status_code=r.status_code, headers=passthrough)
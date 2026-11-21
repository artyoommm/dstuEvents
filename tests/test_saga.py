import importlib.util
import os
import sys

import httpx
import pytest
import respx
from httpx import ASGITransport, AsyncClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_reg_app():
    path = os.path.join(ROOT, "services", "registration-service", "app", "main.py")
    name = "reg_app_under_test"
    if name in sys.modules:
        del sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.asyncio
@respx.mock
async def test_saga_happy_path(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "reg.db"))
    monkeypatch.setenv("EVENT_SERVICE_URL", "http://event.test")
    monkeypatch.setenv("PROFILE_SERVICE_URL", "http://profile.test")

    # Мокаем ответы других сервисов
    respx.get("http://profile.test/internal/v1/students/u1").mock(
        return_value=httpx.Response(200, json={"id": "u1", "status": "ACTIVE", "email": "u1@dstu.ru"}))
    respx.post("http://event.test/internal/v1/events/e1/seats/reserve").mock(
        return_value=httpx.Response(200, json={"ok": True}))
    respx.post("http://event.test/internal/v1/events/e1/seats/release").mock(
        return_value=httpx.Response(200, json={"ok": True}))

    mod = _load_reg_app()

    class FakeBus:
        async def publish(self, *a, **kw): pass
        async def connect(self, *a, **kw): pass
        async def subscribe(self, *a, **kw): pass
        async def close(self): pass

    mod.bus = FakeBus()

    async with AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as c:
        r = await c.post("/api/v1/registrations",
                         headers={"X-Student-Id": "u1", "Idempotency-Key": "reg-1"},
                         json={"eventId": "e1"})
        assert r.status_code == 201, r.text
        assert r.json()["status"] == "CONFIRMED"


@pytest.mark.asyncio
@respx.mock
async def test_saga_banned_student(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "reg.db"))
    monkeypatch.setenv("EVENT_SERVICE_URL", "http://event.test")
    monkeypatch.setenv("PROFILE_SERVICE_URL", "http://profile.test")

    respx.get("http://profile.test/internal/v1/students/u-banned").mock(
        return_value=httpx.Response(200, json={"id": "u-banned", "status": "BANNED", "email": "b@dstu.ru"}))

    mod = _load_reg_app()
    class FakeBus:
        async def publish(self, *a, **kw): pass
        async def connect(self, *a, **kw): pass
        async def subscribe(self, *a, **kw): pass
        async def close(self): pass
    mod.bus = FakeBus()

    async with AsyncClient(transport=ASGITransport(app=mod.app), base_url="http://test") as c:
        r = await c.post("/api/v1/registrations",
                         headers={"X-Student-Id": "u-banned", "Idempotency-Key": "reg-2"},
                         json={"eventId": "e1"})
        assert r.status_code == 403
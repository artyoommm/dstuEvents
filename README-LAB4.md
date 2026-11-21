# DSTU Events — ЛР4 (Реализация микросервисов)

## Стек
Python 3.11 + FastAPI + SQLite (вместо PostgreSQL/MongoDB/Redis) + RabbitMQ + Mailpit.
Gateway — FastAPI reverse-proxy. Keycloak не поднимается — Gateway пробрасывает X-Student-Id.

## Замокано (по отношению к ЛР3)
| ЛР3 | ЛР4 |
|---|---|
| PostgreSQL | SQLite-файл на сервис |
| MongoDB | SQLite |
| Redis (TTL, idempotency, dedup сканов) | таблицы в SQLite |
| Kong | FastAPI proxy |
| Keycloak | заголовки X-Student-Id |
| RabbitMQ | реальный RabbitMQ (docker compose) |
| Mailpit | реальный Mailpit (docker compose) |

## Сервисы
| Сервис | Порт |
|---|---|
| gateway | 8000 |
| event-service | 8001 |
| registration-service | 8002 |
| attendance-service | 8003 |
| profile-service | 8004 |
| notification-service | 8005 |
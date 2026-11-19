import asyncio
import json
import logging
import os
import uuid
from typing import Awaitable, Callable

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message

log = logging.getLogger(__name__)

Handler = Callable[[str, dict], Awaitable[None]]


class RabbitBus:
    def __init__(self, amqp_url: str | None = None, exchange_name: str | None = None):
        self.amqp_url = amqp_url or os.getenv("AMQP_URL", "amqp://dstu:dstu@localhost:5672/")
        self.exchange_name = exchange_name or os.getenv("EXCHANGE_NAME", "dstu.events")
        self._conn = None
        self._channel = None
        self._exchange = None
        self._consumers = []

    async def connect(self, retries: int = 30, delay: float = 1.5):
        for attempt in range(retries):
            try:
                self._conn = await aio_pika.connect_robust(self.amqp_url)
                self._channel = await self._conn.channel()
                await self._channel.set_qos(prefetch_count=10)
                self._exchange = await self._channel.declare_exchange(
                    self.exchange_name, ExchangeType.TOPIC, durable=True
                )
                log.info("connected to rabbitmq exchange=%s", self.exchange_name)
                return
            except Exception as e:
                log.warning("rabbit connect attempt %d: %s", attempt + 1, e)
                await asyncio.sleep(delay)
        raise RuntimeError("rabbitmq unreachable")

    async def publish(self, topic: str, payload: dict):
        msg = Message(
            body=json.dumps(payload, default=str).encode(),
            delivery_mode=DeliveryMode.PERSISTENT,
            content_type="application/json",
            message_id=str(uuid.uuid4()),
        )
        await self._exchange.publish(msg, routing_key=topic)
        log.info("published %s", topic)

    async def subscribe(self, queue_prefix: str, patterns: list[str], handler: Handler):
        queue = await self._channel.declare_queue(f"{queue_prefix}.queue", durable=True)
        for p in patterns:
            await queue.bind(self._exchange, routing_key=p)

        async def _cb(message: aio_pika.IncomingMessage):
            async with message.process(requeue=False):
                try:
                    payload = json.loads(message.body)
                    await handler(message.routing_key, payload)
                except Exception:
                    log.exception("handler failed for %s", message.routing_key)

        consumer = await queue.consume(_cb)
        self._consumers.append(consumer)
        log.info("subscribed %s to %s", queue_prefix, patterns)

    async def close(self):
        for c in self._consumers:
            try:
                await c.cancel()
            except Exception:
                pass
        if self._conn:
            await self._conn.close()
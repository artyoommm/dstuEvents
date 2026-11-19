import asyncio
from shared.rabbit import RabbitBus


async def main():
    bus = RabbitBus()
    await bus.connect()
    received = asyncio.Event()

    async def handler(topic, payload):
        print("got:", topic, payload)
        received.set()

    await bus.subscribe("smoke", ["event.*"], handler)
    await bus.publish("event.published",
                      {"eventId": "e1", "title": "T", "startsAt": "2026-12-01T10:00:00Z", "venue": "8-401"})
    await asyncio.wait_for(received.wait(), timeout=10)
    await bus.close()
    print("OK")


if __name__ == "__main__":
    asyncio.run(main())
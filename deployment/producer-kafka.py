from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json
import time
from datetime import datetime


BOOTSTRAP_SERVERS="192.168.1.15:29092" #localhost:9092,
TOPIC = "telemetry"
TOPIC_EVENTS = "events"

async def producer_task():
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS
    )

    await producer.start()

    try:
        while True:
            message = {
                "tag" : "pressure",
                "timestamp": datetime.now().isoformat(),
                "value": round(time.time() % 120, 2)
            }

            await producer.send_and_wait(
                TOPIC,
                json.dumps(message).encode()
            )

            print("[PRODUCER]", message)

            await asyncio.sleep(1)

    finally:
        await producer.stop()


async def consumer_task():
    consumer = AIOKafkaConsumer(
        TOPIC_EVENTS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="monitor",
        auto_offset_reset="earliest"
    )

    await consumer.start()

    try:
        async for msg in consumer:
            print("[CONSUMER]", json.loads(msg.value))

    finally:
        await consumer.stop()


async def main():
    await asyncio.gather(
        producer_task(),
        consumer_task()
    )


if __name__ == "__main__":
    asyncio.run(main())
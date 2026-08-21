from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import json
import time
from datetime import datetime

TOPIC = "sample-topic"


async def producer_task():
    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9092"
    )

    await producer.start()

    try:
        while True:
            message = {
                "tag" : "pressure",
                "timestamp": datetime.now().isoformat(),
                "value": round(time.time() % 100, 2)
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
        TOPIC,
        bootstrap_servers="localhost:9092",
        group_id="my-group",
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
        #consumer_task()
    )


if __name__ == "__main__":
    asyncio.run(main())
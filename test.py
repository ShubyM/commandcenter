import asyncio
from typing import Optional

import aiormq
from aiormq.abc import DeliveredMessage
from pamqp.commands import Basic

MESSAGE: Optional[DeliveredMessage] = None


async def main():
    global MESSAGE

    body = b'Hello World!'

    # Perform connection
    connection = await aiormq.connect("amqp://guest:guest@localhost//")

    # Creating a channel
    channel = await connection.channel()

    declare_ok = await channel.queue_declare("hello", auto_delete=True)

    # Sending the message
    confirm = await channel.basic_publish(body, routing_key='hello')
    print(type(confirm))

    print(isinstance(confirm, Basic.Ack))
    print(f" [x] Sent {body}")

    MESSAGE = await channel.basic_get(declare_ok.queue)
    print(f" [x] Received message from {declare_ok.queue!r}")


loop = asyncio.get_event_loop()
loop.run_until_complete(main())

assert MESSAGE is not None
assert MESSAGE.routing_key == "hello"
assert MESSAGE.body == b'Hello World!'
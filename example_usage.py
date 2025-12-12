"""
Example usage of worker_client Producer and Consumer with full type safety.

This demonstrates the architecture:
1. Producer: Encoder converts typed object T -> JSON string -> Redis
2. Consumer: Redis -> JSON bytes -> Decoder -> typed object T -> Handler
"""

import json
from dataclasses import dataclass
from typing import Any
from worker_client import Consumer, Producer


@dataclass
class MyMessage:
    """Example message type - can be any dataclass or class"""

    task_id: str
    action: str
    data: dict[str, Any]


def my_encoder(message: MyMessage) -> str:
    """
    Encoder function that converts typed MyMessage to JSON string.

    Args:
        message: Typed MyMessage object

    Returns:
        JSON string to store in Redis payload
    """
    return json.dumps(
        {"task_id": message.task_id, "action": message.action, "data": message.data}
    )


def my_decoder(raw: bytes) -> MyMessage:
    """
    Decoder function that converts JSON bytes to typed MyMessage.

    Args:
        raw: JSON string encoded as UTF-8 bytes (from Redis payload)

    Returns:
        MyMessage: Fully typed message object
    """
    # Decode bytes to JSON
    json_data = json.loads(raw.decode("utf-8"))

    # Convert to typed object
    return MyMessage(
        task_id=json_data["task_id"],
        action=json_data["action"],
        data=json_data.get("data", {}),
    )


def my_handler(message: MyMessage) -> None:
    """
    Handler that processes the typed message.

    Args:
        message: Fully typed MyMessage object (type checker knows the fields)
    """
    print(f"Processing task: {message.task_id}")
    print(f"Action: {message.action}")
    print(f"Data: {message.data}")

    # ✅ Type checker knows all fields!
    # ✅ Autocomplete works!
    # ✅ Refactoring is safe!


def producer_example() -> None:
    """Example of using Producer to send messages"""

    # Create a typed producer
    # The Producer[MyMessage] tells the type checker that:
    # - encoder must accept MyMessage
    producer: Producer[MyMessage] = Producer(encoder=my_encoder)

    # Send a typed message
    message = MyMessage(task_id="task-123", action="process", data={"key": "value"})

    message_id = producer.send(
        message=message,
        log_stream_key="worker:logs:task-123",
        output_stream_key="worker:output:task-123",
        remote_resource_id="resource-456",
    )

    print(f"Sent message with ID: {message_id}")
    producer.close()


def consumer_example() -> None:
    """Example of using Consumer to receive and process messages"""

    # Create a typed consumer
    # The Consumer[MyMessage] tells the type checker that:
    # - decoder must return MyMessage
    # - handler must accept MyMessage
    consumer: Consumer[MyMessage] = Consumer(decoder=my_decoder, handler=my_handler)

    # Run the consumer loop
    # This will:
    # 1. Get JSON string from Redis payload field
    # 2. Encode as bytes and pass to decoder
    # 3. Decoder returns MyMessage
    # Steps:
    # 1. Consumer waits for message from Redis
    # 2. Message payload (JSON string) is retrieved
    # 3. Decoder converts bytes -> MyMessage
    # 4. Handler receives MyMessage
    # 5. Job is acknowledged
    print("Starting consumer...")
    consumer.run()


if __name__ == "__main__":
    # Uncomment one to run:
    # producer_example()  # Send a message
    consumer_example()  # Receive and process messages

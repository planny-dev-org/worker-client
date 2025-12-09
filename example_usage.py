"""
Example usage of worker_client with full type safety.

This demonstrates the architecture:
1. Redis stores JSON strings in the payload field
2. Decoder receives bytes (JSON encoded as UTF-8) and returns typed object T
3. Handler receives the fully typed object T
"""

import json
from dataclasses import dataclass
from typing import Any
from worker_client import Consumer


@dataclass
class MyMessage:
    """Example message type - can be any dataclass or class"""
    task_id: str
    action: str
    data: dict[str, Any]


def my_decoder(raw: bytes) -> MyMessage:
    """
    Decoder function that converts JSON bytes to typed MyMessage.
    
    Args:
        raw: JSON string encoded as UTF-8 bytes (from Redis payload)
        
    Returns:
        MyMessage: Fully typed message object
    """
    # Decode bytes to JSON
    json_data = json.loads(raw.decode('utf-8'))
    
    # Convert to typed object
    return MyMessage(
        task_id=json_data['task_id'],
        action=json_data['action'],
        data=json_data.get('data', {})
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


def main() -> None:
    """Main entry point - set up and run the consumer"""
    
    # Create a typed consumer
    # The Consumer[MyMessage] tells the type checker that:
    # - decoder must return MyMessage
    # - handler must accept MyMessage
    consumer: Consumer[MyMessage] = Consumer(
        decoder=my_decoder,
        handler=my_handler
    )
    
    # Run the consumer loop
    # This will:
    # 1. Get JSON string from Redis payload field
    # 2. Encode as bytes and pass to decoder
    # 3. Decoder returns MyMessage
    # 4. Handler receives MyMessage
    # 5. Acknowledge the job
    print("Starting consumer...")
    consumer.run()


if __name__ == "__main__":
    main()

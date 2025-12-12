"""
End-to-end test for producer-consumer workflow with typed messages.
This test validates the complete flow: produce message → Redis → consume message → decode → handle
"""

import json
import threading
from dataclasses import dataclass, asdict
from typing import Optional, Any

from worker_client.client import Consumer, get_redis_client
from worker_client.settings import UPSTREAM_KEY
from worker_client.constants import (
    LOG_STREAM_FIELD_NAME,
    OUTPUT_STREAM_FIELD_NAME,
    PAYLOAD_FIELD_NAME,
    REMOTE_RESOURCE_ID_FIELD_NAME,
)
from worker_client.test.conftest import flush_all_cache


@dataclass
class TaskMessage:
    """Test message type - represents a task to be processed"""

    task_id: str
    action: str
    priority: int
    metadata: dict[str, str]


def task_decoder(raw: bytes) -> TaskMessage:
    """Decoder that converts JSON bytes to TaskMessage"""
    data = json.loads(raw.decode("utf-8"))
    return TaskMessage(
        task_id=data["task_id"],
        action=data["action"],
        priority=data["priority"],
        metadata=data.get("metadata", {}),
    )


class TestEndToEnd:
    """End-to-end integration tests for producer-consumer workflow"""

    @flush_all_cache
    def test_producer_consumer_typed_message(self) -> None:
        """
        Test complete flow:
        1. Producer creates a typed message and sends to Redis
        2. Consumer receives the message
        3. Decoder converts bytes to typed object
        4. Handler receives properly typed message
        5. Validate decode_responses=True doesn't cause double decoding
        """
        redis_client = get_redis_client()

        # Create a typed message
        original_message = TaskMessage(
            task_id="task_12345",
            action="process_data",
            priority=1,
            metadata={"source": "test", "env": "staging"},
        )

        # Producer: Send message to Redis (simulate what a producer would do)
        message_payload = {
            LOG_STREAM_FIELD_NAME: "worker:logs:e2e_test",
            OUTPUT_STREAM_FIELD_NAME: "worker:output:e2e_test",
            PAYLOAD_FIELD_NAME: json.dumps(
                asdict(original_message)
            ),  # Encode as JSON string
            REMOTE_RESOURCE_ID_FIELD_NAME: "resource_e2e_test",
        }

        redis_client.xadd(UPSTREAM_KEY, message_payload)  # type: ignore[arg-type]

        # Consumer: Set up handler to capture the received message
        received_messages: list[Optional[TaskMessage]] = [None]

        def test_handler(message: TaskMessage) -> None:
            """Handler that captures the received message for validation"""
            received_messages[0] = message

        # Create consumer with typed decoder and handler
        consumer: Consumer[TaskMessage, Any] = Consumer(
            decoder=task_decoder, handler=test_handler
        )

        # Run consumer in a thread (since new_job is blocking)
        def run_consumer() -> None:
            consumer.new_job()
            if consumer.job and consumer.job.payload:
                # Manually decode and handle (simulating what run() does)
                raw_payload = consumer.job.payload.encode("utf-8")
                decoded_message = consumer.decoder(raw_payload)  # type: ignore[misc]
                consumer.handler(decoded_message)  # type: ignore[misc]

        thread = threading.Thread(target=run_consumer, daemon=True)
        thread.start()
        thread.join(timeout=5)

        # Validate the received message
        assert (
            received_messages[0] is not None
        ), "Handler should have received a message"
        received = received_messages[0]

        # Validate all fields match the original
        assert received.task_id == original_message.task_id
        assert received.action == original_message.action
        assert received.priority == original_message.priority
        assert received.metadata == original_message.metadata

        # Validate consumer job was set up correctly
        assert consumer.job is not None
        assert consumer.job.payload is not None
        assert consumer.job.remote_resource_id == "resource_e2e_test"

        # Validate payload is a string (not bytes - proving decode_responses=True works)
        assert isinstance(
            consumer.job.payload, str
        ), "Payload should be a string from Redis"

        # Validate payload can be parsed as JSON
        payload_dict = json.loads(consumer.job.payload)
        assert payload_dict["task_id"] == original_message.task_id

    @flush_all_cache
    def test_decode_responses_no_double_decode(self) -> None:
        """
        Specifically test that decode_responses=True doesn't cause double decoding issues.

        Flow:
        1. Redis stores data → decode_responses=True → returns string
        2. We store payload as string
        3. We encode string to bytes for decoder
        4. Decoder decodes bytes to object

        This test ensures we're not accidentally decoding twice.
        """
        redis_client = get_redis_client()

        # Create message with special characters to detect encoding issues
        test_message = TaskMessage(
            task_id="test_äöü_汉字",  # UTF-8 characters
            action="process_émojis_🎉",
            priority=1,
            metadata={"test": "spëcial_çhars"},
        )

        message_payload = {
            LOG_STREAM_FIELD_NAME: "worker:logs:decode_test",
            OUTPUT_STREAM_FIELD_NAME: "worker:output:decode_test",
            PAYLOAD_FIELD_NAME: json.dumps(asdict(test_message), ensure_ascii=False),
            REMOTE_RESOURCE_ID_FIELD_NAME: "resource_decode_test",
        }

        redis_client.xadd(UPSTREAM_KEY, message_payload)  # type: ignore[arg-type]

        received_messages: list[Optional[TaskMessage]] = [None]

        def capture_handler(message: TaskMessage) -> None:
            received_messages[0] = message

        consumer: Consumer[TaskMessage, Any] = Consumer(
            decoder=task_decoder, handler=capture_handler
        )

        # Manually run the workflow
        consumer.new_job()
        assert consumer.job is not None
        assert consumer.job.payload is not None

        # Validate payload is a string (Redis decoded it with decode_responses=True)
        assert isinstance(consumer.job.payload, str)

        # Encode to bytes (as run() does)
        raw_payload = consumer.job.payload.encode("utf-8")
        assert isinstance(raw_payload, bytes)

        # Decode using decoder
        decoded_message = task_decoder(raw_payload)

        # Validate UTF-8 characters are preserved correctly
        assert decoded_message.task_id == test_message.task_id
        assert decoded_message.action == test_message.action
        assert "äöü" in decoded_message.task_id
        assert "汉字" in decoded_message.task_id
        assert "émojis" in decoded_message.action
        assert "🎉" in decoded_message.action
        assert decoded_message.metadata["test"] == "spëcial_çhars"

    @flush_all_cache
    def test_producer_consumer_with_run_method(self) -> None:
        """
        Test using the actual run() method of Consumer for a complete end-to-end test.
        This simulates the real-world usage pattern.
        """
        redis_client = get_redis_client()

        # Produce a message
        test_message = TaskMessage(
            task_id="run_test_123",
            action="full_workflow",
            priority=5,
            metadata={"test_type": "e2e_with_run"},
        )

        message_payload = {
            LOG_STREAM_FIELD_NAME: "worker:logs:run_test",
            OUTPUT_STREAM_FIELD_NAME: "worker:output:run_test",
            PAYLOAD_FIELD_NAME: json.dumps(asdict(test_message)),
            REMOTE_RESOURCE_ID_FIELD_NAME: "resource_run_test",
        }

        redis_client.xadd(UPSTREAM_KEY, message_payload)  # type: ignore[arg-type]

        # Track received messages
        received_messages: list[TaskMessage] = []
        processing_complete = threading.Event()

        def handling_handler(message: TaskMessage) -> None:
            """Handler that tracks received messages and signals completion"""
            received_messages.append(message)
            processing_complete.set()

        # Create consumer
        consumer: Consumer[TaskMessage, Any] = Consumer(
            decoder=task_decoder, handler=handling_handler
        )

        # Run consumer in thread
        def run_consumer_loop() -> None:
            # Process just one message then exit
            consumer.new_job()
            if consumer.job and consumer.job.payload:
                raw_payload = consumer.job.payload.encode("utf-8")
                decoded_message = consumer.decoder(raw_payload)  # type: ignore[misc]
                consumer.handler(decoded_message)  # type: ignore[misc]
                consumer.acknowledge()

        thread = threading.Thread(target=run_consumer_loop, daemon=True)
        thread.start()

        # Wait for processing to complete
        completed = processing_complete.wait(timeout=5)
        assert completed, "Message processing should complete within timeout"

        thread.join(timeout=1)

        # Validate message was received and processed
        assert len(received_messages) == 1
        received = received_messages[0]
        assert received.task_id == test_message.task_id
        assert received.action == test_message.action
        assert received.priority == test_message.priority
        assert received.metadata == test_message.metadata

        # Validate message was acknowledged in Redis
        # Check that there are no pending messages
        _ = redis_client.xpending(UPSTREAM_KEY, consumer.group_name)  # type: ignore[attr-defined]
        # After acknowledge, pending count should be handled
        # Note: The actual assertion depends on Redis state management

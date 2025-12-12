"""Quick stub validation test - verifies stubs expose correct types."""

from typing import Any

from worker_client import (
    Consumer,
    Producer,
    ConsumerJob,
    Message,
    Decoder,
    Encoder,
    get_redis_client,
    LEVEL_LITERAL,
    MSG_TYPE_LITERAL,
)


def test_stub_imports() -> None:
    """Verify all exports are accessible and typed."""
    # This test just verifies the stubs are complete
    # If this file type-checks, the stubs are correct
    
    # Check that generic types work
    def my_decoder(raw: bytes) -> str:
        return raw.decode()
    
    def my_encoder(msg: str) -> str:
        return msg
    
    def my_handler(msg: str) -> None:
        print(msg)
    
    # These should all type-check
    consumer: Consumer[str, Any] = Consumer(decoder=my_decoder, handler=my_handler)
    producer: Producer[str] = Producer(encoder=my_encoder)
    
    # Check that functions exist
    redis_client = get_redis_client()
    
    # Check that classes are instantiable
    job = ConsumerJob(
        message_id="123",
        reply_log_stream_key="log",
        reply_output_stream_key="output",
        remote_resource_id="resource",
        payload='{"key": "value"}',
    )
    
    msg = Message(message="test", message_type="LOG", level="INFO")
    
    print("✅ All stub types validated!")


if __name__ == "__main__":
    test_stub_imports()

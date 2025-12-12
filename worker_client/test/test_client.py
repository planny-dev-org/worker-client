import datetime
from typing import Optional, Any
import logging
import threading
import time

from worker_client.client import Consumer, get_redis_client
from worker_client.settings import UPSTREAM_KEY
from worker_client.constants import (
    LOG_STREAM_FIELD_NAME,
    OUTPUT_STREAM_FIELD_NAME,
)
from worker_client.test.conftest import flush_all_cache


LOG = logging.getLogger(__name__)


def run_new_job(results: list[Optional[Consumer[Any, Any]]]) -> None:
    """
    Intended for use in a thread
    Run consumer new_job since this call is blocking and can loop indefinitely if job data is wrong
    """
    consumer: Consumer[Any, Any] = Consumer()
    consumer.new_job()
    results[0] = consumer


class TestClient:

    @flush_all_cache
    def test_payload_wrong_type(self, job_message_str_payload: dict[str, str]) -> None:
        """
        Test that validates a JSON string payload is accepted (no longer rejected).
        The new architecture accepts any valid JSON, not just objects.
        """
        redis_client = get_redis_client()
        redis_client.xadd(UPSTREAM_KEY, job_message_str_payload)  # type: ignore[arg-type]

        thread = threading.Thread(target=run_new_job, args=([None],), daemon=True)
        thread.start()

        time.sleep(1)  # give some time to the worker to process the message

        # assert first message received is the worker signal that message has been received
        messages = redis_client.xread(  # type: ignore[assignment]
            streams={job_message_str_payload[LOG_STREAM_FIELD_NAME]: 0}, count=1
        )

        # The message should be accepted now since it's valid JSON
        assert len(messages) > 0, "Expected at least one message in the stream"  # type: ignore[arg-type]
        _, stream_messages = messages[0]  # type: ignore[misc]
        assert len(stream_messages) > 0, "Expected at least one stream message"  # type: ignore[arg-type]
        _, message_data = stream_messages[0]  # type: ignore[misc]
        assert "message" in message_data
        assert "received by worker_client_consumer" in message_data["message"]

    @flush_all_cache
    def test_payload_wrong_encoding(
        self, job_message_unencoded_payload: dict[str, str]
    ) -> None:

        redis_client = get_redis_client()
        redis_client.xadd(UPSTREAM_KEY, job_message_unencoded_payload)  # type: ignore[arg-type]

        thread = threading.Thread(target=run_new_job, args=([None],), daemon=True)
        thread.start()

        time.sleep(1)

        # assert first message received is the worker signal that message has been received
        messages = redis_client.xread(  # type: ignore[assignment]
            streams={job_message_unencoded_payload[LOG_STREAM_FIELD_NAME]: 0}, count=1
        )

        _, stream_messages = messages[0]  # type: ignore[misc]
        first_message_id, message_data = stream_messages[0]  # type: ignore[misc]
        assert "message" in message_data
        assert "received by worker_client_consumer" in message_data["message"]

        # assert second message received is the worker signal that message data type is wrong been received
        messages = redis_client.xread(  # type: ignore[assignment]
            streams={job_message_unencoded_payload[LOG_STREAM_FIELD_NAME]: first_message_id},  # type: ignore[dict-item]
            count=1,
        )
        _, stream_messages = messages[0]  # type: ignore[misc]
        _, message_data = stream_messages[0]  # type: ignore[misc]
        assert "message" in message_data
        assert (
            "unable to decode payload field from message id " in message_data["message"]
        )

    @flush_all_cache
    def test_payload_ok(self, job_message_ok: dict[str, str]) -> None:
        redis_client = get_redis_client()
        redis_client.xadd(UPSTREAM_KEY, job_message_ok)  # type: ignore[arg-type]

        results: list[Optional[Consumer[Any, Any]]] = [None]
        thread = threading.Thread(target=run_new_job, args=(results,), daemon=True)
        thread.start()
        thread.join(timeout=10)

        assert results[0] is not None

        # assert first message received is the worker signal that message has been received
        messages = redis_client.xread(  # type: ignore[assignment]
            streams={job_message_ok[LOG_STREAM_FIELD_NAME]: 0}, count=1
        )

        _, stream_messages = messages[0]  # type: ignore[misc]
        _, message_data = stream_messages[0]  # type: ignore[misc]
        assert "message" in message_data
        assert "received by worker_client_consumer" in message_data["message"]

        # assert consumer instance have a job attached now
        consumer: Optional[Consumer[Any, Any]] = results[0]
        assert consumer is not None
        assert consumer.job is not None

    @flush_all_cache
    def test_issue_messages(self, job_message_ok: dict[str, str]) -> None:
        # get a valid job
        redis_client = get_redis_client()
        redis_client.xadd(UPSTREAM_KEY, job_message_ok)  # type: ignore[arg-type]

        results: list[Optional[Consumer[Any, Any]]] = [None]
        thread = threading.Thread(target=run_new_job, args=(results,), daemon=True)
        thread.start()
        thread.join(timeout=10)

        assert results[0] is not None

        # assert first message received is the worker signal that message has been received
        messages = redis_client.xread(  # type: ignore[assignment]
            streams={job_message_ok[LOG_STREAM_FIELD_NAME]: 0}, count=1
        )

        _, stream_messages = messages[0]  # type: ignore[misc]
        message_id, message_data = stream_messages[0]  # type: ignore[misc]
        assert "message" in message_data
        assert "received by worker_client_consumer" in message_data["message"]

        # assert consumer instance have a job attached now
        consumer: Optional[Consumer[Any, Any]] = results[0]
        assert consumer is not None
        assert consumer.job is not None

        #######################
        # issue a log statement
        #######################
        consumer.log(message="this is a test log message", level="INFO")
        time.sleep(1)  # give some time to redis to register the message

        # assert log message received
        messages = redis_client.xread(  # type: ignore[assignment]
            streams={job_message_ok[LOG_STREAM_FIELD_NAME]: message_id},
            count=1,
            block=100,
        )
        _, stream_messages = messages[0]  # type: ignore[misc]
        message_id, message_data = stream_messages[0]  # type: ignore[misc]
        assert "timestamp" in message_data
        assert "level" in message_data
        assert "message" in message_data
        timestamp = datetime.datetime.fromisoformat(message_data["timestamp"])
        assert timestamp <= datetime.datetime.now()
        assert message_data["level"] == "INFO"
        assert message_data["message"] == "this is a test log message"

        ########################
        # issue an output result
        ########################
        consumer.output(message={"result_key": "result_value"})
        time.sleep(1)  # give some time to redis to register the message

        # assert output message received
        messages = redis_client.xread(  # type: ignore[assignment]
            streams={job_message_ok[OUTPUT_STREAM_FIELD_NAME]: message_id},
            count=1,
            block=100,
        )
        _, stream_messages = messages[0]  # type: ignore[misc]
        message_id, message_data = stream_messages[0]  # type: ignore[misc]
        assert "timestamp" in message_data
        assert "level" in message_data
        assert "message" in message_data
        # redis does not support 2+ level of deepness for dict, message has been encoded by consumer
        assert message_data["message"] == '{"result_key": "result_value"}'

        # acknowledge message
        consumer.acknowledge()

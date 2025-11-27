from typing import List, Optional
import logging
import threading
import time

from worker_client.client import Consumer, get_redis_client
from worker_client.settings import UPSTREAM_KEY
from worker_client.constants import (
    PAYLOAD_FIELD_NAME,
    REMOTE_RESOURCE_ID_FIELD_NAME,
    LOG_STREAM_FIELD_NAME,
    OUTPUT_STREAM_FIELD_NAME,
)
from worker_client.test.conftest import flush_all_cache


LOG = logging.getLogger(__name__)


def run_new_job(results: List) -> None:
    """
    Intended for use in a thread
    Run consumer new_job since this call is blocking and can loop indefinitely if job data is wrong
    """
    consumer = Consumer()
    consumer.new_job()
    results[0] = consumer


class TestClient:

    @flush_all_cache
    def test_payload_wrong_type(self, job_message_str_payload: dict) -> None:

        redis_client = get_redis_client()
        redis_client.xadd(UPSTREAM_KEY, job_message_str_payload)

        thread = threading.Thread(target=run_new_job, args=([None],), daemon=True)
        thread.start()

        time.sleep(1)  # give some time to the worker to process the message

        # assert first message received is the worker signal that message has been received
        messages = redis_client.xread(
            streams={job_message_str_payload[LOG_STREAM_FIELD_NAME]: 0}, count=1
        )

        stream_key, stream_messages = messages[0]
        message_id, message_data = stream_messages[0]
        assert "message" in message_data
        assert "received by worker_client_consumer" in message_data["message"]

        # assert second message received is the worker signal that message data type is wrong been received
        messages = redis_client.xread(
            streams={job_message_str_payload[LOG_STREAM_FIELD_NAME]: message_id},
            count=1,
        )
        stream_key, stream_messages = messages[0]
        message_id, message_data = stream_messages[0]
        assert "message" in message_data
        assert "expected a dict as message data for message" in message_data["message"]

    @flush_all_cache
    def test_payload_wrong_encoding(self, job_message_unencoded_payload: dict) -> None:

        redis_client = get_redis_client()
        redis_client.xadd(UPSTREAM_KEY, job_message_unencoded_payload)

        thread = threading.Thread(target=run_new_job, args=([None],), daemon=True)
        thread.start()

        time.sleep(1)

        # assert first message received is the worker signal that message has been received
        messages = redis_client.xread(
            streams={job_message_unencoded_payload[LOG_STREAM_FIELD_NAME]: 0}, count=1
        )

        stream_key, stream_messages = messages[0]
        message_id, message_data = stream_messages[0]
        assert "message" in message_data
        assert "received by worker_client_consumer" in message_data["message"]

        # assert second message received is the worker signal that message data type is wrong been received
        messages = redis_client.xread(
            streams={job_message_unencoded_payload[LOG_STREAM_FIELD_NAME]: message_id},
            count=1,
        )
        stream_key, stream_messages = messages[0]
        message_id, message_data = stream_messages[0]
        assert "message" in message_data
        assert (
            "unable to decode payload field from message id " in message_data["message"]
        )

    @flush_all_cache
    def test_payload_ok(self, job_message_ok: dict) -> None:
        redis_client = get_redis_client()
        redis_client.xadd(UPSTREAM_KEY, job_message_ok)

        results = [None]
        thread = threading.Thread(target=run_new_job, args=(results,), daemon=True)
        thread.start()
        thread.join(timeout=10)

        assert results[0] is not None

        # assert first message received is the worker signal that message has been received
        messages = redis_client.xread(
            streams={job_message_ok[LOG_STREAM_FIELD_NAME]: 0}, count=1
        )

        stream_key, stream_messages = messages[0]
        message_id, message_data = stream_messages[0]
        assert "message" in message_data
        assert "received by worker_client_consumer" in message_data["message"]

        # assert consumer instance have a job attached now
        consumer: Optional[Consumer] = results[0]
        assert consumer.job is not None

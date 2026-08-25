"""Structured events go to their own stream, and degrade to the log stream.

Logs and events have different readers -- people read logs, the backend consumes
events to drive state -- so they get separate streams. The event stream is optional
in the upstream message: a backend that does not send one must keep working, with
events falling back to the log stream rather than being dropped.
"""

import json
import logging
import threading
from typing import Any, Optional

from worker_client.client import Consumer, ConsumerJob, get_redis_client
from worker_client.constants import EVENT_STREAM_FIELD_NAME, LOG_STREAM_FIELD_NAME
from worker_client.settings import UPSTREAM_KEY
from worker_client.test.conftest import flush_all_cache

LOG = logging.getLogger(__name__)


def run_new_job(results: list[Optional[Consumer[Any, Any]]]) -> None:
    consumer: Consumer[Any, Any] = Consumer()
    consumer.new_job()
    results[0] = consumer


def _entries(key: str) -> list[dict[str, Any]]:
    """Every message on a stream, oldest first, with fields decoded."""
    redis_client = get_redis_client()
    raw: Any = redis_client.xrange(key)  # type: ignore[attr-defined]
    out: list[dict[str, Any]] = []
    for _msg_id, fields in raw:
        out.append(
            {
                (k.decode() if isinstance(k, bytes) else k): (
                    v.decode() if isinstance(v, bytes) else v
                )
                for k, v in fields.items()
            }
        )
    return out


class TestEventStreamKeyResolution:
    """The fallback lives on ConsumerJob, so no call site has to think about it."""

    def test_event_stream_used_when_supplied(self) -> None:
        job = ConsumerJob(
            message_id="1-1",
            reply_log_stream_key="logs",
            reply_event_stream_key="events",
        )
        assert job.event_stream_key == "events"

    def test_falls_back_to_log_stream_when_absent(self) -> None:
        job = ConsumerJob(message_id="1-1", reply_log_stream_key="logs")
        assert job.reply_event_stream_key is None
        assert job.event_stream_key == "logs"

    def test_falls_back_when_supplied_empty(self) -> None:
        """An empty string is a missing key, not a stream named ''."""
        job = ConsumerJob(
            message_id="1-1", reply_log_stream_key="logs", reply_event_stream_key=""
        )
        assert job.event_stream_key == "logs"


class TestEventStreamRouting:

    @flush_all_cache
    def test_events_and_logs_go_to_separate_streams(
        self, job_message_with_event_stream: dict[str, str]
    ) -> None:
        redis_client = get_redis_client()
        redis_client.xadd(UPSTREAM_KEY, job_message_with_event_stream)  # type: ignore[arg-type]

        results: list[Optional[Consumer[Any, Any]]] = [None]
        thread = threading.Thread(target=run_new_job, args=(results,), daemon=True)
        thread.start()
        thread.join(timeout=10)
        consumer = results[0]
        assert consumer is not None and consumer.job is not None

        consumer.log(message="a human line")
        consumer.event(message={"kind": "started", "pct": 0})
        consumer.event(message={"kind": "progress", "pct": 50}, level="INFO")

        log_key = job_message_with_event_stream[LOG_STREAM_FIELD_NAME]
        event_key = job_message_with_event_stream[EVENT_STREAM_FIELD_NAME]
        assert log_key != event_key

        events = _entries(event_key)
        assert [e["type"] for e in events] == ["EVENT", "EVENT"]
        # Order within a stream is arrival order, and one writer wrote both.
        assert [json.loads(e["message"])["kind"] for e in events] == [
            "started",
            "progress",
        ]

        # The log stream carries the human line -- plus new_job()'s own smoke log --
        # and none of the events.
        logs = _entries(log_key)
        assert all(entry["type"] == "LOG" for entry in logs)
        assert any("a human line" == entry["message"] for entry in logs)

    @flush_all_cache
    def test_events_fall_back_to_log_stream(
        self, job_message_str_payload: dict[str, str]
    ) -> None:
        """No event_stream_key in the message: events must still be delivered."""
        assert EVENT_STREAM_FIELD_NAME not in job_message_str_payload
        redis_client = get_redis_client()
        redis_client.xadd(UPSTREAM_KEY, job_message_str_payload)  # type: ignore[arg-type]

        results: list[Optional[Consumer[Any, Any]]] = [None]
        thread = threading.Thread(target=run_new_job, args=(results,), daemon=True)
        thread.start()
        thread.join(timeout=10)
        consumer = results[0]
        assert consumer is not None and consumer.job is not None
        assert consumer.job.reply_event_stream_key is None

        consumer.event(message={"kind": "started"})

        entries = _entries(job_message_str_payload[LOG_STREAM_FIELD_NAME])
        event_entries = [e for e in entries if e["type"] == "EVENT"]
        assert len(event_entries) == 1
        assert json.loads(event_entries[0]["message"])["kind"] == "started"

    @flush_all_cache
    def test_a_message_without_an_event_stream_is_still_accepted(
        self, job_message_str_payload: dict[str, str]
    ) -> None:
        """The event stream is optional -- its absence must not discard the job."""
        redis_client = get_redis_client()
        redis_client.xadd(UPSTREAM_KEY, job_message_str_payload)  # type: ignore[arg-type]

        results: list[Optional[Consumer[Any, Any]]] = [None]
        thread = threading.Thread(target=run_new_job, args=(results,), daemon=True)
        thread.start()
        thread.join(timeout=10)
        consumer = results[0]
        assert consumer is not None
        assert consumer.job is not None
        assert consumer.job.payload is not None

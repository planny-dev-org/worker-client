import decorator

import json
from typing import Any, Callable

import pytest

from worker_client.client import get_redis_client
from worker_client.constants import (
    LOG_STREAM_FIELD_NAME,
    OUTPUT_STREAM_FIELD_NAME,
    PAYLOAD_FIELD_NAME,
    REMOTE_RESOURCE_ID_FIELD_NAME,
)


def flush_all_cache(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(instance: Any, *args: Any, **kwargs: Any) -> Any:
        redis_api = get_redis_client()
        redis_api.flushall()  # type: ignore[dmisc]
        ret = func(*args, **kwargs)
        return ret

    return decorator.decorator(wrapper, func)  # type: ignore[return-value]


@pytest.fixture
def job_message_str_payload() -> dict[str, str]:
    return {
        LOG_STREAM_FIELD_NAME: "worker:logs:12345",
        OUTPUT_STREAM_FIELD_NAME: "worker:output:12345",
        PAYLOAD_FIELD_NAME: json.dumps("process_data"),
        REMOTE_RESOURCE_ID_FIELD_NAME: "resource_67890",
    }


@pytest.fixture
def job_message_unencoded_payload() -> dict[str, str]:
    return {
        LOG_STREAM_FIELD_NAME: "worker:logs:12345",
        OUTPUT_STREAM_FIELD_NAME: "worker:output:12345",
        PAYLOAD_FIELD_NAME: "process_data",  # this is not JSON encoded
        REMOTE_RESOURCE_ID_FIELD_NAME: "resource_67890",
    }


@pytest.fixture
def job_message_ok() -> dict[str, str]:
    return {
        LOG_STREAM_FIELD_NAME: "worker:logs:12345",
        OUTPUT_STREAM_FIELD_NAME: "worker:output:12345",
        PAYLOAD_FIELD_NAME: json.dumps({"task": "process_data", "id": 42}),
        REMOTE_RESOURCE_ID_FIELD_NAME: "resource_67890",
    }


@pytest.fixture
def job_message_missing_payload() -> dict[str, str]:
    return {
        LOG_STREAM_FIELD_NAME: "worker:logs:12345",
        OUTPUT_STREAM_FIELD_NAME: "worker:output:12345",
        REMOTE_RESOURCE_ID_FIELD_NAME: "resource_67890",
    }


@pytest.fixture
def job_message_missing_resource_id() -> dict[str, str]:
    return {
        LOG_STREAM_FIELD_NAME: "worker:logs:12345",
        OUTPUT_STREAM_FIELD_NAME: "worker:output:12345",
        PAYLOAD_FIELD_NAME: json.dumps({"task": "process_data", "data_id": 42}),
    }


@pytest.fixture
def job_message_missing_log_key() -> dict[str, str]:
    return {
        OUTPUT_STREAM_FIELD_NAME: "worker:output:12345",
        PAYLOAD_FIELD_NAME: json.dumps({"task": "process_data", "data_id": 42}),
        REMOTE_RESOURCE_ID_FIELD_NAME: "resource_67890",
    }


@pytest.fixture
def job_message_missing_output_key() -> dict[str, str]:
    return {
        LOG_STREAM_FIELD_NAME: "worker:logs:12345",
        PAYLOAD_FIELD_NAME: json.dumps({"task": "process_data", "data_id": 42}),
        REMOTE_RESOURCE_ID_FIELD_NAME: "resource_67890",
    }

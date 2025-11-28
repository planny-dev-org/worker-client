import json
import sys
import signal
import time
from typing import Optional, List, Tuple, Literal
import datetime
import logging
from dataclasses import dataclass
from worker_client.decorators import check_consumer_job

import redis

from worker_client.constants import (
    OUTPUT_STREAM_FIELD_NAME,
    LOG_STREAM_FIELD_NAME,
    REMOTE_RESOURCE_ID_FIELD_NAME,
    PAYLOAD_FIELD_NAME,
)
from worker_client.settings import (
    REDIS_HOST,
    REDIS_SCHEME,
    REDIS_PORT,
    REDIS_DB,
    REDIS_SSL_KEY_PATH,
    REDIS_SSL_CERT_PATH,
    REDIS_SSL_CERT_REQS,
    UPSTREAM_KEY,
    CONSUMER_VERSION_MAJOR,
    CONSUMER_VERSION_MINOR,
    CONSUMER_NAME_PREFIX,
)

LOG = logging.getLogger(__name__)
LEVEL_LITERAL = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def get_redis_client() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        ssl=True if REDIS_SCHEME == "rediss://" else False,
        db=REDIS_DB,
        ssl_cert_reqs=REDIS_SSL_CERT_REQS,
        ssl_keyfile=REDIS_SSL_KEY_PATH,
        ssl_certfile=REDIS_SSL_CERT_PATH,
        decode_responses=True,
    )


@dataclass
class LogMessage:
    timestamp: datetime.datetime
    message: str
    level_str: Optional[LEVEL_LITERAL] = logging.getLevelName(logging.INFO)

    def __init__(
        self,
        message: str,
        level_str: Optional[str] = logging.getLevelName(logging.INFO),
        timestamp: Optional[datetime.datetime] = datetime.datetime.now(),
    ) -> None:
        self.timestamp = timestamp
        self.level_str = level_str
        self.message = message

    def to_json(self) -> dict:
        return {
            "timestamp": self.timestamp.isoformat(),
            "level": self.level_str,
            "message": self.message,
        }

    @classmethod
    def from_json(cls, data: dict) -> "LogMessage":
        return cls(
            message=data.get("message", ""),
            level_str=data.get("level", logging.getLevelName(logging.INFO)),
            timestamp=datetime.datetime.fromisoformat(data.get("timestamp")),
        )


class ConsumerJob:
    """
    Worker job class that holds job related stream keys and expose methods to interact properly with remote backend
    """

    message_id: str
    reply_log_stream_key: str
    reply_output_stream_key: Optional[str]
    remote_resource_id: Optional[str]
    payload: Optional[dict]

    def __init__(
        self,
        message_id: str,
        reply_log_stream_key: str,
        reply_output_stream_key: Optional[str] = None,
        remote_resource_id: Optional[str] = None,
        payload: Optional[dict] = None,
    ) -> None:
        self.message_id = message_id
        self.reply_log_stream_key = reply_log_stream_key
        self.reply_output_stream_key = reply_output_stream_key
        self.remote_resource_id = remote_resource_id
        self.payload = payload


class Consumer:
    """
    Class that handles interaction with redis as a consumer part of a consumer group
    """

    job: Optional[ConsumerJob] = None
    group_name: str
    consumer_name: str
    redis_client: redis.Redis
    exit_loop: bool = False

    def __init__(self) -> None:
        self.redis_client = get_redis_client()
        self.group_name = f"{CONSUMER_VERSION_MAJOR}_{CONSUMER_VERSION_MINOR}"
        self.consumer_name = (
            f"{CONSUMER_NAME_PREFIX}_{datetime.datetime.now().isoformat()}"
        )
        # init job consumer group
        try:
            self.redis_client.xgroup_create(
                UPSTREAM_KEY, self.group_name, id="0", mkstream=True
            )
        except redis.exceptions.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                # if group already exists (BUSYGROUP) => ignore
                raise

        # setup signal handler for graceful shutdown
        try:
            signal.signal(signal.SIGINT, self.sig_int_handler)
        except ValueError:
            # signal can be set only in main thread, ignore otherwise
            pass

    @check_consumer_job
    def log(self, log_message: LogMessage) -> None:
        self.redis_client.xadd(self.job.reply_log_stream_key, log_message.to_json())

    @check_consumer_job
    def output(self, payload: dict) -> None:
        self.redis_client.xadd(self.job.reply_output_stream_key, payload)

    @check_consumer_job
    def acknowledge(self):
        self.redis_client.xack(UPSTREAM_KEY, self.group_name, self.job.message_id)

    def flat_log(self, level_str: LEVEL_LITERAL, message: str) -> None:
        self.log(LogMessage(level_str=level_str, message=message))

    def health_check(self) -> None:
        """
        Raise a ConnectionError if Redis server ping fails
        """
        if not self.redis_client.ping():
            raise ConnectionError("unable to ping redis server")

    def sig_int_handler(self, sig, frame) -> None:
        """
        Signal INT handler that set exit_loop to True
        """
        self.exit_loop = True

    def new_job(self) -> None:
        """
        Wait for a message and set job field with a CustomerJob instance
        """

        def get_expected_message() -> Tuple[str, dict]:
            """
            Blocking method that get and check messages from a single redis stream
            """
            expected_message: Optional[Tuple[str, dict]] = None
            while expected_message is None:
                try:
                    messages: List[Tuple[str, List[Tuple[str, dict]]]] = (
                        self.redis_client.xreadgroup(
                            self.group_name,
                            self.consumer_name,
                            {UPSTREAM_KEY: ">"},
                            count=1,
                            block=1000,
                        )
                    )
                except redis.exceptions.RedisError as redis_exc:
                    # try to renew client after cooldown
                    LOG.error(
                        f"unable to read from redis stream {UPSTREAM_KEY}: {str(redis_exc)}, retrying in 5 seconds..."
                    )
                    self.redis_client.close()
                    time.sleep(5)
                    self.redis_client = get_redis_client()
                    continue

                if self.exit_loop:
                    # exit loop requested
                    break

                if not messages:
                    # no message retrieved, this happens if read timeout has been reached
                    continue

                # check received messages, discard if more than 1 message is received for stream
                if len(messages) != 1:
                    LOG.error(
                        f"expected a single stream message, received {len(messages)} messages ({str(messages)}). Messages will be left pending (not acknowledged)"
                    )
                    continue

                if len(messages[0]) != 2:
                    LOG.error(
                        f"expected a tuple of length 2 for stream {UPSTREAM_KEY}, received {messages[0]}. Messages will be left pending (not acknowledged)"
                    )
                    continue

                _, stream_messages = messages[0]
                if len(stream_messages) != 1:
                    LOG.error(
                        f"expected a single entry for stream {UPSTREAM_KEY} tuple, received {stream_messages}. Messages will be left pending (not acknowledged)"
                    )
                    continue

                expected_message_id, expected_messages_data = stream_messages[0]
                if not isinstance(expected_messages_data, dict):
                    LOG.error(
                        f"expected a dict as message data for message id {expected_message_id}"
                    )
                    self.acknowledge(expected_message_id)
                    continue

                # check if log stream key is found, otherwise message is discarded
                if LOG_STREAM_FIELD_NAME not in expected_messages_data:
                    LOG.error(
                        f"expected a {LOG_STREAM_FIELD_NAME} key in server message. Message is discarded"
                    )
                    self.acknowledge(expected_message_id)
                    continue

                expected_message = (expected_message_id, expected_messages_data)

            return expected_message

        while True:
            # clean previous job if exists
            if self.job is not None:
                self.acknowledge()
                self.job = None

            self.health_check()

            try:
                message_id, message_data = get_expected_message()
            except TypeError:
                # get_expected_message() has been interrupted
                break

            # instanciate ConsumerJob with minimal setup to dialog with server
            self.job = ConsumerJob(
                message_id=message_id,
                reply_log_stream_key=message_data[LOG_STREAM_FIELD_NAME],
                payload=message_data,
            )

            # send a smoke log message
            message = f"message {message_id} received by {self.consumer_name}"
            self.flat_log(message=message, level_str=logging.getLevelName(logging.INFO))
            LOG.info(message)

            # finish message checks, now server is notified about encountered errors
            if OUTPUT_STREAM_FIELD_NAME not in message_data:
                message = f"'{OUTPUT_STREAM_FIELD_NAME}' field is missing from message_id {message_id}, message is discarded"
                self.flat_log(
                    level_str=logging.getLevelName(logging.ERROR),
                    message=message,
                )
                LOG.error(message)
                continue
            self.job.reply_output_stream_key = message_data[OUTPUT_STREAM_FIELD_NAME]

            if REMOTE_RESOURCE_ID_FIELD_NAME not in message_data:
                message = f"'{REMOTE_RESOURCE_ID_FIELD_NAME}' field is missing from message_id {message_id}, message is discarded"
                self.flat_log(
                    level_str=logging.getLevelName(logging.ERROR),
                    message=message,
                )
                LOG.error(message)
                continue
            self.job.remote_resource_id = message_data[REMOTE_RESOURCE_ID_FIELD_NAME]

            if PAYLOAD_FIELD_NAME not in message_data:
                message = f"'{PAYLOAD_FIELD_NAME}' field is missing from message_id {message_id}, message is discarded"
                self.flat_log(
                    level_str=logging.getLevelName(logging.ERROR),
                    message=message,
                )
                LOG.error(message)
                continue
            try:
                self.job.payload = json.loads(message_data[PAYLOAD_FIELD_NAME])
            except (json.decoder.JSONDecodeError, TypeError, ValueError) as exc:
                message = f"unable to decode payload field from message id {message_data}, message is discarded (remote resource id: {self.job.remote_resource_id}): {str(exc)}"
                self.flat_log(
                    level_str=logging.getLevelName(logging.ERROR),
                    message=message,
                )
                LOG.error(message)
                continue

            if not isinstance(self.job.payload, dict):
                message = f"expected a dict as message data for message id {message_id}, message is discarded (remote resource id: {self.job.remote_resource_id})"
                self.flat_log(
                    level_str=logging.getLevelName(logging.ERROR),
                    message=message,
                )
                LOG.error(message)
                continue

            # set self.job and exit the loop
            self.job.payload = message_data[PAYLOAD_FIELD_NAME]
            break

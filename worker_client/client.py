import os
import json
import signal
import time
import types
import requests

from typing import (
    Optional,
    List,
    Tuple,
    Literal,
    Generic,
    TypeVar,
    Callable,
    Protocol,
    Union,
)
import datetime
import logging
from dataclasses import dataclass
from worker_client.decorators import check_consumer_job

import redis

from worker_client.constants import (
    OUTPUT_STREAM_FIELD_NAME,
    LOG_STREAM_FIELD_NAME,
    REMOTE_RESOURCE_ID_FIELD_NAME,
    REMOTE_CALLBACK_URL_FIELD_NAME,
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
    REMOTE_API_TOKEN,
)

LOG = logging.getLogger(__name__)
LEVEL_LITERAL = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
MSG_TYPE_LITERAL = Literal["LOG", "OUTPUT"]

# Type alias for JSON-compatible dict values (used for messages/logs sent to Redis)
JsonDict = dict[str, Union[str, int, float, bool, None]]

# Type alias for Redis stream data (data received from Redis - runtime values from external source)
# Using object to indicate this is opaque data from Redis that we validate at runtime
RedisStreamData = dict[str, object]

# Generic type variable for message payload (covariant for Protocol)
T_co = TypeVar("T_co", covariant=True)
# Generic type variable for contravariant encoder
T_contra = TypeVar("T_contra", contravariant=True)
# Generic type variable for Consumer, Producer and ConsumerJob
T = TypeVar("T")
# Generic type variable for Consumer output messages
T_out = TypeVar("T_out")


class Decoder(Protocol[T_co]):
    """Protocol for decoder functions that transform raw bytes to typed messages."""

    def __call__(self, raw: bytes) -> T_co: ...


class Encoder(Protocol[T_contra]):
    """Protocol for encoder functions that transform typed messages to JSON strings."""

    def __call__(self, message: T_contra) -> str: ...


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
class Message:
    """
    dataclass structure intended to be used in call to log and output method
    timestamp is set automatically
    """

    timestamp: datetime.datetime
    message: str
    message_type: Optional[MSG_TYPE_LITERAL]  # set by calling method
    level: Optional[LEVEL_LITERAL] = "INFO"

    def __init__(
        self,
        message: Union[str, JsonDict],
        message_type: MSG_TYPE_LITERAL,
        level: Optional[LEVEL_LITERAL] = "INFO",
    ) -> None:
        if isinstance(message, dict):
            message = json.dumps(message)
        self.message = message
        self.message_type = message_type
        self.level = level
        self.timestamp = datetime.datetime.now()

    def to_json(self) -> JsonDict:
        return {
            "type": self.message_type,
            "timestamp": self.timestamp.isoformat(),
            "level": self.level,
            "message": self.message,
        }


class ConsumerJob:
    """
    Worker job class that holds job related stream keys and expose methods to interact properly with remote backend.
    The payload is stored as a raw JSON string from Redis and will be decoded to type T by the Consumer's decoder.
    """

    message_id: str
    reply_log_stream_key: str
    reply_output_stream_key: Optional[str]
    remote_resource_id: Optional[str]
    remote_callback_url: Optional[str]
    payload: Optional[str]  # Raw JSON string from Redis

    def __init__(
        self,
        message_id: str,
        reply_log_stream_key: str,
        reply_output_stream_key: Optional[str] = None,
        remote_resource_id: Optional[str] = None,
        remote_callback_url: Optional[str] = None,
        payload: Optional[str] = None,
    ) -> None:
        self.message_id = message_id
        self.reply_log_stream_key = reply_log_stream_key
        self.reply_output_stream_key = reply_output_stream_key
        self.remote_resource_id = remote_resource_id
        self.remote_callback_url = remote_callback_url
        self.payload = payload


class Producer(Generic[T]):
    """
    Class that handles sending messages to Redis streams.
    Generic over T which is the type of messages that the encoder will receive.
    """

    redis_client: redis.Redis  # type: ignore[type-arg]
    encoder: Encoder[T]
    stream_key: str

    def __init__(
        self,
        encoder: Encoder[T],
        stream_key: str = UPSTREAM_KEY,
    ) -> None:
        """
        Initialize a Producer with an encoder function and target stream.

        Args:
            encoder: Function that encodes typed messages to JSON strings
            stream_key: Redis stream key to send messages to (defaults to UPSTREAM_KEY)
        """
        self.redis_client = get_redis_client()
        self.encoder = encoder
        self.stream_key = stream_key

    def send(
        self,
        message: T,
        log_stream_key: str,
        output_stream_key: str,
        remote_resource_id: str,
        remote_callback_url: str,
    ) -> str:
        """
        Send a typed message to the Redis stream.

        Args:
            message: The typed message to send
            log_stream_key: Stream key for log messages
            output_stream_key: Stream key for output messages
            remote_resource_id: Identifier for the remote resource
            remote_callback_url: Callback URL for the remote resource

        Returns:
            The message ID assigned by Redis
        """
        # Encode the typed message to a JSON string
        payload_json = self.encoder(message)

        # Validate it's valid JSON
        try:
            json.loads(payload_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Encoder must return valid JSON string: {e}")

        # Send to Redis
        message_id = self.redis_client.xadd(
            self.stream_key,
            {
                PAYLOAD_FIELD_NAME: payload_json,
                LOG_STREAM_FIELD_NAME: log_stream_key,
                OUTPUT_STREAM_FIELD_NAME: output_stream_key,
                REMOTE_RESOURCE_ID_FIELD_NAME: remote_resource_id,
                REMOTE_CALLBACK_URL_FIELD_NAME: remote_callback_url,
            },
        )

        LOG.info(f"Sent message {message_id} to stream {self.stream_key}")
        return message_id

    def close(self) -> None:
        """Close the Redis connection."""
        self.redis_client.close()


class Consumer(Generic[T, T_out]):
    """
    Class that handles interaction with redis as a consumer part of a consumer group.
    Generic over T which is the type of decoded messages that the handler will receive,
    and T_out which is the type of messages sent via output().
    """

    job: Optional[ConsumerJob] = None
    group_name: str
    consumer_name: str
    redis_client: redis.Redis  # type: ignore[type-arg]
    exit_loop: bool = False
    decoder: Optional[Decoder[T]]
    handler: Optional[Callable[[T], None]]

    def __init__(
        self,
        decoder: Optional[Decoder[T]] = None,
        handler: Optional[Callable[[T], None]] = None,
    ) -> None:
        self.redis_client = get_redis_client()
        self.group_name = f"{CONSUMER_VERSION_MAJOR}_{CONSUMER_VERSION_MINOR}"
        self.consumer_name = f"{CONSUMER_NAME_PREFIX}_{os.getpid()}"
        self.decoder = decoder
        self.handler = handler
        # init job consumer group
        try:
            self.redis_client.xgroup_create(
                UPSTREAM_KEY, self.group_name, id="0", mkstream=True
            )
        except redis.exceptions.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):  # type: ignore[arg-type]
                # if group already exists (BUSYGROUP) => ignore
                raise

        # setup signal handler for graceful shutdown
        try:
            signal.signal(signal.SIGINT, self.sig_int_handler)
        except ValueError:
            # signal can be set only in main thread, ignore otherwise
            pass

    @check_consumer_job
    def log(self, message: Union[str, JsonDict], level: LEVEL_LITERAL = "INFO") -> None:
        self.redis_client.xadd(
            self.job.reply_log_stream_key,  # type: ignore[union-attr]
            Message(message=message, message_type="LOG", level=level).to_json(),  # type: ignore[arg-type]
        )

    @check_consumer_job
    def output(self, message: T_out) -> None:
        self.redis_client.xadd(
            self.job.reply_output_stream_key,  # type: ignore[union-attr]
            Message(message=message, message_type="OUTPUT", level="INFO").to_json(),  # type: ignore[arg-type]
        )

    @check_consumer_job
    def callback(self) -> requests.Response:
        if self.job.remote_callback_url is None:
            raise ValueError("No remote_callback_url provided from upstream message")

        headers = {
            "Authorization": REMOTE_API_TOKEN,
        }
        response = requests.post(self.job.remote_callback_url, headers=headers)
        return response

    def acknowledge(self, message_id: Optional[str] = None) -> None:
        """
        acknowledge the current job message or the provided message_id (useful if consumer_job is not yet set)
        reset job field to None
        """
        if self.job is None and message_id is None:
            raise ValueError("No job nor message_id to acknowledge")
        self.redis_client.xack(
            UPSTREAM_KEY, self.group_name, message_id or self.job.message_id  # type: ignore[union-attr]
        )
        self.job = None

    def health_check(self) -> None:
        """
        Raise a ConnectionError if Redis server ping fails
        """
        if not self.redis_client.ping():  # type: ignore[misc]
            raise ConnectionError("unable to ping redis server")

    def sig_int_handler(self, sig: int, frame: Optional[types.FrameType]) -> None:
        """
        Signal handler that set exit_loop to True on SIGINT, SIGTERM and SIGPIPE
        """
        if sig in [signal.SIGINT, signal.SIGTERM, signal.SIGPIPE]:
            self.exit_loop = True

    def new_job(self) -> None:
        """
        Wait for a message and set job field with a ConsumerJob instance
        """

        def _get_expected_message() -> Optional[Tuple[str, RedisStreamData]]:
            """
            Blocking method that get and check messages from a single redis stream.
            Returns None if exit_loop is requested.
            """
            expected_message: Optional[Tuple[str, RedisStreamData]] = None
            while expected_message is None:
                try:
                    messages: List[Tuple[str, List[Tuple[str, RedisStreamData]]]] = (
                        self.redis_client.xreadgroup(  # type: ignore[assignment]
                            self.group_name,
                            self.consumer_name,
                            {UPSTREAM_KEY: ">"},
                            count=1,
                            block=3000,
                        )
                    )
                except redis.exceptions.RedisError as redis_exc:
                    # try to renew client after cooldown
                    error_msg = (
                        f"unable to read from redis stream {UPSTREAM_KEY}: "
                        f"{str(redis_exc)}, retrying in 5 seconds..."  # type: ignore[arg-type]
                    )
                    LOG.error(error_msg)
                    self.redis_client.close()
                    time.sleep(5)
                    self.redis_client = get_redis_client()
                    continue

                if self.exit_loop:
                    # exit loop requested
                    LOG.info("exit loop requested, stopping new job retrieval")
                    break

                if not messages:
                    # no message retrieved, this happens if read timeout has been reached
                    continue

                # check received messages, discard if more than 1 message is received for stream
                if len(messages) != 1:
                    LOG.error(
                        f"""
                        expected a single stream message, received {len(messages)} messages ({str(messages)}).
                        Messages will be left pending (not acknowledged)
                        """
                    )
                    continue

                if len(messages[0]) != 2:
                    LOG.error(
                        f"""
                        expected a tuple of length 2 for stream {UPSTREAM_KEY}, received {messages[0]}.
                        Messages will be left pending (not acknowledged)
                        """
                    )
                    continue

                _, stream_messages = messages[0]
                if len(stream_messages) != 1:
                    LOG.error(
                        f"""
                        expected a single entry for stream {UPSTREAM_KEY} tuple, received {stream_messages}.
                        Messages will be left pending (not acknowledged)
                        """
                    )
                    continue

                expected_message_id, expected_messages_data = stream_messages[0]
                if not isinstance(expected_messages_data, dict):  # type: ignore[arg-type]
                    LOG.error(
                        f"expected a dict as message data for message id {expected_message_id}. Message is discarded"
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

        # clean previous job if exists
        if self.job is not None:
            self.acknowledge()
            self.job = None
        self.exit_loop = False

        self.health_check()

        while True:

            try:
                result = _get_expected_message()
                if result is None:
                    # _get_expected_message() was interrupted
                    break
                message_id, message_data = result
            except (TypeError, ValueError):
                # _get_expected_message() has been interrupted
                break

            # instantiate ConsumerJob with minimal setup to dialog with server (message_id and log stream key)
            self.job = ConsumerJob(
                message_id=message_id,
                reply_log_stream_key=str(message_data[LOG_STREAM_FIELD_NAME]),
            )

            # send a smoke log message
            message = f"message {message_id} received by {self.consumer_name}"
            self.log(message=message, level="INFO")
            LOG.info(message)

            # finish message checks, now server is notified about encountered errors
            if OUTPUT_STREAM_FIELD_NAME not in message_data:
                message = f"""
                    '{OUTPUT_STREAM_FIELD_NAME}' field is missing from message_id {message_id}, message is discarded
                """
                self.log(
                    message=message,
                    level="ERROR",
                )
                LOG.error(message)
                continue
            self.job.reply_output_stream_key = str(
                message_data[OUTPUT_STREAM_FIELD_NAME]
            )

            if REMOTE_RESOURCE_ID_FIELD_NAME not in message_data:
                message = f"""
                    '{REMOTE_RESOURCE_ID_FIELD_NAME}' field is missing from message_id {message_id},
                    message is discarded
                """
                self.log(
                    message=message,
                    level="ERROR",
                )
                LOG.error(message)
                continue
            self.job.remote_resource_id = str(
                message_data[REMOTE_RESOURCE_ID_FIELD_NAME]
            )

            if PAYLOAD_FIELD_NAME not in message_data:
                message = f"'{PAYLOAD_FIELD_NAME}' field is missing from message_id {message_id}, message is discarded"
                self.log(message=message, level="ERROR")
                LOG.error(message)
                continue

            # Validate that payload is a valid JSON string
            payload_value = message_data[PAYLOAD_FIELD_NAME]
            if not isinstance(payload_value, str):
                message = f"""
                expected a JSON string as payload for message id {message_id},
                message is discarded (remote resource id: {self.job.remote_resource_id})
                """
                self.log(message=message, level="ERROR")
                LOG.error(message)
                continue

            try:
                # Validate it's valid JSON (but keep as string)
                json.loads(payload_value)
                self.job.payload = payload_value
            except (json.decoder.JSONDecodeError, TypeError, ValueError) as exc:
                message = f"""
                    unable to decode payload field from message id {message_id},
                    message is discarded (remote resource id: {self.job.remote_resource_id}): {str(exc)}
                """
                self.log(
                    message=message,
                    level="ERROR",
                )
                LOG.error(message)
                continue
                LOG.error(message)
                continue

            # job is properly set, exit the loop
            LOG.info("job is set, exiting loop")
            break

    def run(self) -> None:
        """
        Main worker loop that processes jobs using the provided decoder and handler.
        The decoder receives the raw JSON string as bytes and returns a typed message T.
        """
        if self.decoder is None:
            raise ValueError("Decoder must be provided to run the consumer")
        if self.handler is None:
            raise ValueError("Handler must be provided to run the consumer")

        print(f"Consumer[T] listening on: {UPSTREAM_KEY}")

        while not self.exit_loop:
            try:
                # Get a new job from the stream
                self.new_job()

                if self.job is None or self.job.payload is None:
                    # Job retrieval was interrupted or invalid
                    continue

                # Encode the JSON string payload as bytes for the decoder
                raw_payload = self.job.payload.encode("utf-8")

                # Decode to typed message T
                message: T = self.decoder(raw_payload)

                # Handle the typed message
                self.handler(message)

                # Acknowledge the job
                self.acknowledge()

            except Exception as ex:
                error_msg = f"Consumer error: {ex}"
                LOG.error(error_msg)
                print(error_msg)
                if self.job:
                    try:
                        self.log(message=error_msg, level="ERROR")
                    except Exception:
                        pass  # If logging fails, continue
                    self.acknowledge()
                time.sleep(1)

        print("Consumer[T] shutting down gracefully.")

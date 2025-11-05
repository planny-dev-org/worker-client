from typing import Optional
import datetime

import redis
from decouple import config

from worker_client.constants import (
    MESSAGE_JOB_OUTPUT_STREAM_KEY,
    MESSAGE_PAYLOAD_KEY,
    MESSAGE_LOG_STREAM_KEY,
    REDIS_CONSUMER_NAME,
    CONSUMER_VERSION_MAJOR,
    CONSUMER_VERSION_MINOR,
    REDIS_HOST,
    REDIS_USERNAME,
    REDIS_DB,
)


class WorkerClient:
    """
    Worker class that includes methods to read / add messages from redis streams
    """

    log_stream_key: Optional[str]
    job_output_stream_key: Optional[str]
    consumer_version_major: int
    consumer_version_minor: int

    def __init__(self) -> None:
        self.job_stream_key: str = config("REDIS_STREAM_KEY")
        self.redis_client: redis.Redis = redis.Redis(
            host=config(REDIS_HOST, "localhost"),
            username=config(REDIS_USERNAME, None),
            password=config(REDIS_HOST, None),
            db=config(REDIS_DB, 0),
            decode_responses=True,
        )

        self.consumer_version_major: int = int(config(CONSUMER_VERSION_MAJOR))
        self.consumer_version_minor: int = int(config(CONSUMER_VERSION_MINOR))
        self.group_name: str = (
            f"{self.consumer_version_major}_{self.consumer_version_minor}"
        )
        self.consumer_name: str = (
            f"{config(REDIS_CONSUMER_NAME, '')}_{datetime.datetime.now().isoformat()}"
        )

        # init job consumer group
        try:
            self.redis_client.xgroup_create(
                self.job_stream_key, self.group_name, id="0", mkstream=True
            )
        except redis.exceptions.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                # if group already exists (BUSYGROUP) => ignore
                raise

    def read_job(self) -> dict:
        """
        Read a job message, setup log & output job streams
        :return: job payload
        """
        # reset log & output stream keys
        self.log_stream_key = None
        self.job_output_stream_key = None

        # wait for message
        message = self.redis_client.xreadgroup(
            self.group_name,
            self.consumer_name,
            {self.job_stream_key: ">"},
            count=1,
            block=0,
        )

        # check existence of attributes needed to reply otherwise server will loose track of job computation
        if MESSAGE_PAYLOAD_KEY not in message:
            raise KeyError(
                f"expected a {MESSAGE_PAYLOAD_KEY} key in server message ({str(message)})"
            )
        if MESSAGE_LOG_STREAM_KEY not in message:
            raise KeyError(
                f"expected a {MESSAGE_LOG_STREAM_KEY} key in server message ({str(message)})"
            )
        if MESSAGE_JOB_OUTPUT_STREAM_KEY not in message:
            raise KeyError(
                f"expected a output_stream_key key in server message ({str(message)})"
            )

        # message received, assign job output & log stream keys
        self.job_output_stream_key = message[MESSAGE_JOB_OUTPUT_STREAM_KEY]
        self.log_stream_key = message[MESSAGE_LOG_STREAM_KEY]

        return message[MESSAGE_PAYLOAD_KEY]

    def add_log(self, message: dict) -> None:
        self.redis_client.xadd(self.log_stream_key, message)

    def add_output_job(self, message: dict) -> None:
        self.redis_client.xadd(self.job_output_stream_key, message)

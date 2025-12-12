from typing import Generic, TypeVar, Optional, Callable, Protocol, Union
import redis

T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)
T = TypeVar("T")

class Decoder(Protocol[T_co]):
    def __call__(self, raw: bytes) -> T_co: ...

class Encoder(Protocol[T_contra]):
    def __call__(self, message: T_contra) -> str: ...

JsonDict = dict[str, Union[str, int, float, bool, None]]
LEVEL_LITERAL = str
MSG_TYPE_LITERAL = str

def get_redis_client() -> redis.Redis: ...

class Message:
    timestamp: object
    message: str
    message_type: Optional[MSG_TYPE_LITERAL]
    level: Optional[LEVEL_LITERAL]
    def __init__(
        self,
        message: Union[str, JsonDict],
        message_type: MSG_TYPE_LITERAL,
        level: Optional[LEVEL_LITERAL] = "INFO",
    ) -> None: ...
    def to_json(self) -> JsonDict: ...

class ConsumerJob:
    message_id: str
    reply_log_stream_key: str
    reply_output_stream_key: Optional[str]
    remote_resource_id: Optional[str]
    payload: Optional[str]
    def __init__(
        self,
        message_id: str,
        reply_log_stream_key: str,
        reply_output_stream_key: Optional[str] = None,
        remote_resource_id: Optional[str] = None,
        payload: Optional[str] = None,
    ) -> None: ...

class Producer(Generic[T]):
    redis_client: redis.Redis  # type: ignore[type-arg]
    encoder: Encoder[T]
    stream_key: str
    def __init__(
        self,
        encoder: Encoder[T],
        stream_key: str = ...,
    ) -> None: ...
    def send(
        self,
        message: T,
        log_stream_key: str,
        output_stream_key: str,
        remote_resource_id: str,
    ) -> str: ...
    def close(self) -> None: ...

class Consumer(Generic[T]):
    job: Optional[ConsumerJob]
    group_name: str
    consumer_name: str
    redis_client: redis.Redis  # type: ignore[type-arg]
    exit_loop: bool
    decoder: Optional[Decoder[T]]
    handler: Optional[Callable[[T], None]]
    def __init__(
        self,
        decoder: Optional[Decoder[T]] = None,
        handler: Optional[Callable[[T], None]] = None,
    ) -> None: ...
    def log(self, message: Union[str, JsonDict], level: LEVEL_LITERAL = "INFO") -> None: ...
    def output(self, message: Union[str, JsonDict]) -> None: ...
    def acknowledge(self, message_id: Optional[str] = None) -> None: ...
    def health_check(self) -> None: ...
    def new_job(self) -> None: ...
    def run(self) -> None: ...
    def sig_int_handler(self, signum: int, frame: object) -> None: ...

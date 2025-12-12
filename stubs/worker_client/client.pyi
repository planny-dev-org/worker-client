from typing import Generic, TypeVar, Optional, Callable, Protocol, Union, Mapping, Any
import redis

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)
T_out = TypeVar("T_out")  # Output message type for Consumer.output()

class Decoder(Protocol[T_co]):
    def __call__(self, raw: bytes) -> T_co: ...

class Encoder(Protocol[T_contra]):
    def __call__(self, message: T_contra) -> str: ...

JsonDict = Mapping[str, Any]
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
    def to_json(self) -> dict[str, Any]: ...

class ConsumerJob(Generic[T]):
    """
    Job object that holds the message payload.
    When no decoder is provided, payload is a JSON string.
    When a decoder is provided, T represents the decoded type (though in practice payload remains a string).
    """
    message_id: str
    reply_log_stream_key: str
    reply_output_stream_key: Optional[str]
    remote_resource_id: Optional[str]
    payload: Optional[str]  # Always a JSON string, regardless of T
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

class Consumer(Generic[T, T_out]):
    """
    Consumer class for processing jobs from Redis streams.
    
    Type parameters:
        T: Input message type (what the worker receives/processes)
        T_out: Output message type (what the worker sends back via output())
    """
    job: Optional[ConsumerJob[T]]
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
    def output(self, message: T_out) -> None: ...
    def acknowledge(self, message_id: Optional[str] = None) -> None: ...
    def health_check(self) -> None: ...
    def new_job(self) -> None: ...
    def run(self) -> None: ...
    def sig_int_handler(self, signum: int, frame: object) -> None: ...

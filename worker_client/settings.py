from typing import cast, Optional
from decouple import config

# redis upstream key where job payloads are read from
UPSTREAM_KEY: str = cast(
    str, config("STREAM_KEY_INPUT", default="worker_input_stream", cast=str)
)

# consumers
CONSUMER_VERSION_MAJOR: int = cast(
    int, config("CONSUMER_VERSION_MAJOR", default=0, cast=int)
)
CONSUMER_VERSION_MINOR: int = cast(
    int, config("CONSUMER_VERSION_MINOR", default=0, cast=int)
)
CONSUMER_NAME_PREFIX: str = cast(
    str, config("CONSUMER_NAME_PREFIX", default="worker_client_consumer", cast=str)
)

# redis
REDIS_SCHEME: str = cast(str, config("REDIS_SCHEME", default="redis://", cast=str))
REDIS_HOST: str = cast(str, config("REDIS_HOST", default="localhost", cast=str))
REDIS_PORT: int = cast(int, config("REDIS_PORT", default=6379, cast=int))
REDIS_DB: int = cast(int, config("REDIS_DB", default=0, cast=int))
REDIS_SSL_CERT_REQS: str = cast(
    str, config("REDIS_SSL_CERT_REQS", default="none", cast=str)
)
REDIS_SSL_CERT_PATH: Optional[str] = cast(
    Optional[str],
    config("REDIS_SSL_CERT_PATH", default=None, cast=lambda x: x if x else None),
)
REDIS_SSL_KEY_PATH: Optional[str] = cast(
    Optional[str],
    config("REDIS_SSL_KEY_PATH", default=None, cast=lambda x: x if x else None),
)

REMOTE_API_TOKEN: Optional[str] = cast(
    Optional[str],
    config("REMOTE_API_TOKEN", default=None, cast=lambda x: x if x else None),
)

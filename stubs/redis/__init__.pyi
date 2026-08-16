from typing import Any, Optional, TypeVar
from redis import exceptions as exceptions

_T = TypeVar("_T")

class Redis:
    @classmethod
    def from_url(
        cls,
        url: str,
        decode_responses: bool = False,
        ssl_cert_reqs: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        ssl_keyfile: Optional[str] = None,
        **kwargs: Any,
    ) -> Redis: ...
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        ssl: bool = False,
        ssl_cert_reqs: Optional[str] = None,
        ssl_keyfile: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        decode_responses: bool = False,
        **kwargs: Any,
    ) -> None: ...
    def ping(self) -> bool: ...
    def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "$",
        mkstream: bool = False,
    ) -> bool: ...
    def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: dict[str, str],
        count: Optional[int] = None,
        block: Optional[int] = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]: ...
    def xread(
        self,
        streams: dict[str, str | int | bytes],
        count: Optional[int] = None,
        block: Optional[int] = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]: ...
    def xadd(
        self,
        name: str,
        fields: dict[str, Any],
        id: str = "*",
        maxlen: Optional[int] = None,
    ) -> str: ...
    def xack(
        self,
        name: str,
        groupname: str,
        *ids: str,
    ) -> int: ...
    def xpending(
        self,
        name: str,
        groupname: str,
    ) -> Any: ...
    def flushall(self) -> bool: ...
    def close(self) -> None: ...

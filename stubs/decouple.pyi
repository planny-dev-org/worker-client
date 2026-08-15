from typing import TypeVar, Callable, overload, Any

_T = TypeVar("_T")

class AutoConfig:
    @overload
    def __call__(
        self,
        option: str,
        default: _T,
        cast: Callable[[Any], _T],
    ) -> _T: ...
    @overload
    def __call__(
        self,
        option: str,
        *,
        cast: Callable[[Any], _T],
    ) -> _T: ...
    @overload
    def __call__(
        self,
        option: str,
        default: _T,
    ) -> _T: ...
    @overload
    def __call__(
        self,
        option: str,
    ) -> str: ...

config: AutoConfig

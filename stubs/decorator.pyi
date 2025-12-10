from typing import TypeVar, Callable, Any

_F = TypeVar("_F", bound=Callable[..., Any])

def decorator(caller: Callable[..., Any], func: _F) -> _F: ...

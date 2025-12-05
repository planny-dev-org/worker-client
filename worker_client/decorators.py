from typing import Callable, TypeVar, Any
from functools import wraps

# TypeVar for the decorated function
F = TypeVar("F", bound=Callable[..., Any])


def check_consumer_job(func: F) -> F:
    """
    Decorator to check if ConsumerJob is initialized otherwise raise a RunTimeError
    """

    @wraps(func)
    def wrapper(instance: Any, *args: Any, **kwargs: Any) -> Any:
        if not instance.job:
            raise RuntimeError("no pending job associated with this consumer, can't log message")
        return func(instance, *args, **kwargs)

    return wrapper  # type: ignore[return-value]

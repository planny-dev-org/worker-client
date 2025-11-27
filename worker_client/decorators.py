from typing import Callable


def check_consumer_job(func: Callable) -> Callable:
    """
    Decorator to check if ConsumerJob is initialized otherwise raise a RunTimeError
    """

    def wrapper(self, *args, **kwargs):
        if not self._consumer_job:
            raise RuntimeError(
                "no pending job associated with this consumer, can't log message"
            )
        return func(self, *args, **kwargs)

    return wrapper

from typing import Callable


def check_consumer_job(func: Callable) -> Callable:
    """
    Decorator to check if ConsumerJob is initialized otherwise raise a RunTimeError
    """

    def wrapper(instance, *args, **kwargs):
        if not instance.job:
            raise RuntimeError(
                "no pending job associated with this consumer, can't log message"
            )
        return func(instance, *args, **kwargs)

    return wrapper

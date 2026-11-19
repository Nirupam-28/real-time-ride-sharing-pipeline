
import time
import logging
from functools import wraps

from common.config import Config

logger = logging.getLogger("retry")


def retry(
    attempts=None,
    base_delay=None,
    exceptions=(Exception,)
):
    attempts = attempts or Config.RETRY_ATTEMPTS
    base_delay = (
        base_delay
        if base_delay is not None
        else Config.RETRY_BASE_DELAY
    )

    def decorator(func):

        @wraps(func)
        def wrapper(*args, **kwargs):

            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)

                except exceptions:
                    logger.exception(
                        "%s failed on attempt %s/%s",
                        func.__name__,
                        attempt,
                        attempts
                    )

                    if attempt == attempts:
                        logger.error(
                            "%s failed after all retries",
                            func.__name__
                        )
                        raise

                    delay = base_delay * (2 ** (attempt - 1))

                    logger.warning(
                        "Retrying %s in %.2f seconds",
                        func.__name__,
                        delay
                    )

                    time.sleep(delay)

        return wrapper

    return decorator

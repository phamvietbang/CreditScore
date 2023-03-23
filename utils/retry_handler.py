import random
import time
from functools import wraps

from utils.logger_utils import get_logger

logger = get_logger('Retry handler')


def retry_handler(wrapped):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            while True:
                try:
                    return f(*args, **kwargs)
                except Exception as ex:
                    logger.error(ex)
                    time.sleep(10 * random.random())

        return decorated_function

    return decorator(wrapped)

from contextlib import contextmanager

import redis

from .settings import (
    BUGOUT_REDIS_URL,
    BUGOUT_REDIS_PASSWORD,
)


# TODO(andrey) use redis connection method from spire.
@contextmanager
def yield_redis_connection_from_env_ctx():
    try:
        redis_client = redis.Redis().from_url(
            f"redis://:{BUGOUT_REDIS_PASSWORD}@{BUGOUT_REDIS_URL}"
        )
        yield redis_client
    finally:
        redis_client.close()

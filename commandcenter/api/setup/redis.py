from commandcenter.config.redis import (
    CC_REDIS_CONNECT_TIMEOUT,
    CC_REDIS_MAX_CONNECTIONS,
    CC_REDIS_TIMEOUT,
    CC_REDIS_URL
)



def setup_redis(sync=False):
    """Configure a Redis client object from the environment."""
    try:
        if sync:
            from redis import Redis
        else:
            from redis.asyncio import Redis
    except ImportError:
        raise RuntimeError(
            "Attempted to use redis support, but the `redis` package is not "
            "installed. Use 'pip install commandcenter[redis]'."
        )
    return Redis.from_url(
        CC_REDIS_URL,
        max_connections=CC_REDIS_MAX_CONNECTIONS,
        socket_connect_timeout=CC_REDIS_CONNECT_TIMEOUT,
        socket_timeout=CC_REDIS_TIMEOUT
    )
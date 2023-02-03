from commandcenter.util import ObjSelection
from .local import LocalManager
from .rabbitmq import RabbitMQManager
from .redis import RedisManager



__all__ = [
    "LocalManager",
    "RabbitMQManager",
    "RedisManager",
    "Managers"
]


class Managers(ObjSelection):
    DEFAULT = "default", LocalManager
    LOCAL = "local", LocalManager
    RABBITMQ = "rabbitmq", RabbitMQManager
    REDIS = "redis", RedisManager
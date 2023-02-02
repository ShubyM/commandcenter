from commandcenter.config.rabbitmq import CC_RABBITMQ_URL



def setup_rabbitmq():
    """Configure a RabbitMQ connection object from the environment."""
    try:
        from aiormq import Connection
    except ImportError:
        raise RuntimeError(
            "Attempted to use rabbitmq support, but the `aiormq` package is not "
            "installed. Use 'pip install commandcenter[rabbitmq]'."
        )
    return lambda: Connection(CC_RABBITMQ_URL)
from commandcenter.config.rabbitmq import CC_RABBITMQ_URL



def configure_rabbitmq():
    """Configure a RabbitMQ connection factory from the environment."""
    try:
        from aiormq import Connection
    except ImportError:
        raise RuntimeError(
            "Attempted to use rabbitmq support, but the `aiormq` package is not "
            "installed. Use 'pip install commandcenter[rabbitmq]'."
        )
    return lambda: Connection(CC_RABBITMQ_URL)
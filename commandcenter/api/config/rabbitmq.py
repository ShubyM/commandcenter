from starlette.config import Config



config = Config(".env")


CC_RABBITMQ_URL = config(
    "CC_RABBITMQ_URL",
    default="amqp://guest:guest@localhost:5672/"
)
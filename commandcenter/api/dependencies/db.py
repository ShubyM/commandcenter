from collections.abc import AsyncGenerator

from motor.motor_asyncio import AsyncIOMotorClient

from commandcenter.api.setup.mongo import setup_mongo



async def get_database_connection() -> AsyncGenerator[AsyncIOMotorClient, None]:
    """Create a MongoDB session."""
    client = setup_mongo()
    try:
        yield client
    finally:
        client.close()
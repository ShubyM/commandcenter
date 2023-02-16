from collections.abc import AsyncGenerator

from motor.motor_asyncio import AsyncIOMotorClient

from commandcenter.setup.mongo import configure_mongo


# should this be a memo or singleton?
async def get_database_connection() -> AsyncGenerator[AsyncIOMotorClient, None]:
    """Create a MongoDB session."""
    client = configure_mongo()
    try:
        yield client
    finally:
        client.close()
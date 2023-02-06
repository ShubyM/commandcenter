from motor.motor_asyncio import AsyncIOMotorClient

from commandcenter.api.config.mongo import (
    CC_MONGO_APPNAME,
    CC_MONGO_CONNECT_TIMEOUT,
    CC_MONGO_HEARTBEAT,
    CC_MONGO_MAX_POOL_SIZE,
    CC_MONGO_SERVER_SELECTION_TIMEOUT,
    CC_MONGO_TIMEOUT,
    CC_MONGO_URL
)



def setup_mongo():
    """Configure an async MongoDB client from the environment."""
    return AsyncIOMotorClient(
        CC_MONGO_URL,
        maxPoolSize=CC_MONGO_MAX_POOL_SIZE,
        connectTimeoutMS=CC_MONGO_CONNECT_TIMEOUT,
        heartbeatFrequencyMS=CC_MONGO_HEARTBEAT,
        serverSelectionTimeoutMS=CC_MONGO_SERVER_SELECTION_TIMEOUT,
        timeoutMS=CC_MONGO_TIMEOUT,
        appname=CC_MONGO_APPNAME
    )
from typing import Set

from motor.motor_asyncio import AsyncIOMotorClient

from commandcenter.integrations.models import Subscription


class MongoTimeseriesCollectionView:
    def __init__(
        self,
        client: AsyncIOMotorClient,
        subscriptions: Set[Subscription],
        database_name: str,
        
    ):
        pass
from typing import Dict, Type

from aiohttp import ClientSession
from uplink import AiohttpClient, Consumer

from commandcenter.sources.pi_web.data.controllers import (
    DataServers,
    Points,
    Streams
)



class PIWebAPI:
    """HTTP PI Web API client."""
    consumers: Dict[str, Consumer] = {}

    def __init__(self, session: ClientSession) -> None:
        self.session = session
    
    @property
    def dataservers(self) -> DataServers:
        """Returns a `SataServers` consumer."""
        return self._get_consumer_instance(DataServers)

    @property
    def points(self) -> Points:
        """Returns a `Points` consumer."""
        return self._get_consumer_instance(Points)

    @property
    def streams(self) -> Streams:
        """Returns a `Streams` consumer."""
        return self._get_consumer_instance(Streams)

    async def close(self) -> None:
        await self.session.close()
    
    def _get_consumer_instance(self, consumer: Type[Consumer]) -> Consumer:
        """Get an instance of the consumer for the a controller.
        
        This caches the consumer instance in the class for reuse.
        """
        name = consumer.__name__
        if name in self.consumers:
            return self.consumers[name]
        instance = consumer(client=AiohttpClient(session=self.session))
        self.consumers[name] = instance
        return instance
from enum import IntEnum
from typing import Any, Dict, List, Set
import threading

from httpx import Client, Response
from httpx_auth import OAuth2ResourceOwnerPasswordCredentials
from pydantic import BaseModel

from commandcenter.integrations.models import BaseSubscription
from commandcenter.timeseries.collection import TimeseriesCollection
from commandcenter.util import EqualJitterBackoff



class UnitOpStatus(IntEnum):
    IDLE = 0
    RUNNING = 1
    ALARM = 2


class DataItem(BaseModel):
    name: str
    subscription: BaseSubscription


class DataMapping(BaseModel):
    items: List[DataItem]

    @property
    def subscriptions(self) -> Set[BaseSubscription]:
        return set([item.subscription for item in self.items])


class BaseUnitOp(threading.Thread):
    def __init__(
        self,
        name: str,
        client: Client,
        collection: TimeseriesCollection,
        meta: Dict[str, Any] = None,
        max_backoff: float = 5,
        initial_backoff: float = 2
    ) -> None:
        self._name = name
        self._client = client
        self._collection = collection
        self._meta = meta
        self._backoff = EqualJitterBackoff(max=max_backoff, initial=initial_backoff)

        self._response: Response = None
        self._stop: threading.Event = threading.Event()
        self._stopper: threading.Thread = threading.Thread(target=self._wait_stop)
        self._status: UnitOpStatus = UnitOpStatus.IDLE
        self._subscriptions: Set[BaseSubscription] = set()
        self._watchdogs: List[BaseWatchdog] = []

    @property
    def status(self) -> UnitOpStatus:
        return self._status
    
    def _wait_stop(self) -> None:
        self._stop.wait()
        response = self._response
        self._response = None
        if response is not None:
            response.close()
    
    def stop(self) -> None:
        self._stop.set()
    
    def run(self) -> None:
        pass


class BaseWatchdog:
    pass
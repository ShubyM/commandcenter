import asyncio
from datetime import datetime, timedelta

import pandas as pd

from commandcenter.client import CCClient
from commandcenter.timeseries.models import UnitOp
from commandcenter.services.watchdog.base import BaseWatchdog


class UnitOpCollection:
    def __init__(
        self,
        client: CCClient,
        unitop: UnitOp,
        *watchdogs: BaseWatchdog,
        backfill: int = 7200
    ) -> None:
        self._client = client
        self._unitop = unitop
        self._watchdogs = watchdogs
        self._backfill = backfill

        self.data: pd.DataFrame = None
        self._subscription_map = {v: k for k, v in unitop.data_mapping.items()}
        self._pending_update: asyncio.TimerHandle = None

    async def start(self) -> None:
        start_time = datetime.now() - timedelta(seconds=self._backfill)
        self.data = await self._client.download_unitop_data_to_pandas(
            self._unitop.unitop_id,
            start_time=start_time
        )

    async def _ingest_data(self) -> None:
        async for message in self._client.stream_unitop_data(self._unitop.unitop_id):
            data_item = self._subscription_map.get(message.subscription)
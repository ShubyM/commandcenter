import functools
from typing import Any, Dict

from commandcenter.timeseries.mongo.worker import MongoTimeseriesWorker
from commandcenter.__version__ import __title__ as DATABASE_NAME


class MongoTimeseriesHandler:
    worker: MongoTimeseriesWorker = None

    def __init__(
        self,
        connection_url: str = "mongodb://localhost:27017",
        database_name: str = DATABASE_NAME,
        flush_interval: int = 10,
        buffer_size: int = 200,
        expire_after: int = 1209600 # 14 days
    ) -> None:
        self._worker_factory = functools.partial(
            MongoTimeseriesWorker,
            connection_url,
            database_name,
            flush_interval,
            buffer_size,
            expire_after
        )

    def get_worker(self) -> MongoTimeseriesWorker:
        if self.worker is None:
            self.worker = self._worker_factory()
            self.worker.start()
        elif not self.worker.is_running():
            worker, self.worker = self.worker, None
            worker.stop()
            self.worker = self._worker_factory()
            self.worker.start()

        return self.worker

    @classmethod
    def flush(cls, block: bool = False):
        """Tell the worker to send any currently enqueued samples.
        
        Blocks until enqueued samples are sent if `block` is set.
        """
        if cls.worker is not None:
            cls.worker.flush(block)

    def send(self, sample: Dict[str, Any]):
        """Send a sample to the worker."""
        self.get_worker().enqueue(sample)

    def close(self) -> None:
        """Shuts down this handler and the flushes the worker."""
        if self.worker is not None:
            self.worker.flush()
            self.worker.stop()
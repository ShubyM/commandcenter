import logging
import queue
import sys
import threading
import warnings
from collections.abc import Iterable
from dataclasses import asdict
from typing import Any

import pymongo
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError, OperationFailure

from commandcenter.timeseries.models import TimeseriesDocument
from commandcenter.util import MongoWorker



_LOGGER = logging.getLogger("commandcenter.timeseries")


class TimeseriesWorker(MongoWorker):
    """Manages the submission of timeseries samples to MongoDB in a background
    thread.
    """
    def __init__(
        self,
        connection_url: str = "mongodb://localhost:27017",
        database_name: str | None = None,
        collection_name: str | None = None,
        flush_interval: int = 10,
        buffer_size: int = 200,
        max_retries: int = 3,
        expire_after: int = 2_592_000, # 14 days
        **kwargs: Any
    ) -> None:
        super().__init__(
            connection_url,
            database_name,
            collection_name,
            flush_interval,
            buffer_size,
            max_retries,
            **kwargs
        )
        self._expire_after = expire_after

    @classmethod
    def default_collection_name(cls) -> str:
        return "timeseries"

    def send(self, client: MongoClient, exiting: bool = False) -> None:
        done = False

        max_batch_size = self._buffer_size

        db = client[self._database_name]
        try:
            collection = Collection(db, self._collection_name)
            collection.create_index(
                [("timestamp", pymongo.ASCENDING), ("subscription", pymongo.ASCENDING)],
                unique=True
            )
            collection.create_index("expire", expireAfterSeconds=self._expire_after)
        except OperationFailure:
            warnings.warn(
                f"Attempted to set a different expiry for {self._collection_name} "
                "collection. An existing TTL index already exists and will be "
                "used instead.",
                stacklevel=2
            )
            collection = db[self._collection_name]

        # Loop until the queue is empty or we encounter an error
        while not done:
            try:
                while len(self._pending_documents) < max_batch_size:
                    document = self._queue.get_nowait()
                    self._pending_documents.append(document)
                    self._pending_size += sys.getsizeof(document)

            except queue.Empty:
                done = True

            if not self._pending_documents:
                continue
            
            try:
                try:
                    collection.insert_many(self._pending_documents, ordered=False)
                except BulkWriteError as e:
                    codes = [detail.get("code") == 11000 for detail in e.details.get("writeErrors", [])]
                    if codes and not all(codes):
                        raise e
                self._pending_documents.clear()
                self._pending_size = 0
                self._retries = 0
            except Exception:
                # Attempt to send on the next call instead
                done = True
                self._retries += 1

                _LOGGER.warning("Error in worker", exc_info=True)
                
                info = self.info
                
                if exiting:
                    _LOGGER.info("The worker is stopping", extra=info)
                elif self._retries > self._max_retries:
                    _LOGGER.error("Dropping samples", extra=info)
                else:
                    _LOGGER.info("Resending samples attempt %i of %i",
                        self._retries,
                        self._max_retries,
                        extra=info
                    )

                if self._retries > self._max_retries:
                    # Drop this batch of samples
                    self._pending_documents.clear()
                    self._pending_size = 0
                    self._retries = 0


class MongoTimeseriesHandler:
    """A handler that sends timeseries samples to MongoDB.

    Args:
        connection_url: Mongo DSN connection url.
        database_name: The database to save samples to.
        collection_name: The collection name to save samples to. Defaults to 'timeseries'.
        flush_interval: The time between flushes on the worker. Defaults to 10
            seconds.
        buffer_size: The number of samples that can be buffered before a flush
            is done to the database.
        max_retries: The maximum number of attempts to make sending a batch of
            samples before giving up on the batch. Defaults to 3
        expire_after: The value of the TTL index for samples. Defaults to having
            samples expire after 14 days.
    """
    worker: TimeseriesWorker = None

    def __init__(
        self,
        connection_url: str = "mongodb://localhost:27017",
        database_name: str | None = None,
        collection_name: str | None = None,
        flush_interval: int = 10,
        buffer_size: int = 200,
        max_retries: int = 3,
        expire_after: int = 1_209_600, # 14 days
        **kwargs: Any
    ) -> None:
        self._kwargs = {
            "connection_url": connection_url,
            "database_name": database_name,
            "collection_name": collection_name,
            "flush_interval": flush_interval,
            "buffer_size": buffer_size,
            "max_retries": max_retries,
            "expire_after": expire_after
        }
        self._kwargs.update(kwargs)
        self._lock = threading.Lock()

    def start_worker(self) -> TimeseriesWorker:
        """Start the timeseries worker thread."""
        worker = TimeseriesWorker(**self._kwargs)
        worker.start()
        worker.wait(timeout=5)
        if not worker.is_running:
            raise TimeoutError("Timed out waiting for worker.")
        return worker

    def get_worker(self) -> TimeseriesWorker:
        """Get a timeseries worker. If a worker does not exist or the worker
        is not running, a new worker is started.
        """
        if self.worker is None:
            worker = self.start_worker()
            self.worker = worker
        elif not self.worker.is_running:
            worker, self.worker = self.worker, None
            if not worker.is_stopped:
                worker.stop()
            worker = self.start_worker()
            self.worker = worker
        return self.worker

    def flush(self, block: bool = False):
        """Tell the worker to send any currently enqueued samples.
        
        Blocks until enqueued samples are sent if `block` is set.
        """
        with self._lock:
            if self.worker is not None:
                self.worker.flush(block)

    def publish(self, samples: Iterable[TimeseriesDocument]):
        """Publish a sample to the worker."""
        with self._lock:
            worker = self.get_worker()
            for sample in samples:
                worker.publish(asdict(sample))

    def close(self) -> None:
        """Shuts down this handler and the flushes the worker."""
        with self._lock:
            if self.worker is not None:
                self.worker.flush()
                self.worker.stop()
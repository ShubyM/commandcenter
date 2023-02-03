import atexit
import logging
import queue
import sys
import threading
import warnings
from typing import Any, Dict, List

import pymongo
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import (
    BulkWriteError,
    DuplicateKeyError,
    OperationFailure,
    PyMongoError
)



_LOGGER = logging.getLogger("commandcenter.timeseries")


class MongoTimeseriesWorker:
    """Manages the submission of timeseries samples to MongoDB in a background thread."""
    def __init__(
        self,
        connection_url: str,
        database_name: str,
        flush_interval: int,
        buffer_size: int,
        expire_after: int
    ) -> None:
        self.connection_url = connection_url
        self.database_name = database_name
        self.flush_interval = flush_interval
        self.buffer_size = buffer_size
        self.expire_after = expire_after

        self._queue: queue.Queue[dict] = queue.Queue()

        self._send_thread = threading.Thread(
            target=self._send_samples_loop,
            name="mongo-timeseries-worker",
            daemon=True,
        )
        self._flush_event = threading.Event()
        self._stop_event = threading.Event()
        self._send_samples_finished_event = threading.Event()
        self._lock = threading.Lock()
        self._started = False
        self._stopped = False  # Cannot be started again after stopped
        self._running = False

        # Tracks samples that have been pulled from the queue but not sent
        # successfully
        self._pending_samples: List[dict] = []
        self._pending_size: int = 0
        self._retries = 0
        self._max_retries = 3

        atexit.register(self.stop)

    def _send_samples_loop(self):
        """Should only be the target of the `send_thread` as it creates a new
        event loop. Runs until the `stop_event` is set.
        """
        try:
            with MongoClient(
                self.connection_url,
                maxPoolSize=1,
                serverSelectionTimeoutMS=10000
            ) as client:
                client.admin.command("ping")
                client.is_primary
                self._running = True
                while not self._stop_event.is_set():
                    # Wait until flush is called or the batch interval is reached
                    self._flush_event.wait(self.flush_interval)
                    self._flush_event.clear()

                    self._send_samples(client)

                    # Notify watchers that logs were sent
                    self._send_samples_finished_event.set()
                    self._send_samples_finished_event.clear()

                # After the stop event, we are exiting...
                # Try to send any remaining samples
                self._send_samples(client, True)

        except Exception:
            _LOGGER.error("The timeseries worker encountered a fatal error", exc_info=True)

        finally:
            # Set the finished event so anyone waiting on worker completion does
            # not continue to block if an exception is encountered
            self._send_samples_finished_event.set()
            self._running = False

    def _send_samples(self, client: MongoClient, exiting: bool = False) -> None:
        """
        Send all samples in the queue in batches to avoid network limits.

        If a client error is encountered, the samples pulled from the queue are
        retained and will be sent on the next call (up to 3 times).
        """
        done = False

        max_batch_size = self.buffer_size

        db = client[self.database_name]
        
        try:
            collection = Collection(db, "timeseries_data")
            collection.create_index(
                [("timestamp", pymongo.ASCENDING), ("subscription", pymongo.DESCENDING)],
                unique=True
            )
            collection.create_index("expire", expireAfterSeconds=self.expire_after)
        except OperationFailure:
            warnings.warn(
                "Attempted to set a different expiry for timeseries collection. "
                "An existing already exists and will be used instead.",
                stacklevel=2
            )
            collection = db["timeseries_data"]

        # Loop until the queue is empty or we encounter an error
        while not done:

            # Pull samples from the queue until it is empty or we reach the batch size
            try:
                while len(self._pending_samples) < max_batch_size:
                    sample = self._queue.get_nowait()
                    self._pending_samples.append(sample)
                    self._pending_size += sys.getsizeof(sample)

            except queue.Empty:
                done = True

            if not self._pending_samples:
                continue

            try:
                _LOGGER.debug("Writing %i samples", len(self._pending_samples))
                collection.insert_many(self._pending_samples, ordered=False)
                self._pending_samples = []
                self._pending_size = 0
                self._retries = 0
            except (BulkWriteError, DuplicateKeyError):
                self._pending_samples = []
                self._pending_size = 0
                self._retries = 0
            except PyMongoError:
                # Attempt to send these logs on the next call instead
                done = True
                self._retries += 1

                _LOGGER.warning("Error in timeseries worker", exc_info=True)
                
                stats = self.worker_info()
                
                if exiting:
                    _LOGGER.info("The timeseries worker is stopping", extra=stats)
                elif self._retries > self._max_retries:
                    _LOGGER.error(
                        "The timeseries worker has tried to send %i samples %i "
                        "times. These samples will now be dropped",
                        stats["pending_batch_length"],
                        self._max_retries,
                        extra=stats
                    )
                else:
                    _LOGGER.info(
                        "The timeseries worker will try and send these samples again",
                        extra=stats
                    )

                if self._retries > self._max_retries:
                    # Drop this batch of samples
                    self._pending_samples = []
                    self._pending_size = 0
                    self._retries = 0

    def worker_info(self) -> Dict[str, Any]:
        """Returns debugging information with worker sample stats."""
        return {
            "queue_length": self._queue.qsize(),
            "pending_batch_length": len(self._pending_samples),
            "pending_batch_size": self._pending_size
        }

    def enqueue(self, log: Dict[str, Any]):
        if self._stopped:
            raise RuntimeError(
                "Samples cannot be enqueued after the worker is stopped."
            )
        self._queue.put(log)

    def flush(self, block: bool = False) -> None:
        with self._lock:
            if not self._started and not self._stopped:
                raise RuntimeError("Worker was never started.")
            self._flush_event.set()
            if block:
                # TODO: Sometimes the log worker will deadlock and never finish
                # so we will only block for 30 seconds here.
                # Monitor Prefect Orion logging for fix.
                self._send_samples_finished_event.wait(30)

    def start(self) -> None:
        """Start the background thread."""
        with self._lock:
            if not self._started and not self._stopped:
                self._send_thread.start()
                self._started = True
            elif self._stopped:
                raise RuntimeError(
                    "The log worker cannot be started after stopping."
                )

    def stop(self) -> None:
        """Flush all samples and stop the background thread."""
        with self._lock:
            if self._started:
                self._flush_event.set()
                self._stop_event.set()
                self._send_thread.join()
                self._started = False
                self._stopped = True

    def is_stopped(self) -> bool:
        with self._lock:
            return not self._stopped

    def is_running(self) -> bool:
        with self._lock:
            return self._running
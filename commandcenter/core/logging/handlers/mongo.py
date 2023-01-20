import atexit
import functools
import logging
import queue
import sys
import threading
import traceback
from typing import Any, Dict, List

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import OperationFailure, PyMongoError



class MongoWorker:
    """Manages the submission of logs to MongoDB in a background thread."""
    def __init__(
        self,
        connection_url: str,
        database_name: str,
        flush_interval: int,
        buffer_size: int
    ) -> None:
        self.connection_url = connection_url
        self.database_name = database_name
        self.flush_interval = flush_interval
        self.buffer_size = buffer_size

        self._queue: queue.Queue[dict] = queue.Queue()

        self._send_thread = threading.Thread(
            target=self._send_logs_loop,
            name="mongo-log-worker",
            daemon=True,
        )
        self._flush_event = threading.Event()
        self._stop_event = threading.Event()
        self._send_logs_finished_event = threading.Event()
        self._lock = threading.Lock()
        self._started = False
        self._stopped = False  # Cannot be started again after stopped

        # Tracks logs that have been pulled from the queue but not sent successfully
        self._pending_logs: List[dict] = []
        self._pending_size: int = 0
        self._retries = 0
        self._max_retries = 3

        atexit.register(self.stop)

    def _send_logs_loop(self):
        """Should only be the target of the `send_thread` as it creates a new
        event loop. Runs until the `stop_event` is set.
        """
        # Initialize commandcenter in this new thread, but do not reconfigure logging
        try:
            with MongoClient(
                self.connection_url,
                maxPoolSize=1,
                serverSelectionTimeout=10000
            ) as client:
                client.admin.command("ping")
                client.is_primary
                while not self._stop_event.is_set():
                    # Wait until flush is called or the batch interval is reached
                    self._flush_event.wait(self.flush_interval)
                    self._flush_event.clear()

                    self._send_logs(client)

                    # Notify watchers that logs were sent
                    self._send_logs_finished_event.set()
                    self._send_logs_finished_event.clear()

                # After the stop event, we are exiting...
                # Try to send any remaining logs
                self._send_logs(client, True)

        except Exception:
            if logging.raiseExceptions and sys.stderr:
                sys.stderr.write("--- CommandCenter logging error ---\n")
                sys.stderr.write("The log worker encountered a fatal error.\n")
                traceback.print_exc(file=sys.stderr)
                sys.stderr.write(self.worker_info())

        finally:
            # Set the finished event so anyone waiting on worker completion does not
            # continue to block if an exception is encountered
            self._send_logs_finished_event.set()

    def _send_logs(self, client: MongoClient, exiting: bool = False) -> None:
        """
        Send all logs in the queue in batches to avoid network limits.

        If a client error is encountered, the logs pulled from the queue are retained
        and will be sent on the next call (up to 3 times).
        """
        done = False

        max_batch_size = self.buffer_size

        db = client[self.database_name]
        # We dont want to overwrite the existing logs collection so
        # we try and create a new collection in the DB which will
        # throw an error if the collection already exists
        try:
            collection = Collection(
                db,
                "logs",
                timeseries={"timeField": "timestamp"},
                expireAfterSeconds=7*24*60*60  # Retain logs for 7 days
            )
        except OperationFailure:
            collection = db["logs"]

        # Loop until the queue is empty or we encounter an error
        while not done:

            # Pull logs from the queue until it is empty or we reach the batch size
            try:
                while len(self._pending_logs) < max_batch_size:
                    log = self._queue.get_nowait()
                    self._pending_logs.append(log)
                    self._pending_size += sys.getsizeof(log)

            except queue.Empty:
                done = True

            if not self._pending_logs:
                continue

            try:
                collection.insert_many(self._pending_logs)
                self._pending_logs = []
                self._pending_size = 0
                self._retries = 0
            except PyMongoError:
                # Attempt to send these logs on the next call instead
                done = True
                self._retries += 1

                # Roughly replicate the behavior of the stdlib logger error handling
                if logging.raiseExceptions and sys.stderr:
                    sys.stderr.write("--- CommandCenter logging error ---\n")
                    traceback.print_exc(file=sys.stderr)
                    sys.stderr.write(self.worker_info())
                    if exiting:
                        sys.stderr.write(
                            "The log worker is stopping and these logs will not be sent.\n"
                        )
                    elif self._retries > self._max_retries:
                        sys.stderr.write(
                            "The log worker has tried to send these logs "
                            f"{self._retries} times and will now drop them."
                        )
                    else:
                        sys.stderr.write(
                            "The log worker will attempt to send these logs again in "
                            f"{self.flush_interval}s\n"
                        )

                if self._retries > self._max_retries:
                    # Drop this batch of logs
                    self._pending_logs = []
                    self._pending_size = 0
                    self._retries = 0

    def worker_info(self) -> str:
        """Returns a debugging string with worker log stats"""
        return (
            "Worker information:\n"
            f"    Approximate queue length: {self._queue.qsize()}\n"
            f"    Pending log batch length: {len(self._pending_logs)}\n"
            f"    Pending log batch size: {self._pending_size}\n"
        )

    def enqueue(self, log: Dict[str, Any]):
        if self._stopped:
            raise RuntimeError(
                "Logs cannot be enqueued after the log worker is stopped."
            )
        self._queue.put(log)

    def flush(self, block: bool = False) -> None:
        with self._lock:
            if not self._started and not self._stopped:
                raise RuntimeError("Worker was never started.")
            self._flush_event.set()
            if block:
                # TODO: Sometimes the log worker will deadlock and never finish
                # so we will only block for 30 seconds here. When logging is
                # refactored, this deadlock should be resolved.
                # Monitor Prefect Orion logging for fix.
                self._send_logs_finished_event.wait(30)

    def start(self) -> None:
        """Start the background thread"""
        with self._lock:
            if not self._started and not self._stopped:
                self._send_thread.start()
                self._started = True
            elif self._stopped:
                raise RuntimeError(
                    "The log worker cannot be started after stopping."
                )

    def stop(self) -> None:
        """Flush all logs and stop the background thread"""
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


class MongoHandler(logging.Handler):
    """A logging handler that sends logs to MongoDB.

    Sends log records to the log worker which manages sending batches of logs in
    the background.

    Args:
        connection_url: Mongo DSN connection url.
        database_name: The database to save logs to.
        flush_interval: The time between flushes on the log worker.
        flush_level: The log level which will trigger an automatic flush of the
            queue.
        buffer_size: The number of logs that can be buffered before a flush
            is done to the database.
    """
    worker: MongoWorker = None

    def __init__(
        self,
        connection_url: str,
        database_name: str,
        flush_interval: float = 10,
        flush_level: int = logging.ERROR,
        buffer_size: int = 100
    ) -> None:
        self._worker_factory = functools.partial(
            MongoWorker,
            connection_url,
            database_name,
            flush_interval,
            buffer_size
        )
        self._flush_level = flush_level

    def get_worker(self) -> MongoWorker:
        if self.worker is None:
            self.worker = self._worker_factory()
            self.worker.start()

        return self.worker

    @classmethod
    def flush(cls, block: bool = False):
        """Tell the log worker to send any currently enqueued logs.
        
        Blocks until enqueued logs are sent if `block` is set.
        """
        if cls.worker is not None:
            cls.worker.flush(block)

    def emit(self, record: logging.LogRecord):
        """Send a log to the log worker."""
        try:
            self.get_worker().enqueue(self.format(record))
        except Exception:
            self.handleError(record)
        else:
            if record.levelno == self._flush_level:
                self.get_worker().flush()

    def close(self) -> None:
        """Shuts down this handler and the flushes the log worker."""
        if self.worker is not None:
            # Flush the worker instead of stopping because another instance may
            # be using it
            self.worker.flush()

        return super().close()
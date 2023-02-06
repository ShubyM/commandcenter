import atexit
import logging
import queue
import threading
from typing import Any, Dict, List

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from commandcenter.__version__ import __title__ as DATABASE_NAME



_LOGGER = logging.getLogger("commandcenter.util.db")


class MongoWorker:
    """Manages the submission of documents to MongoDB in a background thread."""
    def __init__(
        self,
        connection_url: str = "mongodb://localhost:27017",
        database_name: str | None = None,
        collection_name: str | None = None,
        flush_interval: int = 10,
        buffer_size: int = 200,
        max_retries: int = 3
    ) -> None:
        self._connection_url = connection_url
        self._database_name = database_name or DATABASE_NAME
        self._collection_name = collection_name or self.default_collection_name()
        self._flush_interval = flush_interval
        self._buffer_size = buffer_size
        self._max_retries = max_retries

        self._runner = threading.Thread(target=self._run, daemon=True)

        self._queue: queue.Queue[Dict[Any, Any]] = queue.Queue()
        self._lock = threading.Lock()
        self._flush_event = threading.Event()
        self._stop_event = threading.Event()
        self._send_finished_event = threading.Event()
        self._running_event = threading.Event()
        self._started = False
        self._stopped = False

        # Tracks documents that have been pulled from the queue but not sent
        # successfully
        self._pending_documents: List[Dict[Any, Any]] = []
        self._pending_size: int = 0
        self._retries = 0

        atexit.register(self.stop)

    @property
    def info(self) -> Dict[str, Any]:
        """Returns debugging information with worker sample stats."""
        return {
            "queue_length": self._queue.qsize(),
            "pending_batch_length": len(self._pending_documents),
            "pending_batch_size": self._pending_size
        }

    @property
    def is_running(self) -> bool:
        """`True` if worker can process documents."""
        with self._lock:
            return self._running_event.is_set()
    
    @property
    def is_stopped(self) -> bool:
        """`True` if worker is stopped."""
        with self._lock:
            return not self._stopped

    @classmethod
    def default_collection_name(cls) -> str:
        """Define a default collection for the worker."""
        raise NotImplementedError()

    def start(self) -> None:
        """Start the background thread."""
        with self._lock:
            if not self._started and not self._stopped:
                self._runner.start()
                self._started = True
            elif self._stopped:
                raise RuntimeError(
                    "The log worker cannot be started after stopping."
                )

    def stop(self) -> None:
        """Flush all documents and stop the background thread."""
        with self._lock:
            if self._started:
                self._flush_event.set()
                self._stop_event.set()
                self._runner.join()
                self._started = False
                self._stopped = True

    def flush(self, block: bool = False) -> None:
        """Flush all documents to the database."""
        with self._lock:
            if not self._started and not self._stopped:
                raise RuntimeError("Worker was never started.")
            self._flush_event.set()
            if block:
                self._send_finished_event.wait(30)

    def publish(self, document: Dict[Any, Any]):
        """Enqueue a document for the worker to submit."""
        with self._lock:
            if self._stopped:
                raise RuntimeError(
                    "Samples cannot be enqueued after the worker is stopped."
                )
            self._queue.put_nowait(document)

    def send(self, client: MongoClient, exiting: bool = False) -> None:
        """Send all documents in the queue in batches to avoid network limits.

        If a client error is encountered, the samples pulled from the queue should
        be retained up to `max_retries` times.
        """
        raise NotImplementedError()

    def wait(self, timeout: float) -> None:
        """Wait for worker to establish connection to MongoDB."""
        self._running_event.wait(timeout)

    def _run(self):
        try:
            with MongoClient(
                self._connection_url,
                maxPoolSize=1,
                serverSelectionTimeoutMS=10000
            ) as client:
                pong = client.admin.command("ping")
                if not pong.get("ok"):
                    ConnectionFailure("Unable to ping server.")
                self._running_event.set()
                while not self._stop_event.is_set():
                    self._flush_event.wait(self._flush_interval)
                    self._flush_event.clear()
                    
                    self.send(client)

                    self._send_finished_event.set()
                    self._send_finished_event.clear()

                # After the stop event, we are exiting...
                # Try to send any remaining pending documents
                self.send(client, True)
        except Exception:
            _LOGGER.error("The worker encountered a fatal error", exc_info=True)
        finally:
            self._send_finished_event.set()
            self._running_event.clear()
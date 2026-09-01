from queue import Full, Queue, ShutDown
from threading import Semaphore


class SyncQueue[T]:
    """Thread-safe queue bounding in-flight items (waiting + being processed)

    - drop-in replacement for queue.Queue
    - queue.Queue(maxsize=N) only bounds waiting items, not in-flight
    - uses a Semaphore: acquire on put, release on task_done
    - total in-flight (queued + being processed) never exceeds maxsize
    - supports Queue, PriorityQueue, or LifoQueue as the inner queue
    - shutdown wakes blocked putters via a cascade through the semaphore
    """

    def __init__(self, maxsize: int, queue_cls: type[Queue[T]] = Queue) -> None:
        if maxsize < 1:
            raise ValueError("SyncQueue requires maxsize >= 1")
        self._sem = Semaphore(value=maxsize)
        self._queue = queue_cls(maxsize=maxsize)
        self._maxsize = maxsize

    def put(self, item: T, block: bool = True, timeout: float | None = None) -> None:
        if not self._sem.acquire(blocking=block, timeout=timeout):
            raise Full()
        try:
            self._queue.put_nowait(item)
        except ShutDown:
            self._sem.release()
            raise

    def put_nowait(self, item: T) -> None:
        self.put(item, block=False)

    def get(self, block: bool = True, timeout: float | None = None) -> T:
        return self._queue.get(block=block, timeout=timeout)

    def get_nowait(self) -> T:
        return self._queue.get_nowait()

    def task_done(self) -> None:
        self._queue.task_done()
        self._sem.release()

    def join(self) -> None:
        self._queue.join()

    def qsize(self) -> int:
        return self._maxsize - self._sem._value

    def empty(self) -> bool:
        return self.qsize() == 0

    def full(self) -> bool:
        return self.qsize() == self._maxsize

    def shutdown(self, immediate: bool = False) -> None:
        self._queue.shutdown(immediate=immediate)
        self._sem.release()

import asyncio
from asyncio import Queue, QueueFull, QueueShutDown


class AsyncQueue[T]:
    """Async queue bounding in-flight items (waiting + being processed)

    - drop-in replacement for asyncio.Queue
    - asyncio.Queue(maxsize=N) only bounds waiting items, not in-flight
    - uses an asyncio.Semaphore: acquire on put, release on task_done
    - total in-flight (queued + being processed) never exceeds maxsize
    - supports Queue, PriorityQueue, or LifoQueue as the inner queue
    - shutdown wakes blocked putters via a cascade through the semaphore
    """

    def __init__(self, maxsize: int, queue_cls: type[Queue[T]] = Queue) -> None:
        if maxsize < 1:
            raise ValueError("AsyncQueue requires maxsize >= 1")
        self._sem = asyncio.Semaphore(value=maxsize)
        self._queue = queue_cls(maxsize=maxsize)
        self._maxsize = maxsize

    async def put(self, item: T) -> None:
        await self._sem.acquire()
        try:
            self._queue.put_nowait(item)
        except QueueShutDown:
            self._sem.release()
            raise

    def put_nowait(self, item: T) -> None:
        if not self._sem.locked() and self._sem._value > 0:
            self._sem._value -= 1
        else:
            raise QueueFull()
        try:
            self._queue.put_nowait(item)
        except QueueShutDown:
            self._sem.release()
            raise

    async def get(self) -> T:
        return await self._queue.get()

    def get_nowait(self) -> T:
        return self._queue.get_nowait()

    def task_done(self) -> None:
        self._queue.task_done()
        self._sem.release()

    async def join(self) -> None:
        await self._queue.join()

    def qsize(self) -> int:
        return self._maxsize - self._sem._value

    def empty(self) -> bool:
        return self.qsize() == 0

    def full(self) -> bool:
        return self.qsize() == self._maxsize

    def shutdown(self, immediate: bool = False) -> None:
        self._queue.shutdown(immediate=immediate)
        self._sem.release()

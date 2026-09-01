# semqueue

[![PyPI version](https://img.shields.io/pypi/v/semqueue)](https://pypi.org/project/semqueue/)
[![Python versions](https://img.shields.io/pypi/pyversions/semqueue)](https://pypi.org/project/semqueue/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/github/actions/workflow/status/vmuminov/semqueue/tests.yml?label=tests)](https://github.com/vmuminov/semqueue/actions)

Queues that bound **in-flight** items (waiting + being processed), not just waiting items

- `SyncQueue` - drop-in replacement for `queue.Queue`
- `AsyncQueue` - drop-in replacement for `asyncio.Queue`
- Both support `Queue`, `PriorityQueue`, or `LifoQueue` as the inner queue via `queue_cls`

## Why

Backpressure. When producers are faster than consumers and downstream processing is expensive (memory, connections, rate limits), you want to bound the total work in flight, not just the queue depth

## The problem with `queue.Queue` / `asyncio.Queue`

- `maxsize=N` only bounds items waiting in the queue
- consumers pull items and process them concurrently
- total items in the system (queued + being processed) can exceed N

`semqueue` fixes this by using a Semaphore:
- acquire on `put`, release on `task_done`
- total in-flight (queued + being processed) never exceeds `maxsize`

## Installation

```bash
pip install semqueue
```

## Usage - SyncQueue

### Basic

```python
from semqueue import SyncQueue

q = SyncQueue(maxsize=4)

# Use PriorityQueue or LifoQueue as the inner queue
from queue import PriorityQueue

q = SyncQueue(maxsize=4, queue_cls=PriorityQueue)
```

### Producer / consumer

```python
import threading
from semqueue import SyncQueue

q = SyncQueue(maxsize=4)


def producer():
    for i in range(100):
        q.put(i)  # blocks if 4 items are in-flight


def consumer():
    while True:
        item = q.get()
        process(item)
        q.task_done()  # frees a slot


threading.Thread(target=producer).start()
threading.Thread(target=consumer).start()
```

### Timeout and non-blocking puts

```python
from semqueue import SyncQueue, Full

q = SyncQueue(maxsize=2)
q.put("a")
q.put("b")

try:
    q.put("c", block=True, timeout=0.5)
except Full:
    print("in-flight limit reached")

try:
    q.put_nowait("c")
except Full:
    print("would exceed in-flight limit")
```

`Full` is `queue.Full` re-exported, so existing code works unchanged.

### Shutdown

```python
from semqueue import SyncQueue
from queue import ShutDown

q = SyncQueue(maxsize=4)

q.shutdown()  # graceful: stops new puts, consumers drain
q.shutdown(immediate=True)  # immediate: drains queue, unblocks join()

q.put("x")  # raises ShutDown
q.get()  # raises ShutDown if empty
```

## Usage - AsyncQueue

### Basic

```python
from semqueue import AsyncQueue

q = AsyncQueue(maxsize=4)

# Use PriorityQueue or LifoQueue as the inner queue
from asyncio import PriorityQueue

q = AsyncQueue(maxsize=4, queue_cls=PriorityQueue)
```

### Producer / consumer

```python
import asyncio
from semqueue import AsyncQueue

q = AsyncQueue(maxsize=4)


async def producer():
    for i in range(100):
        await q.put(i)  # blocks if 4 items are in-flight


async def consumer():
    while True:
        item = await q.get()
        await process(item)
        q.task_done()  # frees a slot


async def main():
    await asyncio.gather(producer(), consumer())


asyncio.run(main())
```

### Non-blocking puts

```python
from semqueue import AsyncQueue, QueueFull

q = AsyncQueue(maxsize=2)
await q.put("a")
await q.put("b")

try:
    q.put_nowait("c")
except QueueFull:
    print("would exceed in-flight limit")
```

`QueueFull` is `asyncio.QueueFull` re-exported, so existing code works unchanged.

### Shutdown

```python
from semqueue import AsyncQueue
from asyncio import QueueShutDown

q = AsyncQueue(maxsize=4)

q.shutdown()  # graceful: stops new puts, consumers drain
q.shutdown(immediate=True)  # immediate: drains queue, unblocks join()

await q.put("x")  # raises QueueShutDown
await q.get()  # raises QueueShutDown if empty
```

## License

MIT

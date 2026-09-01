# Changelog

## 0.1.0

- Initial implementation of `SyncQueue`, a drop-in replacement for `queue.Queue` that bounds in-flight items (waiting + being processed) via an internal semaphore.
- Initial implementation of `AsyncQueue`, a drop-in replacement for `asyncio.Queue` with the same in-flight bounding semantics.
- `Full` exception (subclass of `queue.Full`) for `SyncQueue`.
- `QueueFull` exception (subclass of `asyncio.QueueFull`) for `AsyncQueue`.
- Full `queue.Queue` / `asyncio.Queue` API support including `shutdown()` (Python 3.13+).
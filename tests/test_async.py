import asyncio

import pytest

from semqueue import AsyncQueue, QueueFull

QUEUE_TYPES = [None, asyncio.Queue, asyncio.PriorityQueue, asyncio.LifoQueue]
QUEUE_IDS = ["default", "Queue", "PriorityQueue", "LifoQueue"]


def _make_q(maxsize, queue_cls=None):
    if queue_cls is None:
        return AsyncQueue(maxsize=maxsize)
    return AsyncQueue(maxsize=maxsize, queue_cls=queue_cls)


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_provides_queue_interface(queue_cls):
    q = _make_q(1, queue_cls)
    for method in (
        "put",
        "put_nowait",
        "get",
        "get_nowait",
        "task_done",
        "join",
        "qsize",
        "empty",
        "full",
        "shutdown",
    ):
        assert hasattr(q, method)


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_basic_put_get(queue_cls):
    q = _make_q(3, queue_cls)
    await q.put("a")
    assert await q.get() == "a"
    q.task_done()
    assert q.qsize() == 0
    assert q.empty() is True
    await q.join()


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_qsize_reflects_in_flight(queue_cls):
    q = _make_q(3, queue_cls)
    await q.put("a")
    await q.put("b")
    assert q.qsize() == 2
    await q.get()
    assert q.qsize() == 2
    q.task_done()
    assert q.qsize() == 1
    await q.get()
    q.task_done()
    assert q.qsize() == 0


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_empty_reflects_in_flight(queue_cls):
    q = _make_q(2, queue_cls)
    assert q.empty() is True
    await q.put("a")
    assert q.empty() is False
    await q.get()
    assert q.empty() is False
    q.task_done()
    assert q.empty() is True


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_full_false_initially(queue_cls):
    q = _make_q(2, queue_cls)
    assert q.full() is False


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_in_flight_bounds_admission_even_with_queue_room(queue_cls):
    q = _make_q(1, queue_cls)
    await q.put("a")
    await q.get()
    with pytest.raises(QueueFull):
        q.put_nowait("b")


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_full_reflects_in_flight_not_queue_depth(queue_cls):
    q = _make_q(1, queue_cls)
    await q.put("a")
    await q.get()
    assert q.full() is True


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_put_nowait_raises_at_capacity(queue_cls):
    q = _make_q(1, queue_cls)
    await q.put("a")
    with pytest.raises(QueueFull):
        q.put_nowait("b")


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_get_raises_when_empty(queue_cls):
    q = _make_q(1, queue_cls)
    with pytest.raises(asyncio.QueueEmpty):
        q.get_nowait()


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_task_done_overcall_raises_value_error(queue_cls):
    q = _make_q(1, queue_cls)
    await q.put("a")
    await q.get()
    q.task_done()
    with pytest.raises(ValueError):
        q.task_done()


def test_constructor_rejects_invalid_maxsize():
    with pytest.raises(ValueError):
        AsyncQueue(maxsize=0)
    with pytest.raises(ValueError):
        AsyncQueue(maxsize=-1)


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_blocked_put_unblocks_on_task_done(queue_cls):
    q = _make_q(1, queue_cls)
    await q.put("a")

    done = asyncio.Event()

    async def worker():
        await q.put("b")
        done.set()

    t = asyncio.create_task(worker())
    await asyncio.sleep(0.1)
    assert not done.is_set()

    await q.get()
    q.task_done()
    await asyncio.wait_for(t, timeout=2.0)
    assert done.is_set()


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_put_timeout_raises_after_waiting(queue_cls):
    q = _make_q(1, queue_cls)
    await q.put("a")
    start = asyncio.get_event_loop().time()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(q.put("b"), timeout=0.3)
    elapsed = asyncio.get_event_loop().time() - start
    assert elapsed >= 0.3


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_join_unblocks_on_task_done(queue_cls):
    q = _make_q(2, queue_cls)
    await q.put("a")
    await q.put("b")

    finished = asyncio.Event()

    async def joiner():
        await q.join()
        finished.set()

    t = asyncio.create_task(joiner())
    await asyncio.sleep(0.1)
    assert not finished.is_set()

    await q.get()
    q.task_done()
    await q.get()
    q.task_done()

    await asyncio.wait_for(t, timeout=2.0)
    assert finished.is_set()


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_concurrency_never_exceeds_maxsize(queue_cls):
    maxsize = 4
    items = 200
    q = _make_q(maxsize, queue_cls)

    active = 0
    observed_max = 0
    consumed = 0

    async def producer():
        for _ in range(items // 4):
            await q.put(1)

    async def consumer():
        nonlocal active, observed_max, consumed
        while True:
            try:
                await asyncio.wait_for(q.get(), timeout=0.5)
            except TimeoutError:
                return
            active += 1
            observed_max = max(observed_max, active)
            await asyncio.sleep(0.001)
            active -= 1
            consumed += 1
            q.task_done()

    producers = [asyncio.create_task(producer()) for _ in range(4)]
    consumers = [asyncio.create_task(consumer()) for _ in range(4)]
    await asyncio.gather(*producers)
    for c in consumers:
        await c

    assert observed_max <= maxsize
    assert consumed == items


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_raised_exception_is_instance_of_queue_full(queue_cls):
    q = _make_q(1, queue_cls)
    await q.put("a")
    try:
        q.put_nowait("b")
    except asyncio.QueueFull as exc:
        assert isinstance(exc, QueueFull)
    else:
        pytest.fail("expected QueueFull to be raised")


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_shutdown_blocks_new_puts(queue_cls):
    q = _make_q(3, queue_cls)
    q.shutdown()
    with pytest.raises(asyncio.QueueShutDown):
        q.put_nowait("a")


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_shutdown_wakes_blocked_put(queue_cls):
    q = _make_q(1, queue_cls)
    await q.put("a")

    errors = []

    async def worker():
        try:
            await q.put("b")
        except asyncio.QueueShutDown:
            errors.append("shutdown")

    t = asyncio.create_task(worker())
    await asyncio.sleep(0.1)

    q.shutdown()
    await asyncio.wait_for(t, timeout=2.0)

    assert errors == ["shutdown"]


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_shutdown_immediate_drains_and_unblocks_join(queue_cls):
    q = _make_q(3, queue_cls)
    await q.put("a")
    await q.put("b")

    finished = asyncio.Event()

    async def joiner():
        await q.join()
        finished.set()

    t = asyncio.create_task(joiner())
    await asyncio.sleep(0.1)
    assert not finished.is_set()

    q.shutdown(immediate=True)
    await asyncio.wait_for(t, timeout=2.0)
    assert finished.is_set()


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_shutdown_get_raises_when_empty(queue_cls):
    q = _make_q(3, queue_cls)
    q.shutdown()
    with pytest.raises(asyncio.QueueShutDown):
        await q.get()


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_shutdown_wakes_blocked_get(queue_cls):
    q = _make_q(3, queue_cls)

    errors = []

    async def worker():
        try:
            await q.get()
        except asyncio.QueueShutDown:
            errors.append("shutdown")

    t = asyncio.create_task(worker())
    await asyncio.sleep(0.1)

    q.shutdown()
    await asyncio.wait_for(t, timeout=2.0)

    assert errors == ["shutdown"]


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_shutdown_cascades_to_all_blocked_putters(queue_cls):
    q = _make_q(1, queue_cls)
    await q.put("a")

    errors = []

    async def worker(item):
        try:
            await q.put(item)
        except asyncio.QueueShutDown:
            errors.append(item)

    tasks = [asyncio.create_task(worker(i)) for i in range(5)]
    await asyncio.sleep(0.3)

    q.shutdown()
    for t in tasks:
        await asyncio.wait_for(t, timeout=5.0)

    assert len(errors) == 5


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
async def test_put_releases_permit_if_inner_queue_shut_down(queue_cls):
    q = _make_q(1, queue_cls)
    await q.put("a")
    await q.get()
    q.task_done()
    q._queue.shutdown()
    with pytest.raises(asyncio.QueueShutDown):
        q.put_nowait("b")
    assert q.full() is False

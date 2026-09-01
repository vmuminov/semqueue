import queue
import threading
import time

import pytest

from semqueue import Full, SyncQueue

QUEUE_TYPES = [None, queue.Queue, queue.PriorityQueue, queue.LifoQueue]
QUEUE_IDS = ["default", "Queue", "PriorityQueue", "LifoQueue"]


def _make_q(maxsize, queue_cls=None):
    if queue_cls is None:
        return SyncQueue(maxsize=maxsize)
    return SyncQueue(maxsize=maxsize, queue_cls=queue_cls)


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_basic_put_get(queue_cls):
    q = _make_q(3, queue_cls)
    q.put("a")
    assert q.get() == "a"
    q.task_done()
    assert q.qsize() == 0
    assert q.empty() is True
    q.join()


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_qsize_reflects_in_flight(queue_cls):
    q = _make_q(3, queue_cls)
    q.put("a")
    q.put("b")
    assert q.qsize() == 2
    q.get()
    assert q.qsize() == 2
    q.task_done()
    assert q.qsize() == 1
    q.get()
    q.task_done()
    assert q.qsize() == 0


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_empty_reflects_in_flight(queue_cls):
    q = _make_q(2, queue_cls)
    assert q.empty() is True
    q.put("a")
    assert q.empty() is False
    q.get()
    assert q.empty() is False
    q.task_done()
    assert q.empty() is True


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
def test_put_nowait_succeeds_when_room(queue_cls):
    q = _make_q(2, queue_cls)
    q.put_nowait("a")
    q.put_nowait("b")
    assert q.qsize() == 2
    q.get()
    q.task_done()
    q.get()
    q.task_done()
    q.join()


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_full_false_initially(queue_cls):
    q = _make_q(2, queue_cls)
    assert q.full() is False


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_semaphore_bounds_admission_even_with_queue_room(queue_cls):
    q = _make_q(1, queue_cls)
    q.put("a")
    q.get()
    with pytest.raises(Full):
        q.put("b", block=False)


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_full_reflects_in_flight_not_queue_depth(queue_cls):
    q = _make_q(1, queue_cls)
    q.put("a")
    q.get()
    assert q.full() is True


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_put_timeout_raises_after_waiting(queue_cls):
    q = _make_q(1, queue_cls)
    q.put("a")
    start = time.monotonic()
    with pytest.raises(Full):
        q.put("b", block=True, timeout=0.3)
    elapsed = time.monotonic() - start
    assert elapsed >= 0.3
    assert elapsed < 1.0


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_blocked_put_unblocks_on_task_done(queue_cls):
    q = _make_q(1, queue_cls)
    q.put("a")

    result = {}

    def worker():
        q.put("b", block=True, timeout=2.0)
        result["done"] = True

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(0.2)
    assert "done" not in result

    q.get()
    q.task_done()
    t.join(timeout=2.0)

    assert result.get("done") is True


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_put_nowait_raises_immediately_at_capacity(queue_cls):
    q = _make_q(1, queue_cls)
    q.put("a")
    start = time.monotonic()
    with pytest.raises(Full):
        q.put_nowait("b")
    elapsed = time.monotonic() - start
    assert elapsed < 0.1


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_get_and_get_nowait_raise_empty(queue_cls):
    q = _make_q(1, queue_cls)
    with pytest.raises(queue.Empty):
        q.get_nowait()
    with pytest.raises(queue.Empty):
        q.get(block=True, timeout=0.1)


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_task_done_overcall_raises_value_error(queue_cls):
    q = _make_q(1, queue_cls)
    q.put("a")
    q.get()
    q.task_done()
    with pytest.raises(ValueError):
        q.task_done()


def test_constructor_rejects_invalid_maxsize():
    with pytest.raises(ValueError):
        SyncQueue(maxsize=0)
    with pytest.raises(ValueError):
        SyncQueue(maxsize=-1)


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_join_unblocks_on_task_done(queue_cls):
    q = _make_q(2, queue_cls)
    q.put("a")
    q.put("b")

    finished = threading.Event()

    def joiner():
        q.join()
        finished.set()

    t = threading.Thread(target=joiner)
    t.start()
    time.sleep(0.1)
    assert not finished.is_set()

    q.get()
    q.task_done()
    q.get()
    q.task_done()

    assert finished.wait(timeout=2.0)
    t.join(timeout=2.0)


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_concurrency_never_exceeds_maxsize(queue_cls):
    maxsize = 4
    items = 200
    q = _make_q(maxsize, queue_cls)

    active = [0]
    observed_max = [0]
    consumed = [0]
    produced = [0]
    lock = threading.Lock()

    def producer():
        while True:
            with lock:
                if produced[0] >= items:
                    return
                produced[0] += 1
            q.put(1, block=True, timeout=5.0)

    def consumer():
        while True:
            try:
                q.get(block=True, timeout=0.5)
            except queue.Empty:
                return
            with lock:
                active[0] += 1
                observed_max[0] = max(observed_max[0], active[0])
            time.sleep(0.001)
            with lock:
                active[0] -= 1
                consumed[0] += 1
            q.task_done()

    threads = [threading.Thread(target=producer) for _ in range(4)] + [
        threading.Thread(target=consumer) for _ in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert not any(t.is_alive() for t in threads)
    assert observed_max[0] <= maxsize
    assert consumed[0] == items


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_raised_exception_is_instance_of_queue_full(queue_cls):
    q = _make_q(1, queue_cls)
    q.put("a")
    try:
        q.put_nowait("b")
    except queue.Full as exc:
        assert isinstance(exc, Full)
    else:
        pytest.fail("expected Full to be raised")


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_shutdown_blocks_new_puts(queue_cls):
    q = _make_q(3, queue_cls)
    q.shutdown()
    with pytest.raises(queue.ShutDown):
        q.put("a")


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_shutdown_immediate_drains_and_unblocks_join(queue_cls):
    q = _make_q(3, queue_cls)
    q.put("a")
    q.put("b")

    finished = threading.Event()

    def joiner():
        q.join()
        finished.set()

    t = threading.Thread(target=joiner)
    t.start()
    time.sleep(0.1)
    assert not finished.is_set()

    q.shutdown(immediate=True)
    assert finished.wait(timeout=2.0)
    t.join(timeout=2.0)


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_shutdown_wakes_blocked_put(queue_cls):
    q = _make_q(1, queue_cls)
    q.put("a")

    errors = []

    def worker():
        try:
            q.put("b", block=True, timeout=5.0)
        except queue.ShutDown:
            errors.append("shutdown")

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(0.2)

    q.shutdown()
    t.join(timeout=2.0)

    assert errors == ["shutdown"]


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_shutdown_wakes_blocked_put_untimed(queue_cls):
    q = _make_q(1, queue_cls)
    q.put("a")

    errors = []

    def worker():
        try:
            q.put("b", block=True, timeout=None)
        except queue.ShutDown:
            errors.append("shutdown")

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(0.2)

    q.shutdown()
    t.join(timeout=2.0)

    assert errors == ["shutdown"]


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_shutdown_cascades_to_all_blocked_putters(queue_cls):
    q = _make_q(1, queue_cls)
    q.put("a")

    errors = []
    errors_lock = threading.Lock()

    def worker(item):
        try:
            q.put(item, block=True, timeout=5.0)
        except queue.ShutDown:
            with errors_lock:
                errors.append(item)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    time.sleep(0.3)

    q.shutdown()
    for t in threads:
        t.join(timeout=5.0)

    assert len(errors) == 5


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_shutdown_get_raises_when_empty(queue_cls):
    q = _make_q(3, queue_cls)
    q.shutdown()
    with pytest.raises(queue.ShutDown):
        q.get(block=True, timeout=0.5)


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_put_releases_permit_if_inner_queue_shut_down(queue_cls):
    q = _make_q(1, queue_cls)
    q.put("a")
    q.get()
    q.task_done()
    q._queue.shutdown()
    with pytest.raises(queue.ShutDown):
        q.put("b", block=False)
    assert q.full() is False


@pytest.mark.parametrize("queue_cls", QUEUE_TYPES, ids=QUEUE_IDS)
def test_blocked_put_releases_permit_if_inner_queue_shut_down(queue_cls):
    q = _make_q(1, queue_cls)
    q.put("a")
    q.get()
    q.task_done()
    q._queue.shutdown()
    with pytest.raises(queue.ShutDown):
        q.put("b", block=True, timeout=0.5)
    assert q.full() is False

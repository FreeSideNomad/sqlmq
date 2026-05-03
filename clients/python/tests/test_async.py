"""Async smoke tests mirroring the sync coverage."""

from __future__ import annotations

import time

import pytest

from sqlmq import Message, QueueMetrics
from tests.conftest import unique_queue_name


pytestmark = pytest.mark.asyncio


async def test_async_create_drop(async_queue_factory):
    q = async_queue_factory()
    await q.init()
    name = unique_queue_name()
    await q.create_queue(name)
    assert name in await q.list_queues()
    assert await q.drop_queue(name) is True
    assert await q.drop_queue(name) is False


async def test_async_send_read(async_queue_factory):
    q = async_queue_factory()
    name = unique_queue_name()
    await q.create_queue(name)
    msg_id = await q.send(name, {"a": 1})
    assert isinstance(msg_id, int) and msg_id >= 1
    got = await q.read(name)
    assert isinstance(got, Message)
    assert got.message == {"a": 1}


async def test_async_send_batch_read_batch(async_queue_factory):
    q = async_queue_factory()
    name = unique_queue_name()
    await q.create_queue(name)
    ids = await q.send_batch(name, [{"i": i} for i in range(3)])
    assert len(ids) == 3
    msgs = await q.read_batch(name, batch_size=10)
    assert len(msgs) == 3


async def test_async_pop(async_queue_factory):
    q = async_queue_factory()
    name = unique_queue_name()
    await q.create_queue(name)
    await q.send(name, {"x": 1})
    popped = await q.pop(name)
    assert popped is not None and popped.message == {"x": 1}
    assert await q.pop(name) is None


async def test_async_delete_archive(async_queue_factory):
    q = async_queue_factory()
    name = unique_queue_name()
    await q.create_queue(name)
    a, b = await q.send(name, {"i": 1}), await q.send(name, {"i": 2})
    assert await q.delete(name, a) is True
    assert await q.archive(name, b) is True


async def test_async_purge_metrics(async_queue_factory):
    q = async_queue_factory()
    name = unique_queue_name()
    await q.create_queue(name)
    await q.send_batch(name, [{"i": i} for i in range(3)])
    m = await q.metrics(name)
    assert isinstance(m, QueueMetrics)
    assert m.queue_length == 3
    assert await q.purge(name) == 3


async def test_async_read_with_poll_returns_empty_after_deadline(async_queue_factory):
    q = async_queue_factory()
    name = unique_queue_name()
    await q.create_queue(name)
    t0 = time.monotonic()
    msgs = await q.read_with_poll(name, max_poll_seconds=1, poll_interval_ms=100)
    assert msgs == []
    assert time.monotonic() - t0 < 2.5


async def test_async_unsupported_methods_raise(async_queue_factory):
    q = async_queue_factory()
    name = unique_queue_name()
    await q.create_queue(name)
    with pytest.raises(NotImplementedError):
        await q.create_queue(unique_queue_name(), unlogged=True)
    with pytest.raises(NotImplementedError):
        await q.create_partitioned_queue(unique_queue_name())
    with pytest.raises(NotImplementedError):
        await q.set_vt(name, 1, 60)
    with pytest.raises(NotImplementedError):
        await q.detach_archive(name)

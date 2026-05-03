"""send, send_batch, read, read_batch."""

from __future__ import annotations

import pytest

from sqlmq import Message
from tests.conftest import unique_queue_name


def test_send_returns_msg_id(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    msg_id = q.send(name, {"hello": "world"})
    assert isinstance(msg_id, int) and msg_id >= 1


def test_read_returns_message(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    msg_id = q.send(name, {"x": 1, "nested": {"y": 2}})
    got = q.read(name)
    assert isinstance(got, Message)
    assert got.msg_id == msg_id
    assert got.read_ct == 1
    assert got.message == {"x": 1, "nested": {"y": 2}}


def test_read_returns_none_for_empty_queue(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    assert q.read(name) is None


def test_send_batch_returns_ids(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    ids = q.send_batch(name, [{"i": i} for i in range(5)])
    assert len(ids) == 5
    assert all(isinstance(i, int) for i in ids)
    assert ids == sorted(ids)  # monotonic


def test_send_batch_empty_returns_empty(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    assert q.send_batch(name, []) == []


def test_read_batch_returns_multiple(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    q.send_batch(name, [{"i": i} for i in range(3)])
    msgs = q.read_batch(name, batch_size=10)
    assert len(msgs) == 3
    assert sorted([m.message["i"] for m in msgs]) == [0, 1, 2]


def test_read_batch_empty_returns_empty_list(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    assert q.read_batch(name, batch_size=5) == []


def test_send_with_delay_makes_message_invisible_briefly(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    q.send(name, {"a": 1}, delay=2)
    assert q.read(name) is None  # still invisible

    import time as _t
    _t.sleep(2.5)
    msg = q.read(name)
    assert msg is not None
    assert msg.message == {"a": 1}


def test_send_with_tz_raises(queue_factory):
    from datetime import datetime, timezone
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    with pytest.raises(NotImplementedError, match="tz"):
        q.send(name, {"a": 1}, tz=datetime.now(timezone.utc))

"""pop: read+delete in one atomic step."""

from __future__ import annotations

from sqlmq import Message
from tests.conftest import unique_queue_name


def test_pop_returns_message_and_removes(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    q.send(name, {"a": 1})
    popped = q.pop(name)
    assert isinstance(popped, Message)
    assert popped.message == {"a": 1}
    # Now empty.
    assert q.read(name) is None


def test_pop_returns_none_when_empty(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    assert q.pop(name) is None

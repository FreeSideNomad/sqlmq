"""purge."""

from __future__ import annotations

from tests.conftest import unique_queue_name


def test_purge_removes_all_messages(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    q.send_batch(name, [{"i": i} for i in range(5)])
    n = q.purge(name)
    assert n == 5
    assert q.read(name) is None


def test_purge_empty_queue_returns_zero(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    assert q.purge(name) == 0

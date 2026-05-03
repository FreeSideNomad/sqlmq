"""metrics, metrics_all."""

from __future__ import annotations

from sqlmq import QueueMetrics
from tests.conftest import unique_queue_name


def test_metrics_for_empty_queue(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    m = q.metrics(name)
    assert isinstance(m, QueueMetrics)
    assert m.queue_name == name
    assert m.queue_length == 0
    assert m.total_messages == 0
    # newest_msg_age_sec is always None for sqlmq.
    assert m.newest_msg_age_sec is None


def test_metrics_after_sends(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    q.send_batch(name, [{"i": i} for i in range(3)])
    m = q.metrics(name)
    assert m.queue_length == 3
    assert m.total_messages == 3


def test_metrics_all_returns_every_queue(queue_factory):
    q = queue_factory()
    a, b = unique_queue_name("a"), unique_queue_name("b")
    q.create_queue(a)
    q.create_queue(b)
    names = {m.queue_name for m in q.metrics_all()}
    assert {a, b} <= names

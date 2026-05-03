"""read_with_poll: client-side polling, since sqlmq has no server-side long-poll."""

from __future__ import annotations

import threading
import time

from tests.conftest import unique_queue_name


def test_read_with_poll_returns_immediately_when_message_present(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    q.send(name, {"a": 1})
    t0 = time.monotonic()
    msgs = q.read_with_poll(name, max_poll_seconds=5, poll_interval_ms=100)
    assert msgs and msgs[0].message == {"a": 1}
    assert time.monotonic() - t0 < 1.0  # Got it on the first try.


def test_read_with_poll_returns_empty_after_deadline(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    t0 = time.monotonic()
    msgs = q.read_with_poll(name, max_poll_seconds=1, poll_interval_ms=100)
    elapsed = time.monotonic() - t0
    assert msgs == []
    assert 0.9 <= elapsed <= 2.5  # Approximately the deadline.


def test_read_with_poll_picks_up_late_arrival(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    # Use a separate PGMQueue for the producer to avoid sharing connections.
    producer = queue_factory()

    def _send_after_delay():
        time.sleep(0.5)
        producer.send(name, {"late": True})

    threading.Thread(target=_send_after_delay, daemon=True).start()
    msgs = q.read_with_poll(name, max_poll_seconds=5, poll_interval_ms=100)
    assert msgs and msgs[0].message == {"late": True}

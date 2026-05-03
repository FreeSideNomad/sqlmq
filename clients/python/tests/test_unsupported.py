"""Verify that pgmq features sqlmq does not support raise NotImplementedError
with helpful messages."""

from __future__ import annotations

import pytest

from tests.conftest import unique_queue_name


def test_unlogged_create_queue_raises(queue_factory):
    q = queue_factory()
    with pytest.raises(NotImplementedError) as ei:
        q.create_queue(unique_queue_name(), unlogged=True)
    msg = str(ei.value)
    assert "unlogged" in msg.lower()
    assert "V014" in msg or "rationale" in msg.lower()


def test_create_partitioned_queue_raises(queue_factory):
    q = queue_factory()
    with pytest.raises(NotImplementedError) as ei:
        q.create_partitioned_queue(unique_queue_name())
    assert "partition" in str(ei.value).lower()


def test_set_vt_raises(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    msg_id = q.send(name, {"a": 1})
    with pytest.raises(NotImplementedError, match="set_vt"):
        q.set_vt(name, msg_id, 60)


def test_detach_archive_raises(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    with pytest.raises(NotImplementedError, match="detach_archive"):
        q.detach_archive(name)

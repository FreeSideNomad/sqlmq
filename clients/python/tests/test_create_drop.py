"""create_queue, drop_queue, list_queues, validate_queue_name."""

from __future__ import annotations

import pytest

from tests.conftest import unique_queue_name


def test_create_then_list(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    assert name in q.list_queues()


def test_drop_returns_true_when_present(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    assert q.drop_queue(name) is True
    assert name not in q.list_queues()


def test_drop_returns_false_when_absent(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    # Never created.
    assert q.drop_queue(name) is False


def test_drop_partitioned_kwarg_is_ignored(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    assert q.drop_queue(name, partitioned=True) is True


def test_validate_queue_name_accepts_legal_names(queue_factory):
    q = queue_factory()
    for name in ["a", "abc", "_x", "Q1", "abc_123", "x" * 60]:
        q.validate_queue_name(name)  # no raise


def test_validate_queue_name_rejects_illegal(queue_factory):
    q = queue_factory()
    for bad in ["", "1abc", "ab-cd", "ab.cd", "x" * 61, "drop table"]:
        with pytest.raises(ValueError):
            q.validate_queue_name(bad)


def test_create_queue_unlogged_raises(queue_factory):
    q = queue_factory()
    with pytest.raises(NotImplementedError, match="unlogged"):
        q.create_queue(unique_queue_name(), unlogged=True)


def test_create_partitioned_queue_raises(queue_factory):
    q = queue_factory()
    with pytest.raises(NotImplementedError, match="partition"):
        q.create_partitioned_queue(unique_queue_name())

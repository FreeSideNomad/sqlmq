"""delete, delete_batch, archive, archive_batch."""

from __future__ import annotations

from tests.conftest import unique_queue_name


def test_delete_returns_true_when_present(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    msg_id = q.send(name, {"a": 1})
    assert q.delete(name, msg_id) is True


def test_delete_returns_false_when_absent(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    # never sent — id 99999 doesn't exist
    assert q.delete(name, 99999) is False


def test_delete_batch_returns_actually_deleted_ids(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    ids = q.send_batch(name, [{"i": i} for i in range(3)])
    bogus = ids + [99999]
    deleted = q.delete_batch(name, bogus)
    assert sorted(deleted) == sorted(ids)


def test_delete_batch_empty_returns_empty(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    assert q.delete_batch(name, []) == []


def test_archive_moves_message(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    msg_id = q.send(name, {"a": 1})
    assert q.archive(name, msg_id) is True
    # No longer readable from the active queue.
    assert q.read(name) is None


def test_archive_returns_false_when_absent(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    assert q.archive(name, 99999) is False


def test_archive_batch_returns_actually_archived_ids(queue_factory):
    q = queue_factory()
    name = unique_queue_name()
    q.create_queue(name)
    ids = q.send_batch(name, [{"i": i} for i in range(3)])
    archived = q.archive_batch(name, ids + [99999])
    assert sorted(archived) == sorted(ids)
    assert q.read(name) is None

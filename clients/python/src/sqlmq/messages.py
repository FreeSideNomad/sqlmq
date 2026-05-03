"""Dataclasses mirroring tembo-pgmq-python's messages module.

These shapes are identical to ``tembo_pgmq_python.messages`` so that consumer
code that imports ``Message`` / ``QueueMetrics`` does not need to change.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass
class Message:
    """A single queued message.

    Mirrors ``tembo_pgmq_python.messages.Message`` exactly. ``message`` is the
    decoded JSON payload as a Python dict, not the raw stored string.

    ``headers`` is a sqlmq-specific addition: pgmq does not carry per-message
    headers, but sqlmq's storage layer does (NVARCHAR(MAX) JSON column on
    every queue table). The Python client used to silently drop the column
    even when populated; this field surfaces it. ``None`` when the underlying
    row has no headers (the common case for messages sent through the strict
    pgmq surface).
    """

    msg_id: int
    read_ct: int
    enqueued_at: datetime
    vt: datetime
    message: dict
    headers: Optional[dict[str, Any]] = None


@dataclass
class QueueMetrics:
    """Metrics snapshot for a single queue.

    Mirrors ``tembo_pgmq_python.messages.QueueMetrics`` exactly.

    Notes on sqlmq-specific behavior:

    * ``scrape_time`` is filled in client-side at the moment ``metrics``
      returns (UTC), since sqlmq does not return it from the proc.
    """

    queue_name: str
    queue_length: int
    newest_msg_age_sec: Optional[int]
    oldest_msg_age_sec: Optional[int]
    total_messages: int
    scrape_time: datetime

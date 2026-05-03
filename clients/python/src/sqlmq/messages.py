"""Dataclasses mirroring tembo-pgmq-python's messages module.

These shapes are identical to ``tembo_pgmq_python.messages`` so that consumer
code that imports ``Message`` / ``QueueMetrics`` does not need to change.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Message:
    """A single queued message.

    Mirrors ``tembo_pgmq_python.messages.Message`` exactly. ``message`` is the
    decoded JSON payload as a Python dict, not the raw stored string.
    """

    msg_id: int
    read_ct: int
    enqueued_at: datetime
    vt: datetime
    message: dict


@dataclass
class QueueMetrics:
    """Metrics snapshot for a single queue.

    Mirrors ``tembo_pgmq_python.messages.QueueMetrics`` exactly.

    Notes on sqlmq-specific behavior:

    * ``newest_msg_age_sec`` is always ``None`` because sqlmq's ``metrics``
      proc does not currently surface it (pgmq derives it cheaply from the
      same query; sqlmq's would require a second scan and was deferred).
    * ``scrape_time`` is filled in client-side at the moment ``metrics``
      returns (UTC), since sqlmq does not return it from the proc.
    """

    queue_name: str
    queue_length: int
    newest_msg_age_sec: Optional[int]
    oldest_msg_age_sec: Optional[int]
    total_messages: int
    scrape_time: datetime

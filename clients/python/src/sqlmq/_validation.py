"""Client-side queue name validation.

Mirrors V003__create_queue_hardening.sql's server-side rules so we can fail
fast with a clear Python error before the round-trip to SQL Server.
"""

from __future__ import annotations

import re

# sqlmq V003: name must start with a letter or underscore, contain only
# [A-Za-z0-9_], and be at most 60 characters (to leave room for derived
# constraint and index names within SYSNAME's 128-char limit).
_QUEUE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,59}$")


def validate_queue_name(queue_name: str) -> None:
    """Validate a queue name. Raises ValueError on mismatch."""
    if queue_name is None or queue_name == "":
        raise ValueError("queue name must be non-empty")
    if not _QUEUE_NAME_RE.match(queue_name):
        raise ValueError(
            f"invalid queue name {queue_name!r}: must match "
            r"^[A-Za-z_][A-Za-z0-9_]{0,59}$ "
            "(start with letter/underscore, then letters/digits/underscores, max 60 chars)"
        )

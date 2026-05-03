"""sqlmq: strict pgmq-API-compatible Python client for sqlmq on SQL Server.

The exports below are deliberately a 1:1 mirror of
``tembo_pgmq_python.__init__``:

    from sqlmq import PGMQueue, Message

is sufficient to migrate Python code that previously imported from
``tembo_pgmq_python``.

We additionally export ``AsyncPGMQueue`` (mirroring tembo's
``AsyncPGMQueue``) and ``QueueMetrics`` (a public dataclass returned by
``metrics()``).

For users who would rather not pretend to be pgmq, ``SqlmqQueue`` and
``AsyncSqlmqQueue`` are aliases of the same classes.
"""

from .messages import Message, QueueMetrics
from .queue import PGMQueue
from .async_queue import AsyncPGMQueue

# Friendlier-named aliases for users who don't care about pgmq compat.
SqlmqQueue = PGMQueue
AsyncSqlmqQueue = AsyncPGMQueue

__all__ = [
    "PGMQueue",
    "AsyncPGMQueue",
    "Message",
    "QueueMetrics",
    "SqlmqQueue",
    "AsyncSqlmqQueue",
]

__version__ = "0.1.0"

"""Asynchronous PGMQueue: strict pgmq-API-compatible client for sqlmq.

We deliberately wrap the synchronous client via ``asyncio.to_thread`` rather
than depending on ``aioodbc``:

* ``aioodbc`` is itself a thread-pool wrapper around pyodbc — there is no
  truly-async ODBC available in CPython today.
* TVP passing through aioodbc has historically been flaky; the sync path is
  battle-tested.
* Wrapping ``to_thread`` keeps a single source of truth for SQL behavior.

The cost is one extra thread hop per call. For an I/O-bound database client,
this is negligible compared with the network round trip.

Public surface mirrors ``tembo_pgmq_python.async_queue.PGMQueue`` exactly.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, List, Optional

from .messages import Message, QueueMetrics
from .queue import PGMQueue as _SyncPGMQueue


@dataclass
class AsyncPGMQueue:
    """Async wrapper over the sync ``PGMQueue``.

    Construction is identical to the sync class; ``init()`` is a no-op kept
    for parity with tembo's async client (which uses it to lazily create the
    asyncpg pool). Calling ``init()`` is safe but optional.
    """

    host: str = field(default_factory=lambda: os.getenv("MSSQL_HOST", "localhost"))
    port: str = field(default_factory=lambda: os.getenv("MSSQL_PORT", "1433"))
    database: str = field(default_factory=lambda: os.getenv("MSSQL_DATABASE", "sqlmq"))
    username: str = field(default_factory=lambda: os.getenv("MSSQL_USERNAME", "sa"))
    password: str = field(default_factory=lambda: os.getenv("MSSQL_PASSWORD", ""))
    delay: int = 0
    vt: int = 30
    pool_size: int = 10
    conn_string: Optional[str] = None
    kwargs: dict = field(default_factory=dict)
    verbose: bool = False
    log_filename: Optional[str] = None

    _sync: _SyncPGMQueue = field(init=False)

    def __post_init__(self) -> None:
        self._sync = _SyncPGMQueue(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password,
            delay=self.delay,
            vt=self.vt,
            pool_size=self.pool_size,
            conn_string=self.conn_string,
            kwargs=self.kwargs,
            verbose=self.verbose,
            log_filename=self.log_filename,
        )

    async def init(self) -> None:
        """No-op kept for tembo-pgmq-python parity. Connection is lazy."""
        return None

    # The dispatcher: every method below just delegates to the sync impl
    # via to_thread. We spell each method out so static analyzers and
    # users see a real signature, not a generic ``__getattr__``.

    async def validate_queue_name(self, queue_name: str) -> None:
        return await asyncio.to_thread(self._sync.validate_queue_name, queue_name)

    async def create_queue(self, queue: str, unlogged: bool = False, conn=None) -> None:
        return await asyncio.to_thread(self._sync.create_queue, queue, unlogged, conn)

    async def create_partitioned_queue(
        self,
        queue: str,
        partition_interval: int = 10000,
        retention_interval: int = 100000,
        conn=None,
    ) -> None:
        return await asyncio.to_thread(
            self._sync.create_partitioned_queue,
            queue,
            partition_interval,
            retention_interval,
            conn,
        )

    async def drop_queue(self, queue: str, partitioned: bool = False, conn=None) -> bool:
        return await asyncio.to_thread(self._sync.drop_queue, queue, partitioned, conn)

    async def list_queues(self, conn=None) -> List[str]:
        return await asyncio.to_thread(self._sync.list_queues, conn)

    async def send(
        self,
        queue: str,
        message: dict,
        delay: int = 0,
        tz: Optional[datetime] = None,
        conn=None,
    ) -> int:
        return await asyncio.to_thread(self._sync.send, queue, message, delay, tz, conn)

    async def send_batch(
        self,
        queue: str,
        messages: List[dict],
        delay: int = 0,
        tz: Optional[datetime] = None,
        conn=None,
    ) -> List[int]:
        return await asyncio.to_thread(self._sync.send_batch, queue, messages, delay, tz, conn)

    async def read(self, queue: str, vt: Optional[int] = None, conn=None) -> Optional[Message]:
        return await asyncio.to_thread(self._sync.read, queue, vt, conn)

    async def read_batch(
        self, queue: str, vt: Optional[int] = None, batch_size: int = 1, conn=None
    ) -> Optional[List[Message]]:
        return await asyncio.to_thread(self._sync.read_batch, queue, vt, batch_size, conn)

    async def read_with_poll(
        self,
        queue: str,
        vt: Optional[int] = None,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
        conn=None,
    ) -> Optional[List[Message]]:
        # Honest async polling: the inner loop sleeps with asyncio.sleep so
        # the caller's event loop isn't blocked. We therefore reimplement
        # the loop here rather than delegating directly to sync.
        deadline = asyncio.get_running_loop().time() + max_poll_seconds
        sleep_s = max(poll_interval_ms, 1) / 1000.0
        while True:
            msgs = await self.read_batch(queue, vt=vt, batch_size=qty, conn=conn)
            if msgs:
                return msgs
            if asyncio.get_running_loop().time() >= deadline:
                return msgs
            await asyncio.sleep(sleep_s)

    async def pop(self, queue: str, conn=None) -> Optional[Message]:
        return await asyncio.to_thread(self._sync.pop, queue, conn)

    async def delete(self, queue: str, msg_id: int, conn=None) -> bool:
        return await asyncio.to_thread(self._sync.delete, queue, msg_id, conn)

    async def delete_batch(self, queue: str, msg_ids: List[int], conn=None) -> List[int]:
        return await asyncio.to_thread(self._sync.delete_batch, queue, msg_ids, conn)

    async def archive(self, queue: str, msg_id: int, conn=None) -> bool:
        return await asyncio.to_thread(self._sync.archive, queue, msg_id, conn)

    async def archive_batch(self, queue: str, msg_ids: List[int], conn=None) -> List[int]:
        return await asyncio.to_thread(self._sync.archive_batch, queue, msg_ids, conn)

    async def purge(self, queue: str, conn=None) -> int:
        return await asyncio.to_thread(self._sync.purge, queue, conn)

    async def metrics(self, queue: str, conn=None) -> QueueMetrics:
        return await asyncio.to_thread(self._sync.metrics, queue, conn)

    async def metrics_all(self, conn=None) -> List[QueueMetrics]:
        return await asyncio.to_thread(self._sync.metrics_all, conn)

    async def set_vt(self, queue: str, msg_id: int, vt: int, conn=None) -> Message:
        return await asyncio.to_thread(self._sync.set_vt, queue, msg_id, vt, conn)

    async def detach_archive(self, queue: str, conn=None) -> None:
        return await asyncio.to_thread(self._sync.detach_archive, queue, conn)
